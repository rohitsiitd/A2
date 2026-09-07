/*
 * exchange_server.cpp — order matching engine for The Socket Exchange.
 *
 * Usage: ./exchange_server <host> <port>
 *   e.g. ./exchange_server 127.0.0.1 5000
 *
 * A single-threaded poll() event loop owns every connection and all exchange
 * state, so none of it needs locking. Sockets are non-blocking and each
 * connection carries its own outbound queue, so a peer that stops reading
 * backs up only its own queue and never delays anybody else.
 *
 * Closes are deferred to the end of a tick instead of taken on the spot.
 * Matching an order can fail a send on a *different* connection from the one
 * whose message is being processed, and erasing that connection mid-loop
 * would either relocate a not-yet-visited entry into an already-visited slot
 * or invalidate the reference the loop is holding. Marking it and sweeping up
 * afterwards keeps `clients` and `fds` structurally stable for the whole
 * tick, so any Client& taken during a tick stays valid for that tick.
 */

#include <algorithm>     // std::min
#include <array>
#include <arpa/inet.h>   // inet_pton, inet_ntoa
#include <cerrno>        // errno, EAGAIN, EWOULDBLOCK, EINTR
#include <charconv>      // std::from_chars
#include <csignal>       // std::signal, SIGPIPE, SIG_IGN
#include <cstdio>        // std::printf, std::fprintf, perror
#include <cstdlib>       // std::atoi
#include <fcntl.h>       // fcntl, O_NONBLOCK
#include <netinet/in.h>  // struct sockaddr_in, htons, ntohs
#include <optional>
#include <poll.h>        // struct pollfd, poll, POLLIN, POLLOUT, POLLHUP, POLLERR
#include <sstream>       // std::istringstream
#include <string>
#include <sys/resource.h>  // getrlimit, setrlimit, RLIMIT_NOFILE
#include <sys/socket.h>  // socket, bind, listen, accept, recv, send
#include <unistd.h>      // close
#include <unordered_map>
#include <vector>

namespace {

// Deep enough that a burst of simultaneous connection attempts waits in the
// kernel rather than being refused while the loop is busy elsewhere.
constexpr int kBacklog = 1024;
// Not a capacity limit — a guard against a peer that connects and then never
// sends a '\n'.
constexpr std::size_t kMaxInbufBytes = 4096;
constexpr std::size_t kRecvChunk = 4096;

// --- Roles and instruments -------------------------------------------------

enum class Role { Unknown, Trader, MarketData };
enum class Instrument { JNST, IMCT };
enum class Side { Buy, Sell };

constexpr std::size_t kInstrumentCount = 2;  // JNST, IMCT

std::optional<Instrument> parse_instrument(const std::string &s) {
    if (s == "JNST") return Instrument::JNST;
    if (s == "IMCT") return Instrument::IMCT;
    return std::nullopt;
}

const char *instrument_name(Instrument inst) {
    return (inst == Instrument::JNST) ? "JNST" : "IMCT";
}

// --- Small parsing helpers ---------------------------------------------
// from_chars rather than stoi: it reports failure without throwing, and the
// end pointer it hands back lets us insist the WHOLE token was digits rather
// than just a valid prefix — "12abc" has to be rejected, not read as 12.
std::optional<int> parse_positive_int(const std::string &s) {
    int value = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    if (ec != std::errc{} || ptr != s.data() + s.size()) return std::nullopt;
    if (value <= 0) return std::nullopt;
    return value;
}

std::optional<int> parse_nonneg_int(const std::string &s) {
    int value = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    if (ec != std::errc{} || ptr != s.data() + s.size()) return std::nullopt;
    if (value < 0) return std::nullopt;
    return value;
}

// --- Per-connection state ---------------------------------------------------
// client_id is a separate identity from fd because the OS recycles fd numbers
// as soon as they are closed. An order has to keep pointing at the connection
// that placed it even after some later, unrelated client inherits that number.
// `outq` holds bytes handed to us but not yet accepted by the kernel.
struct Client {
    int fd = -1;
    int client_id = -1;
    Role role = Role::Unknown;
    std::string username;
    std::array<bool, kInstrumentCount> subscribed{false, false};
    std::string inbuf;
    std::string outq;
};

// --- Orders ------------------------------------------------------------
// `open` stays true while unfilled quantity is still resting in the book;
// either a CANCEL or match_order() filling it to zero clears it. A partial
// fill leaves the order open with `qty` reduced to the remainder.
struct Order {
    int id = 0;
    int owner_client_id = 0;
    Instrument inst = Instrument::JNST;
    Side side = Side::Buy;
    int price = 0;
    int qty = 0;
    bool open = true;
};

// `orders` only ever grows, via push_back from BUY/SELL handling, and never
// while a reference into it is held across that push_back. That invariant is
// what keeps the Order* returned by find_open_order() and the Order& handed
// to match_order() from dangling.
std::vector<pollfd> fds;
std::unordered_map<int, Client> clients;  // keyed by fd
std::vector<Order> orders;
int next_client_id = 0;
int next_order_id = 0;

// Filled during a tick, drained once the client loop finishes. May list the
// same fd twice; the cleanup pass skips whatever has already gone.
std::vector<int> pending_close;

void mark_for_close(int fd) {
    pending_close.push_back(fd);
}

pollfd *find_pollfd(int fd) {
    for (auto &p : fds) {
        if (p.fd == fd) return &p;
    }
    return nullptr;
}

Order *find_open_order(int id, int owner_client_id) {
    for (auto &o : orders) {
        if (o.id == id && o.owner_client_id == owner_client_id && o.open) {
            return &o;
        }
    }
    return nullptr;
}

// A client_id outlives its entry in `clients`, since a disconnect erases the
// entry outright. Returns nullptr once that trader has left, which a match
// completing after their departure has to tolerate.
Client *find_client_by_id(int client_id) {
    for (auto &[fd, c] : clients) {
        (void)fd;
        if (c.client_id == client_id) return &c;
    }
    return nullptr;
}

// Send whatever the kernel will take now; queue the rest and ask for POLLOUT
// so the loop hears about it the moment there is room for more. When the
// queue is already non-empty this connection is backed up, so new data is
// appended without attempting a send — otherwise it would overtake bytes
// that have been waiting longer.
void send_or_queue(int fd, const std::string &data) {
    auto it = clients.find(fd);
    if (it == clients.end()) return;  // connection already gone this tick
    Client &c = it->second;

    if (!c.outq.empty()) {
        c.outq += data;
        return;
    }

    ssize_t n = send(fd, data.data(), data.size(), 0);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            n = 0;  // kernel took nothing; queue all of it below
        } else {
            // EPIPE, ECONNRESET and the like: the peer is gone.
            perror("send");
            mark_for_close(fd);
            return;
        }
    }

    auto sent = static_cast<std::size_t>(n);
    if (sent < data.size()) {
        c.outq = data.substr(sent);
        if (pollfd *p = find_pollfd(fd)) {
            p->events |= POLLOUT;
        }
    }
}

void reply(int fd, const std::string &msg) {
    send_or_queue(fd, msg + "\n");
}

// Each of these sends can end up queued independently, so one slow subscriber
// does not hold up delivery to the rest.
void broadcast_trade(Instrument inst, int qty, int price) {
    for (auto &[fd, c] : clients) {
        if (c.role == Role::MarketData && c.subscribed[static_cast<std::size_t>(inst)]) {
            reply(fd, std::string("TRADE ") + instrument_name(inst) + " " +
                          std::to_string(qty) + " " + std::to_string(price));
        }
    }
}

// Match a newly-accepted order against the resting book. `orders` is
// append-only in acceptance order, so a front-to-back scan gives the oldest
// resting orders priority for free. One incoming order can cross several
// resting ones in a single call — a large BUY eating three small SELLs at the
// same price — so the loop keeps going until `o` is filled or the book has
// been walked once.
//
// Resting orders never need re-matching against each other: the book only
// ever holds mutually uncrossable orders at rest, and the only moment that
// can stop being true is when a new order arrives, which is when this runs.
void match_order(Order &o) {
    for (auto &other : orders) {
        if (o.qty == 0) break;
        if (&other == &o) continue;
        if (!other.open) continue;
        if (other.inst != o.inst) continue;
        if (other.side == o.side) continue;
        if (other.price != o.price) continue;

        int traded_qty = std::min(o.qty, other.qty);
        int traded_price = o.price;  // == other.price, guaranteed above

        o.qty -= traded_qty;
        other.qty -= traded_qty;
        if (o.qty == 0) o.open = false;
        if (other.qty == 0) other.open = false;

        // BOUGHT/SOLD go to each order's own owner, not to whoever sent the
        // message that triggered the match, so both sides need looking up.
        // Either may have disconnected after placing their order; nullptr
        // simply means there is nobody left to tell.
        Client *o_owner = find_client_by_id(o.owner_client_id);
        Client *other_owner = find_client_by_id(other.owner_client_id);
        const char *inst_str = instrument_name(o.inst);

        auto notify = [&](Client *owner, Side side) {
            if (owner == nullptr) return;
            const char *verb = (side == Side::Buy) ? "BOUGHT" : "SOLD";
            reply(owner->fd, std::string(verb) + " " + inst_str + " " +
                                  std::to_string(traded_qty) + " " +
                                  std::to_string(traded_price));
        };
        notify(o_owner, o.side);
        notify(other_owner, other.side);

        broadcast_trade(o.inst, traded_qty, traded_price);
    }
}

// --- Command handling ----------------------------------------------------

std::vector<std::string> tokenize(const std::string &msg) {
    std::istringstream iss(msg);
    std::vector<std::string> tokens;
    std::string tok;
    while (iss >> tok) tokens.push_back(std::move(tok));
    return tokens;
}

// Returns true if the connection should be closed (QUIT), false otherwise.
bool handle_trader_command(Client &c, const std::string &cmd, const std::vector<std::string> &tok) {
    if (cmd == "LOGIN") {
        reply(c.fd, "ERROR already logged in");
    } else if (cmd == "BUY" || cmd == "SELL") {
        if (tok.size() != 4) {
            reply(c.fd, "ERROR bad arguments");
        } else {
            auto inst = parse_instrument(tok[1]);
            auto qty = parse_positive_int(tok[2]);
            auto price = parse_positive_int(tok[3]);

            if (!inst) {
                reply(c.fd, "ERROR unknown instrument");
            } else if (!qty) {
                reply(c.fd, "ERROR bad quantity");
            } else if (!price) {
                reply(c.fd, "ERROR bad price");
            } else {
                Order o;
                o.id = next_order_id++;
                o.owner_client_id = c.client_id;
                o.inst = *inst;
                o.side = (cmd == "BUY") ? Side::Buy : Side::Sell;
                o.price = *price;
                o.qty = *qty;
                o.open = true;
                orders.push_back(o);

                reply(c.fd, "ORDER_ACCEPTED " + std::to_string(o.id));
                match_order(orders.back());
            }
        }
    } else if (cmd == "CANCEL") {
        if (tok.size() != 2) {
            reply(c.fd, "ERROR bad arguments");
        } else {
            auto id = parse_nonneg_int(tok[1]);
            if (!id) {
                reply(c.fd, "ERROR bad order id");
            } else {
                Order *o = find_open_order(*id, c.client_id);
                if (o == nullptr) {
                    reply(c.fd, "ERROR order not found");
                } else {
                    o->open = false;
                    reply(c.fd, "ORDER_CANCELLED " + std::to_string(*id));
                }
            }
        }
    } else if (cmd == "SUBSCRIBE" || cmd == "UNSUBSCRIBE") {
        reply(c.fd, "ERROR wrong role");
    } else {
        reply(c.fd, "ERROR unknown command");
    }
    return false;
}

bool handle_marketdata_command(Client &c, const std::string &cmd, const std::vector<std::string> &tok) {
    if (cmd == "SUBSCRIBE" || cmd == "UNSUBSCRIBE") {
        if (tok.size() != 2) {
            reply(c.fd, "ERROR bad arguments");
        } else {
            auto inst = parse_instrument(tok[1]);
            if (!inst) {
                reply(c.fd, "ERROR unknown instrument");
            } else {
                c.subscribed[static_cast<std::size_t>(*inst)] = (cmd == "SUBSCRIBE");
                reply(c.fd, "OK");
            }
        }
    } else if (cmd == "LOGIN" || cmd == "BUY" || cmd == "SELL" || cmd == "CANCEL") {
        reply(c.fd, "ERROR wrong role");
    } else {
        reply(c.fd, "ERROR unknown command");
    }
    return false;
}

bool handle_command(Client &c, const std::string &msg) {
    std::vector<std::string> tok = tokenize(msg);

    if (tok.empty()) {
        reply(c.fd, "ERROR empty command");
        return false;
    }

    const std::string &cmd = tok[0];

    if (cmd == "QUIT") {
        return true;  // no reply defined for QUIT; just close the connection
    }

    if (c.role == Role::Unknown) {
        if (cmd == "LOGIN" && tok.size() == 2) {
            c.role = Role::Trader;
            c.username = tok[1];
            reply(c.fd, "OK");
        } else if (cmd == "SUBSCRIBE" && tok.size() == 2) {
            auto inst = parse_instrument(tok[1]);
            if (!inst) {
                reply(c.fd, "ERROR unknown instrument");
            } else {
                c.role = Role::MarketData;
                c.subscribed[static_cast<std::size_t>(*inst)] = true;
                reply(c.fd, "OK");
            }
        } else {
            reply(c.fd, "ERROR must LOGIN or SUBSCRIBE first");
        }
        return false;
    }

    if (c.role == Role::Trader) {
        return handle_trader_command(c, cmd, tok);
    }
    return handle_marketdata_command(c, cmd, tok);
}

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// EXCHANGE_DEBUG=1 traces every recv(). An environment variable rather than a
// command-line flag, because the server takes exactly <host> <port> and
// rejects anything else.
bool debug_enabled() {
    static const bool on = (std::getenv("EXCHANGE_DEBUG") != nullptr);
    return on;
}

// A '\n' has to survive into the log as two visible characters. Printed
// literally it becomes a line break like any other, and the trace can no
// longer show where one message ends and the next begins.
std::string escape_bytes(const char *p, std::size_t n) {
    std::string out;
    for (std::size_t i = 0; i < n; i++) {
        unsigned char ch = static_cast<unsigned char>(p[i]);
        if (ch == '\n') {
            out += "\\n";
        } else if (ch == '\r') {
            out += "\\r";
        } else if (ch >= 32 && ch < 127) {
            out += static_cast<char>(ch);
        } else {
            char b[8];
            std::snprintf(b, sizeof(b), "\\x%02x", ch);
            out += b;
        }
    }
    return out;
}

}  // namespace

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <host> <port>\n", argv[0]);
        return 1;
    }

    // A send() to a peer that has already closed would otherwise raise
    // SIGPIPE, and its default disposition kills the process outright.
    // Ignoring it turns the same event into EPIPE from send(), which
    // send_or_queue() handles like any other per-connection failure.
    std::signal(SIGPIPE, SIG_IGN);

    // A default soft limit of around 1024 descriptors caps concurrent clients
    // far below what a single event loop can comfortably carry. Best effort:
    // the hard ceiling and the system-wide limit both still apply, so failing
    // here is not fatal.
    rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }

    const char *host = argv[1];
    int port = std::atoi(argv[2]);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt(SO_REUSEADDR)");
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        std::fprintf(stderr, "invalid host address: %s\n", host);
        return 1;
    }

    if (bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(listen_fd, kBacklog) < 0) {
        perror("listen");
        return 1;
    }

    // The accept loop below drains until the queue is empty, so its last call
    // of each burst always finds nothing left. On a blocking listener that
    // call would park the entire event loop until some unrelated client
    // happened to connect; non-blocking turns it into EWOULDBLOCK, which is
    // what tells the loop to stop.
    set_nonblocking(listen_fd);

    std::printf("exchange_server: listening on %s:%d\n", host, port);
    std::fflush(stdout);

    fds.push_back(pollfd{listen_fd, POLLIN, 0});

    for (;;) {
        int ready = poll(fds.data(), fds.size(), -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("poll");
            return 1;
        }

        // --- New connections. poll() is level-triggered: it says the
        // listener is readable when at least one connection is pending, never
        // how many. Taking only one per wake-up would spend a full poll()
        // cycle — O(n) across every registered fd — on each connection, so
        // during a burst the drain rate falls further behind arrivals the
        // more clients are already connected. The kernel's accept queue then
        // overflows, and FreeBSD discards the excess in sonewconn() without
        // sending RST, leaving those peers ESTABLISHED against a socket that
        // no longer exists. Draining to EWOULDBLOCK keeps the queue empty
        // whatever the burst size.
        if (fds[0].revents & POLLIN) {
            for (;;) {
                sockaddr_in client_addr{};
                socklen_t client_len = sizeof(client_addr);

                int client_fd = accept(
                    listen_fd,
                    reinterpret_cast<sockaddr *>(&client_addr),
                    &client_len
                );

                if (client_fd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    // The peer reset between completing the handshake and
                    // being accepted. That loses one pending connection, not
                    // the listener, so keep draining the rest of the queue.
                    if (errno == ECONNABORTED || errno == EINTR) continue;
                    perror("accept");
                    break;
                }

                set_nonblocking(client_fd);

                std::printf(
                    "exchange_server: client connected from %s:%d (fd=%d)\n",
                    inet_ntoa(client_addr.sin_addr),
                    ntohs(client_addr.sin_port),
                    client_fd
                );
                std::fflush(stdout);

                // revents starts at 0 so the scan below skips this entry. A
                // client accepted during this tick gets picked up on the next
                // poll() rather than read through a stale mask.
                fds.push_back(pollfd{client_fd, POLLIN, 0});

                Client c;
                c.fd = client_fd;
                c.client_id = next_client_id++;
                c.role = Role::Unknown;
                clients.emplace(client_fd, std::move(c));
            }
        }

        // --- Existing clients. Nothing in this loop changes the structure of
        // `fds` or `clients`, so every Client& taken here stays valid for the
        // rest of the tick however many connections get marked for closing.
        for (std::size_t i = 1; i < fds.size(); i++) {
            short revents = fds[i].revents;
            if (revents == 0) continue;

            int fd = fds[i].fd;

            if (revents & POLLOUT) {
                Client &c = clients.at(fd);
                ssize_t n = send(fd, c.outq.data(), c.outq.size(), 0);
                if (n < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("send");
                        mark_for_close(fd);
                    }
                } else {
                    c.outq.erase(0, static_cast<std::size_t>(n));
                    if (c.outq.empty()) {
                        // A drained socket is writable almost all the time,
                        // so leaving POLLOUT set here would wake poll() on
                        // this fd every single call.
                        fds[i].events &= ~POLLOUT;
                    }
                }
            }

            if (revents & (POLLIN | POLLHUP | POLLERR)) {
                Client &c = clients.at(fd);

                if (c.inbuf.size() >= kMaxInbufBytes) {
                    reply(fd, "ERROR message too long");
                    std::fprintf(stderr, "exchange_server: fd=%d message too long, dropping\n", fd);
                    mark_for_close(fd);
                } else {
                    char chunk[kRecvChunk];
                    ssize_t n = recv(fd, chunk, sizeof(chunk), 0);

                    if (n > 0) {
                        if (debug_enabled()) {
                            std::printf(
                                "exchange_server: [recv] fd=%d %zd bytes: \"%s\"\n",
                                fd, n, escape_bytes(chunk, static_cast<std::size_t>(n)).c_str()
                            );
                        }

                        c.inbuf.append(chunk, static_cast<std::size_t>(n));

                        if (debug_enabled()) {
                            std::printf(
                                "exchange_server: [buf ] fd=%d holds %zu bytes: \"%s\"\n",
                                fd, c.inbuf.size(),
                                escape_bytes(c.inbuf.data(), c.inbuf.size()).c_str()
                            );
                        }

                        for (;;) {
                            std::size_t nl = c.inbuf.find('\n');
                            if (nl == std::string::npos) break;

                            std::string line = c.inbuf.substr(0, nl);
                            c.inbuf.erase(0, nl + 1);

                            if (debug_enabled()) {
                                std::printf(
                                    "exchange_server: [line] fd=%d complete message: \"%s\"\n",
                                    fd, line.c_str()
                                );
                            }

                            if (handle_command(c, line)) {
                                mark_for_close(fd);
                                break;
                            }
                        }

                        if (debug_enabled()) {
                            if (c.inbuf.empty()) {
                                std::printf("exchange_server: [buf ] fd=%d empty\n", fd);
                            } else {
                                std::printf(
                                    "exchange_server: [wait] fd=%d %zu bytes buffered, no newline yet\n",
                                    fd, c.inbuf.size()
                                );
                            }
                            std::fflush(stdout);
                        }
                    } else if (n == 0) {
                        std::printf("exchange_server: fd=%d client disconnected\n", fd);
                        std::fflush(stdout);
                        mark_for_close(fd);
                    } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("recv");
                        mark_for_close(fd);
                    }
                }
            }
        }

        // --- The loop is done walking them, so it is safe to mutate
        // `fds`/`clients` here.
        for (int fd : pending_close) {
            auto it = clients.find(fd);
            if (it == clients.end()) continue;  // already closed this tick

            close(fd);
            clients.erase(it);

            for (std::size_t i = 0; i < fds.size(); i++) {
                if (fds[i].fd == fd) {
                    fds[i] = fds.back();
                    fds.pop_back();
                    break;
                }
            }
        }
        pending_close.clear();
    }
}
