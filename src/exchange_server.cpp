/*
 * exchange_server.cpp — the real exchange protocol on top of poll().
 *
 * Concurrency/I/O: poll(), single-threaded, one event loop, no locking —
 * finalized choice (see CLAUDE.md), including for the 70,000-connection
 * bonus. No threads, no kqueue: for idle connections specifically, poll()
 * only costs anything when it wakes up and scans for ready fds, and while
 * genuinely idle it just blocks. The expected bottleneck at that scale is
 * fd/memory limits, not the multiplexing algorithm (see the RLIMIT_NOFILE
 * bump in main()).
 *
 * This revision adds what the earlier version deliberately deferred:
 *   - non-blocking sockets + a per-connection outbound queue (`Client::outq`)
 *     so a slow/dead reader can never make a blocking send() stall the
 *     whole server (Experiment 7's backpressure concern). reply() now goes
 *     through send_or_queue(): try to send immediately; whatever doesn't
 *     fit is queued, and POLLOUT is requested for that fd so the poll loop
 *     is told exactly when there's room to drain more — no busy-looping,
 *     no polling "is it writable yet?" in a spin loop.
 *   - SIGPIPE is ignored process-wide (main()), so a send() to an already-
 *     dead socket returns -1/EPIPE instead of killing the whole server —
 *     handled per-connection like any other send failure (Experiment 8).
 *   - closes are now deferred: mark_for_close() just records an fd; nothing
 *     is actually erased from `clients`/`fds` until a single cleanup pass
 *     after the whole per-tick client loop finishes. This matters because
 *     matching/broadcasting can now fail-and-close a DIFFERENT connection
 *     than the one currently being processed — erasing it immediately
 *     mid-loop (via the old swap-with-last-element trick) could silently
 *     relocate an not-yet-visited entry into an already-visited slot, or
 *     invalidate the very reference the outer loop is using. Deferring
 *     until after the loop sidesteps that entirely: `clients`/`fds` never
 *     mutate structurally while the loop walks them, so every Client&
 *     obtained during a tick stays valid for that whole tick regardless of
 *     how many other connections get marked for closing along the way.
 *
 * Everything else — role inference, command parsing, order storage and
 * matching (BOUGHT/SOLD/TRADE) — is unchanged from the previous revision.
 *
 * Usage: ./exchange_server <host> <port>
 *   e.g. ./exchange_server 127.0.0.1 5000
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

// Generous enough that a sudden burst of connection attempts (e.g. the
// bonus's client-generator opening tens of thousands of connections) isn't
// refused just because poll() hasn't gotten back around to accept()ing yet.
constexpr int kBacklog = 1024;
// Sanity cap on unterminated buffered input per connection — not a
// container limit, just a guard against a client that never sends '\n'.
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

// The reverse of parse_instrument: turn our internal enum back into the
// wire-format token, for building outgoing TRADE/BOUGHT/SOLD messages.
const char *instrument_name(Instrument inst) {
    return (inst == Instrument::JNST) ? "JNST" : "IMCT";
}

// --- Small parsing helpers ---------------------------------------------
// std::from_chars (not std::stoi) because it reports failure without
// exceptions and tells us exactly where it stopped, the same thing strtol's
// `end` pointer gave the C version — needed to confirm the WHOLE token was
// digits, not just a valid prefix of one ("12abc" must be rejected).
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
// Keyed by fd in the `clients` map below. client_id is a separate identity
// from fd: fds get reused by the OS once closed, but an order must always
// trace back to the exact connection that placed it, even after that fd
// number gets recycled by a later, unrelated client. `outq` is bytes queued
// to send but not yet accepted by the kernel — see send_or_queue().
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
// `open` is true while an order still has unfilled quantity resting in the
// book, set to false by either a CANCEL or match_order() filling it to 0.
// A partially-filled order stays open with `qty` reduced to what remains.
struct Order {
    int id = 0;
    int owner_client_id = 0;
    Instrument inst = Instrument::JNST;
    Side side = Side::Buy;
    int price = 0;
    int qty = 0;
    bool open = true;
};

// All of this is central, single-threaded exchange state — no locking
// needed since one poll() loop is the only thing that ever touches it.
// `orders` grows via push_back only from BUY/SELL handling, and never
// while a pointer/reference into it is held across a push_back — that
// invariant is what keeps Order* from find_open_order() and the Order&
// passed into match_order() safe to use without becoming dangling.
std::vector<pollfd> fds;
std::unordered_map<int, Client> clients;  // keyed by fd
std::vector<Order> orders;
int next_client_id = 0;
int next_order_id = 0;

// Connections queued to close once the current tick's client loop finishes
// (see the big header comment for why this is deferred rather than
// immediate). May contain the same fd more than once; harmless, the
// cleanup pass just skips whatever's already gone.
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

// client_id is stable for the connection's whole lifetime, but its slot in
// `clients` is not (a disconnect erases it outright) — this walks the live
// clients to find whichever one currently holds that client_id, or nullptr
// if that trader/subscriber has since disconnected, which a match arriving
// after they leave must handle gracefully.
Client *find_client_by_id(int client_id) {
    for (auto &[fd, c] : clients) {
        (void)fd;
        if (c.client_id == client_id) return &c;
    }
    return nullptr;
}

// Try to send `data` on `fd` right now; whatever the kernel won't take yet
// goes into that connection's outq, with POLLOUT requested so the poll
// loop finds out the moment there's room to drain more. If `outq` is
// already non-empty, this connection is already backed up — data is just
// appended, preserving order, without attempting a send at all (avoids
// sending new bytes ahead of older still-queued ones).
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
            // EPIPE (peer closed, SIGPIPE ignored in main()), ECONNRESET,
            // etc. — the peer is gone; let the deferred cleanup handle it.
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

// Build one line, append '\n', hand it to send_or_queue(). No va_list/
// vsnprintf needed in C++: std::string concatenation does the formatting.
void reply(int fd, const std::string &msg) {
    send_or_queue(fd, msg + "\n");
}

// Send a TRADE notification to every currently-connected market-data client
// subscribed to `inst`. Each of these can independently end up queued
// rather than sent immediately (see send_or_queue) — a slow subscriber no
// longer blocks delivery to any of the others.
void broadcast_trade(Instrument inst, int qty, int price) {
    for (auto &[fd, c] : clients) {
        if (c.role == Role::MarketData && c.subscribed[static_cast<std::size_t>(inst)]) {
            reply(fd, std::string("TRADE ") + instrument_name(inst) + " " +
                          std::to_string(qty) + " " + std::to_string(price));
        }
    }
}

// Attempt to match a newly-accepted order `o` against the resting book.
// `orders` is append-only in acceptance order, so scanning it front-to-back
// naturally checks the oldest resting orders first (simple FIFO/time
// priority) before newer ones. The loop keeps trading against successive
// matches until either `o` is fully filled (o.qty hits 0) or the whole book
// has been scanned once — a single incoming order can cross several resting
// orders in one call (e.g. a big BUY eating three small SELLs at the same
// price). Existing resting orders never need to be re-matched against each
// other: the invariant is that the book never holds two crossable orders at
// rest, only ever right after a NEW order arrives, which is exactly when
// this function runs.
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

        // BOUGHT/SOLD are private to each order's own owner, not to
        // whichever connection's incoming message caused this match — so
        // both lookups are needed, and either owner may already be gone
        // (they disconnected after placing the resting order); nullptr
        // here just means there's no one left to notify.
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

// Set EXCHANGE_DEBUG=1 to trace every recv(). Deliberately an environment
// variable rather than an argv flag: the experiment harness invokes
// ./server/run-server with exactly <host> <port> and main() rejects any
// other argc, so there is nowhere to put a flag without breaking it.
bool debug_enabled() {
    static const bool on = (std::getenv("EXCHANGE_DEBUG") != nullptr);
    return on;
}

// Render bytes so framing is visible: a '\n' has to be distinguishable from
// a line break in the log itself, otherwise the trace can't show where one
// application message actually ends.
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

    // A send() to a peer that has already closed its end would otherwise
    // deliver SIGPIPE, whose default disposition kills the whole process.
    // Ignoring it means send() just returns -1/EPIPE instead, handled like
    // any other per-connection send failure in send_or_queue().
    std::signal(SIGPIPE, SIG_IGN);

    // Bonus: raise this process's fd limit toward its hard ceiling. A
    // default `ulimit -n` (often ~1024) would otherwise cap accepted
    // connections far below the 70,000-connection bonus target regardless
    // of poll() vs. any other I/O model. Best-effort: if the system-wide
    // ceiling itself is low, this can't exceed it (that needs raising via
    // sysctl as documented in README.md), so a failure here is not fatal.
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

    // The accept loop below drains the queue until it is empty, which means
    // the last accept() of each burst has nothing left to return. On a
    // blocking listening socket that call would park the whole event loop
    // until some unrelated client happened to connect; non-blocking makes it
    // return EWOULDBLOCK instead, which is the loop's termination condition.
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

        // --- New connections. poll() is level-triggered: it reports the
        // listening socket as readable when at least one connection is
        // pending, but never says how many. Accepting only one per wake-up
        // therefore costs a whole poll() cycle -- O(n) over every fd already
        // in the set -- per connection, so during a burst the drain rate
        // falls further behind the arrival rate the more clients are already
        // connected. The kernel's accept queue then overflows (FreeBSD drops
        // the excess in sonewconn() without sending RST, leaving the peer
        // ESTABLISHED against a socket that no longer exists). Draining until
        // EWOULDBLOCK keeps the queue empty regardless of arrival burst size.
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
                    // being accepted (the harness's startup probe does this
                    // deliberately). That kills one pending connection, not
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

                // revents is explicitly 0: the scan below skips entries with
                // no events, so a client accepted this tick is simply picked
                // up on the next poll() rather than read with a stale mask.
                fds.push_back(pollfd{client_fd, POLLIN, 0});

                Client c;
                c.fd = client_fd;
                c.client_id = next_client_id++;
                c.role = Role::Unknown;
                clients.emplace(client_fd, std::move(c));
            }
        }

        // --- Existing clients. Neither `fds` nor `clients` is structurally
        // changed anywhere in this loop (see mark_for_close/pending_close
        // above) — every Client& obtained here stays valid for the whole
        // tick no matter what else gets marked for closing along the way.
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
                        // Stop asking for "writable" once there's nothing
                        // left to write — a drained socket is writable
                        // almost all the time, so leaving POLLOUT set would
                        // make poll() wake up on it every single call.
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

        // --- Deferred cleanup: now safe to actually mutate `fds`/`clients`.
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
