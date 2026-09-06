/*
 * echo_server.cpp — TCP echo server: concurrent clients (poll()) PLUS
 * per-connection input buffering / newline framing.
 *
 * Same design as the earlier echo_server.c, now in C++ (instructor-confirmed
 * exception to the handout's C/Python language list). The POSIX calls are
 * identical either way — socket/bind/listen/accept/poll/recv/send/close,
 * called directly, no networking library involved. What C++ changes is
 * everything AROUND those calls:
 *   - std::string replaces the fixed char[BUF_SIZE] + manual memchr/memmove
 *     buffer-compaction dance. find('\n') and erase(0, n) do the same job.
 *   - std::vector<pollfd> + std::unordered_map<int, Client> (keyed by fd)
 *     replace the fixed-size fds[]/clients[] arrays and the "swap the last
 *     slot into the gap to keep two parallel arrays aligned" bookkeeping —
 *     an fd's Client is just erased by key, and there's no more MAX_CLIENTS
 *     cap (the containers grow as needed).
 *
 * Framing is still the "TCP is a byte stream" rule: a single recv() must
 * NOT be treated as one message. Each connection keeps its own inbuf;
 * incoming bytes are appended, and whenever a '\n' shows up anywhere in it,
 * everything before it is one complete message.
 *
 * Usage: ./echo_server <host> <port>
 *   e.g. ./echo_server 127.0.0.1 5000
 */

#include <arpa/inet.h>   // inet_pton, inet_ntoa
#include <cerrno>        // errno, EINTR
#include <cstdio>        // std::printf, std::fprintf, perror
#include <cstdlib>       // std::atoi
#include <netinet/in.h>  // struct sockaddr_in, htons, ntohs
#include <poll.h>        // struct pollfd, poll, POLLIN, POLLHUP, POLLERR
#include <string>
#include <sys/socket.h>  // socket, bind, listen, accept, recv, send
#include <unistd.h>      // close
#include <unordered_map>
#include <vector>

namespace {

constexpr int kBacklog = 10;
// Sanity cap on unterminated buffered input (no '\n' yet) per connection —
// not a container limit (std::string grows on its own), just a guard
// against a client that never sends a newline.
constexpr std::size_t kMaxInbufBytes = 4096;
constexpr std::size_t kRecvChunk = 4096;

// Per-connection state, keyed by fd in the `clients` map below.
struct Client {
    int fd = -1;
    std::string inbuf;
};

}  // namespace

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <host> <port>\n", argv[0]);
        return 1;
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

    sockaddr_in addr{};  // value-initialized to all zero bits, no memset needed
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

    std::printf("echo_server: listening on %s:%d\n", host, port);
    std::fflush(stdout);

    // fds is what poll() watches, a flat contiguous array (poll() needs a
    // real array/pointer, hence vector rather than e.g. a map). clients
    // holds OUR per-connection state, keyed by fd — a disconnect is just
    // one erase() by key; no more parallel-array index bookkeeping.
    // fds[0] is always the listening socket and has no Client entry.
    std::vector<pollfd> fds;
    std::unordered_map<int, Client> clients;

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

        // --- New connections.
        if (fds[0].revents & POLLIN) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);

            int client_fd = accept(
                listen_fd,
                reinterpret_cast<sockaddr *>(&client_addr),
                &client_len
            );

            if (client_fd < 0) {
                perror("accept");
            } else {
                std::printf(
                    "echo_server: client connected from %s:%d (fd=%d)\n",
                    inet_ntoa(client_addr.sin_addr),
                    ntohs(client_addr.sin_port),
                    client_fd
                );
                std::fflush(stdout);

                fds.push_back(pollfd{client_fd, POLLIN, 0});
                clients.emplace(client_fd, Client{client_fd, std::string()});
            }
        }

        // --- Existing clients.
        for (std::size_t i = 1; i < fds.size(); i++) {
            if (!(fds[i].revents & (POLLIN | POLLHUP | POLLERR))) {
                continue;
            }

            Client &c = clients.at(fds[i].fd);
            bool drop = false;

            if (c.inbuf.size() >= kMaxInbufBytes) {
                std::fprintf(stderr, "echo_server: fd=%d message too long, dropping\n", c.fd);
                drop = true;
            } else {
                // recv() into a small stack buffer, then append to the
                // connection's own std::string — one extra copy versus
                // reading straight into the tail of a persistent buffer,
                // traded for not having to manage that buffer by hand.
                char chunk[kRecvChunk];
                ssize_t n = recv(c.fd, chunk, sizeof(chunk), 0);

                if (n > 0) {
                    std::printf(
                        "echo_server: fd=%d recv() %zd bytes: \"%.*s\"\n",
                        c.fd, n, static_cast<int>(n), chunk
                    );
                    std::fflush(stdout);

                    c.inbuf.append(chunk, static_cast<std::size_t>(n));

                    // Pull out every complete message currently sitting in
                    // the buffer. There can be zero (nothing complete yet),
                    // one, or several (multiple messages arrived in this
                    // one recv()) — the loop handles all three by
                    // re-scanning after each extraction.
                    for (;;) {
                        std::size_t nl = c.inbuf.find('\n');
                        if (nl == std::string::npos) {
                            break;  // no complete message left in the buffer
                        }

                        std::string line = c.inbuf.substr(0, nl);

                        std::printf(
                            "echo_server: fd=%d framed message: \"%s\"\n",
                            c.fd, line.c_str()
                        );
                        std::fflush(stdout);

                        std::string out = line + "\n";
                        if (send(c.fd, out.data(), out.size(), 0) < 0) {
                            perror("send");
                            drop = true;
                            break;
                        }

                        c.inbuf.erase(0, nl + 1);  // drop the message + its '\n'
                    }
                } else if (n == 0) {
                    std::printf("echo_server: fd=%d client disconnected\n", c.fd);
                    std::fflush(stdout);
                    drop = true;
                } else {
                    perror("recv");
                    drop = true;
                }
            }

            if (drop) {
                close(fds[i].fd);
                clients.erase(fds[i].fd);
                fds[i] = fds.back();
                fds.pop_back();
                i--;
            }
        }
    }
}
