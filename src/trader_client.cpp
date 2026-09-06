/*
 * trader_client.cpp — Trader Client for The Socket Exchange.
 *
 * Usage: ./trader_client <host> <port> <username>
 *   e.g. ./trader_client 127.0.0.1 5000 alice
 *
 * Connects, sends LOGIN <username> automatically, then runs an interactive
 * loop: lines typed on stdin are sent as commands (BUY/SELL/CANCEL/QUIT),
 * and lines from the server are printed as they arrive.
 *
 * The protocol is explicit that server messages are asynchronous (section
 * 2.7): a BOUGHT/SOLD notification can arrive at any time, not just right
 * after this trader's own request, because it fires when some OTHER
 * trader's order matches this one. A client that did a strict "send a
 * command, then recv() the reply" cycle per typed line — the way
 * echo_client.cpp does — would miss any notification that arrives while
 * it's sitting at the keyboard waiting for the next line to be typed.
 * So this client poll()s TWO fds at once, stdin and the socket, and reacts
 * to whichever one has something first, independent of the other.
 */

#include <arpa/inet.h>   // inet_pton
#include <cstdio>        // std::fprintf, perror
#include <cstdlib>       // std::atoi
#include <iostream>      // std::cin, std::cout, std::getline
#include <netinet/in.h>  // struct sockaddr_in, htons
#include <poll.h>        // struct pollfd, poll, POLLIN
#include <string>
#include <sys/socket.h>  // socket, connect, send, recv
#include <unistd.h>      // close, STDIN_FILENO

namespace {
constexpr std::size_t kRecvChunk = 4096;
}  // namespace

int main(int argc, char *argv[]) {
    if (argc != 4) {
        std::fprintf(stderr, "usage: %s <host> <port> <username>\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int port = std::atoi(argv[2]);
    const char *username = argv[3];

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        std::fprintf(stderr, "invalid host address: %s\n", host);
        return 1;
    }

    if (connect(sock_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        perror("connect");
        return 1;
    }

    std::string login = std::string("LOGIN ") + username + "\n";
    if (send(sock_fd, login.data(), login.size(), 0) < 0) {
        perror("send");
        return 1;
    }

    std::cout << "trader_client: connected as " << username
              << ". Commands: BUY/SELL <inst> <qty> <price>, CANCEL <id>, QUIT.\n"
              << std::flush;

    pollfd fds[2];
    fds[0] = pollfd{STDIN_FILENO, POLLIN, 0};
    fds[1] = pollfd{sock_fd, POLLIN, 0};

    std::string inbuf;  // buffered, not-yet-framed bytes from the socket
    bool quitting = false;

    while (!quitting) {
        if (poll(fds, 2, -1) < 0) {
            perror("poll");
            break;
        }

        // Server -> us: buffer and frame on '\n', same reasoning as the
        // server side — one recv() is not one message.
        if (fds[1].revents & POLLIN) {
            char chunk[kRecvChunk];
            ssize_t n = recv(sock_fd, chunk, sizeof(chunk), 0);
            if (n > 0) {
                inbuf.append(chunk, static_cast<std::size_t>(n));
                for (;;) {
                    std::size_t nl = inbuf.find('\n');
                    if (nl == std::string::npos) break;
                    std::cout << "< " << inbuf.substr(0, nl) << "\n" << std::flush;
                    inbuf.erase(0, nl + 1);
                }
            } else {
                if (n == 0) {
                    std::cout << "trader_client: server closed the connection.\n" << std::flush;
                } else {
                    perror("recv");
                }
                break;
            }
        }
        if (fds[1].revents & (POLLHUP | POLLERR)) {
            std::cout << "trader_client: server closed the connection.\n";
            break;
        }

        // Us -> server: one line of stdin per poll() wakeup is fine here —
        // unlike the TCP socket, stdin is a terminal delivering
        // line-buffered input, so a single getline() reliably returns
        // exactly what the user just typed and pressed Enter on.
        if (fds[0].revents & POLLIN) {
            std::string line;
            if (!std::getline(std::cin, line)) {
                // EOF (Ctrl-D): leave gracefully rather than just vanishing.
               break;
            }
            std::string out = line + "\n";
            if (send(sock_fd, out.data(), out.size(), 0) < 0) {
                perror("send");
                break;
            }
        }
    }

    close(sock_fd);
    return 0;
}
