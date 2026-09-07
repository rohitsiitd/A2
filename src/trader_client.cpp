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
 * Server messages are asynchronous. A BOUGHT/SOLD notification fires when
 * some *other* trader's order crosses one of ours, so it can land at any
 * moment rather than as the answer to something we just sent. A client built
 * around a strict send-then-recv cycle per typed line would sit blocked at
 * the keyboard and not see those until the user happened to type again.
 * Polling stdin and the socket together lets each be handled the moment it
 * has something, independent of the other.
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

        // One recv() is not one message, so buffer and split on '\n'.
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

        // One line per wake-up is enough here: stdin is a terminal handing us
        // line-buffered input, so a single getline() returns exactly what the
        // user typed before pressing Enter. The socket needs no such luck.
        if (fds[0].revents & POLLIN) {
            std::string line;
            if (!std::getline(std::cin, line)) {
                break;  // Ctrl-D
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
