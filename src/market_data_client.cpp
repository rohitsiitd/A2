/*
 * market_data_client.cpp — Market-Data Client for The Socket Exchange.
 *
 * Usage: ./market_data_client <host> <port> <instrument>
 *   e.g. ./market_data_client 127.0.0.1 5000 JNST
 *
 * Connects, sends SUBSCRIBE <instrument> automatically, then prints TRADE
 * updates as they arrive.
 *
 * TRADE messages are pushed by the server whenever a matching order executes,
 * with nothing on this end having asked for them. Polling stdin and the
 * socket together means those keep arriving and printing while the user sits
 * mid-command, and that typing SUBSCRIBE or UNSUBSCRIBE never has to wait for
 * a quiet moment on the socket.
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
        std::fprintf(stderr, "usage: %s <host> <port> <instrument>\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int port = std::atoi(argv[2]);
    const char *instrument = argv[3];

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

    std::string subscribe = std::string("SUBSCRIBE ") + instrument + "\n";
    if (send(sock_fd, subscribe.data(), subscribe.size(), 0) < 0) {
        perror("send");
        return 1;
    }

    std::cout << "market_data_client: subscribed to " << instrument
              << ". Commands: SUBSCRIBE/UNSUBSCRIBE <inst>, QUIT.\n"
              << std::flush;

    pollfd fds[2];
    fds[0] = pollfd{STDIN_FILENO, POLLIN, 0};
    fds[1] = pollfd{sock_fd, POLLIN, 0};

    std::string inbuf;
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
                    std::cout << "market_data_client: server closed the connection.\n" << std::flush;
                } else {
                    perror("recv");
                }
                break;
            }
        }
        if (fds[1].revents & (POLLHUP | POLLERR)) {
            std::cout << "market_data_client: server closed the connection.\n";
            break;
        }

        if (fds[0].revents & POLLIN) {
            std::string line;
            if (!std::getline(std::cin, line)) {
                line = "QUIT";
            }
            if (line == "QUIT") {
                quitting = true;
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
