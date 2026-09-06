/*
 * echo_client.cpp — minimal TCP echo client.
 *
 * Connects once, then repeatedly: reads a line from stdin, sends it,
 * receives the echoed reply, prints it. EOF on stdin (Ctrl-D) ends the loop
 * and closes the connection.
 *
 * Same POSIX calls as the earlier echo_client.c (socket/connect/send/recv/
 * close, direct, no library in between). The one behavioral note: unlike
 * C's fgets(), std::getline() strips the trailing '\n' from what it reads,
 * so it's added back explicitly before send() — the wire protocol still
 * needs it.
 *
 * Usage: ./echo_client <host> <port>
 *   e.g. ./echo_client 127.0.0.1 5000
 */

#include <arpa/inet.h>   // inet_pton
#include <cstdio>        // std::printf, std::fprintf, perror
#include <cstdlib>       // std::atoi
#include <iostream>      // std::cin, std::getline
#include <netinet/in.h>  // struct sockaddr_in, htons
#include <string>
#include <sys/socket.h>  // socket, connect, send, recv
#include <unistd.h>      // close

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <host> <port>\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int port = std::atoi(argv[2]);

    // Step 1: create a TCP socket, same as the server.
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in addr{};  // value-initialized to zero, no memset needed
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        std::fprintf(stderr, "invalid host address: %s\n", host);
        return 1;
    }

    // Step 2: connect() actively opens the TCP connection to the server's
    // listening socket. This is where the three-way handshake happens.
    if (connect(sock_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        perror("connect");
        return 1;
    }

    std::printf(
        "echo_client: connected to %s:%d. Type a line + Enter (Ctrl-D to quit).\n",
        host, port
    );

    std::string line;

    // std::getline blocks on stdin, reading one line at a time (without the
    // trailing '\n'), until Ctrl-D signals end-of-file.
    while (std::getline(std::cin, line)) {
        std::string out = line + "\n";

        // Step 3: send the line to the server.
        if (send(sock_fd, out.data(), out.size(), 0) < 0) {
            perror("send");
            break;
        }

        // Step 4: receive the echoed reply. A real protocol cannot assume
        // one recv() == one message, but for this toy example one send
        // from the server is enough to read back what it echoed.
        char buf[4096];
        ssize_t n = recv(sock_fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            if (n < 0) {
                perror("recv");
            } else {
                std::printf("echo_client: server closed the connection.\n");
            }
            break;
        }
        buf[n] = '\0';  // NUL-terminate so we can printf() it as a C string

        std::printf("echo_client: echo: %s", buf);
    }

    // Step 5: close the connection.
    close(sock_fd);
    return 0;
}
