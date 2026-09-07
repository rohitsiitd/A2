/*
 * client_generator.cpp — open and hold many idle TCP connections to the
 * Exchange Server, for measuring what a connection costs at scale.
 *
 * Usage: ./client_generator <host> <port> <count> [num_source_ips]
 *   e.g. ./client_generator 127.0.0.1 5000 70000 8
 *
 * Connections are opened and then left alone; no data is ever sent. Ctrl-C
 * closes them all and exits.
 *
 * One source address reaches a single destination only as many times as it
 * has ephemeral ports — roughly 55,000 on FreeBSD's default range, which puts
 * a hard ceiling well under what this tool is meant to reach. An outbound
 * connection is identified by its (source IP, source port) pair, so spreading
 * across several source addresses gives each one its own independent pool.
 *
 * FreeBSD needs those extra addresses added explicitly, as root:
 *   ifconfig lo0 alias 127.0.0.2 netmask 255.255.255.255   (repeat per IP)
 * lo0 owns only 127.0.0.1. Its 0xff000000 netmask does NOT make the rest of
 * 127.0.0.0/8 local, so an unaliased 127.0.0.2 falls through to the default
 * route and bind() fails with EADDRNOTAVAIL. Linux binds the whole /8 to lo
 * and needs none of this — one of the places the two genuinely differ.
 */

#include <arpa/inet.h>   // inet_pton
#include <atomic>
#include <cerrno>        // errno
#include <chrono>
#include <csignal>       // std::signal, SIGINT, SIGTERM
#include <cstdio>        // std::printf, std::fprintf
#include <cstdlib>       // std::atoi, std::atol
#include <cstring>       // std::strerror
#include <netinet/in.h>  // struct sockaddr_in, htons
#include <string>
#include <sys/resource.h>  // getrlimit, setrlimit, RLIMIT_NOFILE
#include <sys/socket.h>  // socket, bind, connect, close
#include <thread>
#include <unistd.h>      // close
#include <vector>

namespace {

std::atomic<bool> g_stop{false};

void handle_signal(int) { g_stop.store(true); }

void raise_fd_limit() {
    rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

}  // namespace

int main(int argc, char *argv[]) {
    if (argc != 4 && argc != 5) {
        std::fprintf(stderr, "usage: %s <host> <port> <count> [num_source_ips]\n", argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int port = std::atoi(argv[2]);
    long count = std::atol(argv[3]);
    int num_source_ips = (argc == 5) ? std::atoi(argv[4]) : 1;
    if (num_source_ips < 1) num_source_ips = 1;

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    raise_fd_limit();  // every held connection costs this process a descriptor

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host, &dest.sin_addr) != 1) {
        std::fprintf(stderr, "invalid host address: %s\n", host);
        return 1;
    }

    std::vector<int> fds;
    fds.reserve(static_cast<std::size_t>(count));

    long connected = 0;
    long failed = 0;

    // Which call refused first, and why. A bare failure count is
    // undiagnosable: an exhausted ephemeral port range, an unaliased source
    // address and a full listen queue all present identically without this.
    int first_socket_errno = 0;
    int first_bind_errno = 0;
    int first_connect_errno = 0;

    for (long i = 0; i < count && !g_stop.load(); i++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            if (first_socket_errno == 0) first_socket_errno = errno;
            failed++;
            continue;
        }

        if (num_source_ips > 1) {
            // Cycle through 127.0.0.1, 127.0.0.2, ... so the connections draw
            // on several ephemeral-port pools rather than exhausting one.
            sockaddr_in src{};
            src.sin_family = AF_INET;
            src.sin_port = 0;  // let the kernel pick a free port for this address
            std::string src_ip = "127.0.0." + std::to_string(1 + (i % num_source_ips));
            inet_pton(AF_INET, src_ip.c_str(), &src.sin_addr);

            if (bind(fd, reinterpret_cast<sockaddr *>(&src), sizeof(src)) < 0) {
                if (first_bind_errno == 0) first_bind_errno = errno;
                close(fd);
                failed++;
                continue;
            }
        }

        if (connect(fd, reinterpret_cast<sockaddr *>(&dest), sizeof(dest)) < 0) {
            if (first_connect_errno == 0) first_connect_errno = errno;
            close(fd);
            failed++;
            continue;
        }

        fds.push_back(fd);
        connected++;

        if (connected % 1000 == 0) {
            std::printf("client_generator: %ld connected, %ld failed\n", connected, failed);
            std::fflush(stdout);
        }
    }

    std::printf(
        "client_generator: done -- %ld connected, %ld failed. Holding idle, Ctrl-C to stop.\n",
        connected, failed
    );
    if (first_socket_errno != 0) {
        std::printf("client_generator: first socket() failure: %s\n", std::strerror(first_socket_errno));
    }
    if (first_bind_errno != 0) {
        std::printf("client_generator: first bind() failure: %s\n", std::strerror(first_bind_errno));
    }
    if (first_connect_errno != 0) {
        std::printf("client_generator: first connect() failure: %s\n", std::strerror(first_connect_errno));
    }
    std::fflush(stdout);

    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::printf("client_generator: closing %zu connections...\n", fds.size());
    std::fflush(stdout);
    for (int fd : fds) close(fd);

    return 0;
}
