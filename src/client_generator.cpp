/*
 * client_generator.cpp — Bonus: open and hold many idle TCP connections to
 * the Exchange Server, for measuring resource usage at scale (up to the
 * 70,000-connection target). Not one of the three required launchers —
 * a standalone tool for the bonus experiment.
 *
 * Usage: ./client_generator <host> <port> <count> [num_source_ips]
 *   e.g. ./client_generator 127.0.0.1 5000 70000 8
 *
 * A single source IP only has as many usable outbound connections to one
 * destination as its ephemeral port range has ports — on FreeBSD's default
 * range that's on the order of 55,000, short of the 70,000 target by
 * itself. Each (source IP, source port) pair is a distinct identity for an
 * outbound connection, though, so binding across several source addresses
 * (127.0.0.1, 127.0.0.2, ...) gives each one its own independent ephemeral
 * pool.
 *
 * On FreeBSD those extra addresses must be added explicitly, as root:
 *   ifconfig lo0 alias 127.0.0.2 netmask 255.255.255.255   (repeat per IP)
 * lo0 owns only the single address 127.0.0.1 -- its 0xff000000 netmask does
 * NOT make the rest of 127.0.0.0/8 local, and an unaliased 127.0.0.2 falls
 * through to the default route instead, so bind() fails with EADDRNOTAVAIL.
 * (Linux does bind the whole /8 to lo and needs no aliases; this is one of
 * the places the two systems genuinely differ.)
 *
 * Connections are opened and then left alone — no data is ever sent,
 * matching the bonus's "idle connections" requirement. Ctrl-C closes
 * everything and exits.
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
    raise_fd_limit();  // this process also needs a high fd ceiling to open that many sockets

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

    // Which call first refused, and why. Without this a failure count alone
    // is undiagnosable: an exhausted ephemeral port range, an unaliased
    // source address, and a full listen queue all just look like "failed".
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
            // Cycle the source address through 127.0.0.1, 127.0.0.2, ...
            // so each gets its own independent ephemeral-port pool instead
            // of all connections competing for one shared pool.
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
