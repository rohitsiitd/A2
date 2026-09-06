# The Socket Exchange

## Language and build

C++ (direct POSIX sockets — instructor-confirmed exception to the handout's
"C or Python" list). No networking library is used anywhere (no Boost.Asio,
etc.) — every socket operation (`socket`, `bind`, `listen`, `accept`,
`connect`, `send`/`recv`, `close`, `shutdown`) is a direct POSIX call.

Compiler: `c++` (clang++ on FreeBSD), `-std=c++17`.

Build everything from the repository root:

```sh
make
```

This produces five binaries at the repository root: `echo_server`,
`echo_client` (the learning skeleton, not part of the graded system),
`exchange_server`, `trader_client`, and `market_data_client`.

## Running

Exchange Server:

```sh
./server/run-server <host> <port>
# e.g. ./server/run-server 127.0.0.1 5000
```

Trader Client (auto-sends `LOGIN <username>` on connect, then reads
commands from stdin and prints server messages — including asynchronous
`BOUGHT`/`SOLD` notifications — as they arrive):

```sh
./client/run-trader <host> <port> <username>
# e.g. ./client/run-trader 127.0.0.1 5000 alice
```

Market-Data Client (auto-sends `SUBSCRIBE <instrument>` on connect, then
prints incoming `TRADE` updates; SUBSCRIBE/UNSUBSCRIBE/QUIT can still be
typed interactively):

```sh
./client/run-market-data <host> <port> <instrument>
# e.g. ./client/run-market-data 127.0.0.1 5000 JNST
```

No configuration files or environment variables are required.

## Concurrency / I/O design

Single-threaded, `poll()`-based event loop — one thread services every
connection, no locking anywhere (there's nothing to lock: the order book,
client table, etc. are only ever touched from this one loop). Chosen over
threads or `kqueue`:

- Threads would need locking around shared exchange state (the order book,
  client table) and don't reduce per-connection resource cost, which is
  what actually limits scaling to many connections.
- `kqueue` is FreeBSD-native and more efficient for *very* large numbers of
  active/ready connections, but for the target scenarios here (10+ clients
  normally, up to 70,000 *idle* connections for the bonus) the expected
  bottleneck is file-descriptor and per-socket kernel memory limits, not
  the multiplexing algorithm — `poll()` only costs anything when it wakes
  up and scans for ready fds, and an idle connection doesn't wake it.
  `poll()` also has no hard connection-count ceiling the way `select()`
  does (no `FD_SETSIZE`).

Per-connection outbound data is queued (`Client::outq`) rather than sent
with a raw blocking `send()`: a slow or dead reader gets its own queue and
non-blocking sends, so it cannot stall delivery to any other connection
(a dead-reader/backpressure scenario is the assignment's Experiment 7).
`SIGPIPE` is ignored process-wide; a `send()` to an already-dead connection
fails with `EPIPE`/`ECONNRESET` instead of killing the server, and that one
connection is closed instead (Experiment 8).

## Bonus: scaling to many idle connections

`exchange_server` raises its own file-descriptor limit at startup
(`setrlimit(RLIMIT_NOFILE, ...)` toward the process's hard ceiling), but that
can't exceed the *system-wide* ceiling. A stock FreeBSD host will not reach
70,000 connections. Run the setup script as root first:

```sh
sudo sh bonus/setup-freebsd.sh            # apply now
sudo sh bonus/setup-freebsd.sh --persist  # apply now and survive reboot
```

It is idempotent and does two things, both documented inline in
`bonus/sysctl.conf` and `bonus/rc.conf`:

1. **Raises kernel limits.** `kern.maxfiles=200000`,
   `kern.maxfilesperproc=100000`, `kern.ipc.maxsockets=200000`,
   `kern.ipc.soacceptqueue=1024`. The server and `client_generator` run on
   the same host over loopback, so each connection costs *two* fds and *two*
   sockets system-wide — 70,000 connections needs ~140,000 of each, not
   70,000. `soacceptqueue` matters because `listen()` is called with a
   backlog of 1024 and the stock value of 128 silently clamps it.

2. **Adds `lo0` aliases for 127.0.0.2–127.0.0.5.** Required on FreeBSD: one
   source IP can hold only ~55,535 outbound connections to a single
   destination (the ephemeral port range), short of 70,000. Extra source
   addresses each get their own pool — but `lo0` owns *only* 127.0.0.1, and
   its `0xff000000` netmask does **not** make the rest of 127.0.0.0/8 local.
   An unaliased 127.0.0.2 falls through to the default route, so `bind()`
   fails with `EADDRNOTAVAIL`. (Linux binds the whole /8 to `lo` and needs
   no aliases — this is a real difference between the two systems.)

A process must be started *after* the script runs to inherit the raised
`RLIMIT_NOFILE` ceiling, so restart `exchange_server` if it was already up.

Then, to hold N idle connections across M source addresses:

```sh
./client_generator 127.0.0.1 5000 70000 4
```

Socket buffer sizes are deliberately left at FreeBSD's defaults
(`net.inet.tcp.sendspace`/`recvspace`). No reduction is needed: those are
high-water marks rather than preallocations, so idle connections carry no
data and allocate no mbufs. Measured cost at 70,000 connections was 328 MB
of kernel memory — 2.34 KB per endpoint of fixed per-socket structures
(`socket` 1024 B + `tcp_inpcb` 1320 B + port 32 B) — not the ~9 GB a naive
"64 KB × 140,000 endpoints" estimate would predict.

The client-generator program used to create and hold many idle connections
for that measurement is `src/client_generator.cpp` (built as
`client_generator`; not one of the three required launchers, since nothing
in the handout's submission structure calls for it beyond being included).
