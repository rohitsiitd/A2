# Project: A2 — The Socket Exchange (IIT Delhi networking assignment)

A simplified trading system used as a vehicle to learn TCP socket programming.
The trading logic is intentionally minimal; the real focus is the networking:
TCP connections, concurrent clients, message framing, blocking/non-blocking I/O,
connection termination, and OS-level behavior. Deadline: 10 Sep 2026.

## Who I am / how to help me
- I know **C++** well — that's my native language here, not something to be
  taught from scratch. Explanations of new code should focus on the networking
  semantics (why a call behaves the way it does, what the protocol/OS is doing)
  rather than basic C++ syntax.
- I learn best by building **small, fully working examples** and understanding
  every line before adding complexity. Do not jump ahead or skip steps.

## Language & rules
- Implementation language: **C++** (direct POSIX sockets). Originally built in
  C per the handout's literal "C or Python" list; switched 2026-09-06 after
  **instructor/LMS confirmation that C++ is an accepted exception** for this
  assignment. The handout PDF in this repo still shows the old C/Python
  wording — that's expected, the confirmation isn't reflected in the static
  file.
- Required socket calls to use directly: `socket`, `bind`, `listen`, `accept`,
  `connect`, `send`/`write`, `recv`/`read`, `close`, `shutdown`.
- **Not allowed**: any high-level networking framework/library that hides the
  socket layer — no Boost.Asio, libevent, libuv, Python asyncio, HTTP/WebSocket/RPC
  frameworks, or third-party connection/event-loop/framing libraries. Raw POSIX
  sockets only, called directly — this applies to the C++ standard library just
  as much as it did to C (i.e. `std::string`/`std::vector`/etc. for ordinary
  data handling are fine; there's no C++ networking library involved anywhere).
  Standard OS mechanisms (`select`, `poll`, `kqueue`) ARE allowed.

## Environment & workflow
- Development is **split**: I edit source on **Windows** (`D:\A2`, source of
  truth), then it's pushed into a **FreeBSD 14.4-RELEASE VM** (VirtualBox) and
  compiled/run **inside the VM**. `D:\A2` mirrors the submission zip layout
  exactly — no separate "dev layout".
- Sync/build: `.\deploy.ps1` (repo root). Packs `src/`, `server/`, `client/`,
  `Makefile` etc. into one tar, `scp`s it, unpacks over SSH into `~/A2` in the
  VM, fixes CRLF + exec bits on the launchers (NTFS drops both), then runs
  `make`. Flags: `-NoBuild` (sync only), `-Clean` (wipe `~/A2` first — deletes
  files removed on Windows; off by default), `-Exec "<cmd>"` (run after build).
  **Never edit files directly in `~/A2`** — next deploy overwrites them.
- SSH: host port 2222 -> guest port 22, user `blatonan`. Key auth is set up:
  `~/.ssh/id_ed25519_freebsd` (no passphrase — no ssh-agent running), alias
  `bsd` in `~/.ssh/config`. Claude Code **can** run commands in the VM directly
  over this (`ssh bsd "..."`) — e.g. compile and read `clang` errors itself —
  not just via deploy.ps1. Interactive-only work (tmux panes, live `tcpdump`,
  the Ctrl-C-driven experiment phases) stays a manual terminal you keep open.
- Everything (server + all clients + `experiment.py`) runs as separate processes
  **inside the same VM**, communicating over the **loopback interface** (127.0.0.1).
- FreeBSD investigation tools used for the experiments: `sockstat`, `netstat`,
  `tcpdump`, `procstat`, `ktrace`. (These, plus `kqueue`, are why the assignment
  mandates FreeBSD rather than Linux — `kqueue` does not exist on Linux.)
  Use `tmux` split panes for experiments — one screenshot showing server output
  + tool output together is much better evidence than separate captures.

## What we're building
Three programs, one text-based protocol over TCP, newline (`\n`) framed.

**Exchange Server** — central server. Listens on TCP, holds a separate connection
per client, distinguishes Trader vs Market-Data clients, matches orders, sends
responses and async notifications. Must handle multiple clients concurrently and
survive a client disconnecting (one client's failure must not kill the server or
stall others). Must support >= 10 simultaneous clients (>= 2 traders, >= 4
market-data); must not assume a fixed client count.

**Trader Client** — sends orders, receives responses/executions.

**Market-Data Client** — read-only; subscribes to instruments, receives TRADE
updates automatically.

### Protocol (integers only; quantity & price are positive ints; order IDs are
### non-negative ints, unique for the server's lifetime). Instruments: JNST, IMCT.
Trader -> Server: `LOGIN <user>`, `BUY <inst> <qty> <price>`,
  `SELL <inst> <qty> <price>`, `CANCEL <order_id>`, `QUIT`
Market-Data -> Server: `SUBSCRIBE <inst>`, `UNSUBSCRIBE <inst>`, `QUIT`
Server -> Trader: `OK`, `ERROR <reason>`, `ORDER_ACCEPTED <id>`,
  `ORDER_CANCELLED <id>`, `BOUGHT <inst> <qty> <price>`, `SOLD <inst> <qty> <price>`
Server -> Market-Data: `OK`, `ERROR <reason>`, `TRADE <inst> <qty> <price>`
- `OK` acknowledges a successful LOGIN/SUBSCRIBE/UNSUBSCRIBE. A successful CANCEL
  returns `ORDER_CANCELLED <id>` instead.
- On a valid BUY/SELL: send `ORDER_ACCEPTED <id>` first, then place in order book.
- Match rule: same instrument, opposite side, **exactly equal price**. Traded
  quantity = min of the two remaining quantities; remainder stays in the book.
- BOUGHT/SOLD are private to the trader whose order executed. TRADE goes to all
  Market-Data clients subscribed to that instrument.
- A client may not use commands outside its role.

### Message framing (critical)
TCP is a byte stream: one `send` != one `recv`. A single message may arrive across
multiple `recv` calls, and multiple messages may arrive in one `recv`. Must buffer
per-connection and split on `\n` to reconstruct application messages. Do NOT assume
one recv == one message.

## Bonus VM setup (REQUIRED before any large-scale run)
The 70,000-connection bonus does not run on a stock FreeBSD box. Setup lives
in `bonus/` and is idempotent:
```sh
sudo sh bonus/setup-freebsd.sh --persist   # --persist also writes /etc/{sysctl,rc}.conf
```
- `bonus/sysctl.conf` — `kern.maxfiles=200000`, `kern.maxfilesperproc=100000`,
  `kern.ipc.maxsockets=200000`, `kern.ipc.soacceptqueue=1024`. Server and
  generator share the host, so 70,000 connections cost ~140,000 fds and
  sockets *system-wide*, not 70,000.
- `bonus/rc.conf` — `lo0` aliases for 127.0.0.2–.5. **Mandatory on FreeBSD**:
  `lo0` owns only 127.0.0.1 and the `0xff000000` netmask does NOT make the
  rest of 127/8 local — an unaliased 127.0.0.2 falls through to the default
  route, so `bind()` fails `EADDRNOTAVAIL`. (Linux binds the whole /8 to `lo`;
  this is a genuine FreeBSD/Linux difference.) Symptom when missing: an exact
  50/50 connected/failed split, since the generator alternates addresses.
- A process must be **started after** this to inherit the raised
  `RLIMIT_NOFILE` ceiling — restart `exchange_server` if it was already up.
- Buffer sizes were **never reduced**: every tier ran with stock
  `net.inet.tcp.sendspace=32768` / `recvspace=65536`. No reduction was needed
  because those are high-water marks, not preallocations — idle connections
  carry no data and so allocate no mbufs. Only the fixed per-socket structs
  cost anything (2.34 KB/endpoint), which is why 70,000 connections came to
  328 MB rather than the ~9 GB a naive 64 KB × 140,000 estimate suggests.

## Concurrency / I/O decision (finalized 2026-09-06)
**`poll()`, single-threaded, everywhere — including the bonus.** No `kqueue`,
no threads. Reasoning to reuse in the report/viva: for *idle* connections
specifically (the bonus's exact scenario, and largely true of normal trading
traffic too), `poll()`'s O(n) cost only bites when it wakes up and has to scan
for which fds are ready — while genuinely idle it just blocks doing nothing.
The expected bottleneck at 70,000 connections is file-descriptor limits and
per-socket kernel memory, not the multiplexing algorithm — a legitimate,
measurable answer to the bonus's own analysis question ("would changing the
I/O mechanism help the bottleneck you found — why or why not"), without
needing a second (kqueue-based) implementation to build, test, and explain.

**Measured 2026-09-06 — the prediction was half right.** Ran the full tier
ramp to 70,000 idle connections (0 failures). The "idle connections never
wake poll()" half held exactly: zero spurious wakeups at 70,000. The
conclusion drawn from it ("so they cost nothing") did **not**. Identical
protocol check with 70,000 idle held vs. torn down:

| Operation | 70,000 idle | ~0 idle | Penalty |
|---|---|---|---|
| connect | 14.6 ms | 3.7 ms | 3.9× |
| LOGIN round-trip | 11.0 ms | 1.4 ms | 7.9× |
| BUY round-trip | 13.0 ms | 1.1 ms | 11.8× |
| fill notification | 0.1 ms | 0.1 ms | 1.0× (control) |

Every `poll()` call copies the whole 70,001-entry `pollfd` array
userspace→kernel (~560 KB), the kernel scans all of it, copies it back, and
our loop scans all 70,001 for non-zero `revents`. **Per-event latency is
O(total connections), not O(active connections)** — an active client's
request pays for all 70,000 idle ones. The fill notification is the control:
unaffected, because it's delivered inside the same tick as the work that
produced it and needs no fresh wake-up.

So the viva answer is two-sided and both sides are measured:
- **Capacity** (how many can be held): fd limits + `socket` UMA zone +
  kernel memory. `kqueue` would help **not at all** — identical under any
  multiplexing model.
- **Latency** (responsiveness while holding them): this *is* the
  multiplexing algorithm, and `kqueue` **would** help — its cost is
  O(ready events) with one-time registration vs. `poll()`'s O(total fds)
  per call. ~12× degradation measured, so this is quantified, not asserted.

Still not building a kqueue implementation — but the honest framing is now
"we measured where poll() costs us and can say exactly what kqueue would
fix", which is stronger than the original a-priori argument.

## Current status (as of 2026-09-06)
Implementation is **functionally complete, including the bonus's code side**.
What's left is entirely experiments/report (see below) — deliberately not
started yet on my own instruction (I said not to run experiments for now).

- FreeBSD VM set up; SSH key auth working from Windows; `deploy.ps1` and all
  five launchers/binaries work. **Fixed a real bug in `deploy.ps1`**: the
  VM's clock had drifted ~5.5h ahead of the host's, so `tar`-preserved
  source mtimes could look older than an already-built binary's VM-clock
  timestamp, making `make` silently skip rebuilding changed files. Fix:
  `deploy.ps1` now `touch`es every just-extracted file to the VM's own
  current clock right after unpacking, so staleness comparisons never cross
  two different clocks. Confirmed via a `-Clean` full rebuild + rerunning
  the functional tests below.
- All five programs exist, are C++ (`src/*.cpp`, instructor-confirmed
  exception to the handout's C/Python list), build clean with no warnings:
  `echo_server`/`echo_client` (concurrency+framing skeleton), `exchange_server`,
  `trader_client`, `market_data_client`. Plus `client_generator` (bonus tool,
  not one of the three required launchers).
- **Concurrency/I/O: finalized as `poll()` only, single-threaded, no
  threads/kqueue** — including for the bonus. Reasoning: idle connections
  don't make poll() do any work; expected 70k bottleneck is fd/memory
  limits, not the multiplexing algorithm (see CLAUDE.md's dedicated section
  above for the full reasoning to reuse in the report/viva).
- `exchange_server` implements the full protocol: role inference, command
  parsing/validation, order storage + matching (BOUGHT/SOLD/TRADE),
  **non-blocking sockets + per-connection outbound queues** (a slow/dead
  reader can no longer stall any other connection — Exp 7's concern), and
  **`SIGPIPE` ignored + EPIPE/ECONNRESET handled per-connection** (a killed
  market-data client can no longer crash the server — Exp 8's concern).
  Closes are deferred to a single post-tick cleanup pass specifically so a
  broadcast/match failing on connection X mid-processing of connection Y's
  message can't corrupt the poll loop's iteration or invalidate references
  — see the header comment in exchange_server.cpp for why.
- Bonus code: `RLIMIT_NOFILE` raised at startup (both `exchange_server` and
  `client_generator`), `listen()` backlog raised to 1024 (was 10 — a burst
  of connection attempts at scale could otherwise be refused), and
  `client_generator.cpp` supports cycling outbound sockets across multiple
  127.0.0.x source addresses to get past the ~55k single-source-IP
  ephemeral-port ceiling.
- **Bonus RUN AND PASSED 2026-09-06: 70,000 idle connections, 0 failures**,
  protocol still fully working at that scale (LOGIN/SUBSCRIBE/BUY/SELL
  matching/BOUGHT/SOLD/TRADE all verified live). Tiers: 1k/5k/15k/30k/50k/70k.
  Measured cost: **2.34 KB kernel memory per endpoint** (1024 B `socket` +
  1320 B `tcp_inpcb` + 32 B port), 4.69 KB per loopback pair, ~200 B/conn
  userspace RSS. At 70,000: 328 MB kernel memory, 15 MB server RSS, 26% CPU
  during the ramp, 0.85 s to establish all of them. Ceilings at that point:
  socket zone 140,026/200,000, `kern.openfiles` 140,099/200,000, per-process
  fds 70,008/100,000 — all ~70%. Note `vmstat`'s free-RAM column is
  misleading here (it reports free *pages*); `vmstat -z` UMA zone stats are
  what actually track per-connection cost.
- **Two real bugs found and fixed by running it** (see git log):
  1. `exchange_server` accepted exactly **one connection per `poll()`
     wake-up**, so draining N queued connections cost N full O(n) poll
     cycles — quadratic during a ramp. The kernel accept queue overflowed
     (`sonewconn: Listen queue overflow: 1537 already in queue`, = FreeBSD's
     3/2 × backlog grace) and **~1,520 connections were silently orphaned**:
     handshake completed so the client saw `connect()` succeed, then dropped
     before `accept()`, with **no RST sent** — peer stuck ESTABLISHED against
     a socket that no longer exists. At 5,000 the generator reported only 211
     failures while the server actually held just 3,269. Fixed by making the
     listening socket non-blocking and draining `accept()` until
     `EWOULDBLOCK` (`ECONNABORTED`/`EINTR` continue rather than abort — the
     harness's startup probe RSTs deliberately). After: 5,000/5,000, exact
     fd match, zero overflows. **The fix did not make the ramp faster
     (0.22→0.23 s) — it made it correct.**
  2. `client_generator`'s comment claimed 127/8 needs no aliases on FreeBSD.
     False (see Bonus VM setup above); cost us a whole tier to diagnose from
     a 50/50 split, because failures discarded `errno`. Now records and
     reports the first `errno` per failing call.
- Timing caveat worth keeping: connect time 0.22 s (15k) → 0.51 s (30k) is
  **not** server degradation — 30k was the tier that went 1→2 source IPs,
  adding a `bind()` per connection. Held at 2 IPs, 30k = 0.512 s and
  50k = 0.550 s, i.e. 1.67× the connections for 7% more time. Accept path
  does not degrade with n after the fix.
- Verified via scripted functional tests (not the graded `experiment.py`,
  throwaway scripts in the session's scratchpad): order matching + partial
  fills + cancel + role enforcement (13/13 checks), SIGPIPE resilience —
  server survives an abruptly-killed market-data client and stays
  responsive (4/4 checks), both real clients end-to-end via their launchers
  (async BOUGHT/SOLD/TRADE delivery confirmed independent of each client's
  own request/response timing), and `client_generator` (20 connections,
  0 failures, clean shutdown).
- Experiments actually run so far (before this session's "no experiments"
  instruction kicked in): **Exp 1** (listening vs connected socket) done,
  clean `sockstat`/`netstat`/`procstat` evidence captured. **Exp 7**
  (backpressure) was run once against the then-naive server (plain blocking
  `send()`, before this session's fix): the slow market-data client's
  Recv-Q plateaued at exactly ~85,000 bytes (the full 5000-trade payload) —
  FreeBSD's auto-tuned receive buffer absorbed the whole burst without
  actually stalling anything. That finding is still valid evidence for the
  report (it's what a naive implementation does under these exact
  conditions), but it's now a *before* data point — the backpressure fix
  described above is the *after*, not yet re-run against Exp 7 to compare.
  Exps 2/3/4/5/6/8 not yet run; report.pdf/README's experiment sections not
  yet written (README.md's build/run/design sections ARE done).

## Submission structure (strict — checker enforces it)
```
A2_<ROLL1>_<ROLL2>.zip/
  server/run-server            # launcher, must exec the server, pass args through
  client/run-trader            # launcher
  client/run-market-data       # launcher
  src/<source code>
  Makefile (optional)
  README.md
  report.pdf                   # answers + screenshots for the experiments
```
Launchers exist already (`server/run-server`, `client/run-trader`,
`client/run-market-data`), executable, LF-only. They resolve the binary via
`$(dirname "$0")` rather than the handout's literal `./exchange_server` example
— the harness's CWD is the submission root when it runs `./server/run-server`,
so a bare relative path would resolve wrong. Binaries expected one level up
from each launcher's directory (i.e. at the submission root) — adjust if the
Makefile puts them elsewhere.

## experiment.py — hard constraints on the implementation
Read in full; these are non-negotiable, not just descriptive:
- **Harness never runs `run-trader`/`run-market-data`.** It only launches
  `./server/run-server 127.0.0.1 5000` (fixed port) and talks to the server
  itself via raw sockets it opens. The two client launchers still must work
  (structure checker + viva) but no experiment exercises them.
- **No role handshake.** A connection starts role-unknown; the harness sends
  `LOGIN <user>\n` for a trader or `SUBSCRIBE <inst>\n` for market-data with
  no preamble. Role must be inferred from the first valid command, not from a
  flag or hello message.
- **Startup probe sends an immediate RST.** `wait_for_server()` connects, sets
  `SO_LINGER{1,0}`, and closes — every single run, the first connection the
  server ever accepts resets instantly. `recv`/`accept`-adjacent `ECONNRESET`
  on a fresh connection must not be fatal.
- **`SO_REUSEADDR` required.** Server is started/killed once per experiment on
  the same port 5000; without it, `bind` hits `EADDRINUSE` from lingering
  `TIME_WAIT`.
- **Experiment 4 is a timed test of the concurrency design.** Client 1 sends
  `"LOGIN blocked_client"` with **no trailing `\n`**, then goes silent
  forever. Client 2 then connects and expects a reply within 5s (elapsed time
  is printed). Requirements this proves: (a) the partial line must sit
  buffered/unparsed — framing correctness — and (b) the server must not be
  blocked in a `recv()` on client 1 — concurrency correctness. A naive
  accept-then-loop-per-client-blocking-recv server fails this outright.
- **Experiment 7 sizes backpressure.** 5000 matching trades at 1ms intervals
  against one market-data client that never reads (`TRADE JNST 1 238\n` = 17
  bytes/msg, ~85KB total — close enough to default socket buffers that
  whether it actually blocks is worth measuring, not assuming). A dead/slow
  reader must not stall other clients (§4.4) — needs per-connection outbound
  queues + non-blocking `send`, not a blocking write in the broadcast path.
- **Experiment 8 requires `SIGPIPE` handling.** A market-data client process
  is `SIGKILL`ed, then the harness generates 50 more trades the server will
  try to write to that now-dead socket. Default `SIGPIPE` disposition kills
  the whole server process. Must `signal(SIGPIPE, SIG_IGN)` and handle
  `EPIPE`/`ECONNRESET` from `send` per-connection instead.
- **Persistent connections.** One TCP connection carries many messages over
  its lifetime — never assume one message per connection.

## Experiments (40% of grade — investigation, not just code)
Run via `python3 experiment.py <n>`. Each needs FreeBSD-tool screenshots + a short
written answer in report.pdf. Topics: (1) listening vs connected sockets;
(2) TCP connection states over a lifetime; (3) TCP as a byte stream / framing;
(4) an idle client must not stall others (find the blocking operation);
(5, optional) I/O multiplexing / which sockets are ready; (6) FIN vs RST (orderly
vs abrupt close); (7) backpressure / slow receiver; (8) unexpected disconnect
detection. Bonus: scale to 70,000 idle connections + a client-generator program +
a resource-measurement table + analysis of the first bottleneck.
Build in a verbose/debug mode from the start that logs every `recv()`'s byte
count + raw bytes per connection — this log **is** the Experiment 3 screenshot
(proves one message arrived across multiple `recv` calls).

## Working style reminders
- Explain every line of new code, focused on networking/OS semantics rather
  than basic C++ syntax (see "Who I am" above).
- Grow the code incrementally from the working echo example; keep each step small
  and runnable before adding the next piece.
