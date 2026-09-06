#!/usr/bin/env python3

"""
experiment.py

Experiment harness for The Socket Exchange assignment.

Usage:
    python3 experiment.py <experiment_number>

The harness always runs the student's Exchange Server through:
    ./server/run-server <host> <port>

Experiments use controlled TCP clients created by this harness so that the
experimental conditions do not depend on the programming language or UI of
the student's clients.
"""

from __future__ import annotations

import argparse
import os
import signal
import socket
import struct
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HOST = "127.0.0.1"
PORT = 5000

SERVER_LAUNCHER = "./server/run-server"

SERVER_START_TIMEOUT = 5.0
SERVER_START_POLL_INTERVAL = 0.10

OBSERVATION_TIME = 15.0

# Experiment 7:
# More pairs => more trades => more market-data notifications.
TRADE_COUNT = 5000
TRADE_INTERVAL = 0.001

# Experiment 8:
PRE_DISCONNECT_TRADES = 20
POST_DISCONNECT_TRADES = 50
POST_DISCONNECT_INTERVAL = 0.05


# ---------------------------------------------------------------------------
# Managed process helpers
# ---------------------------------------------------------------------------

@dataclass
class ManagedProcess:
    process: subprocess.Popen
    name: str


def start_process(command: list[str], name: str) -> ManagedProcess:
    """
    Start a process in a new session so its whole process group can be
    terminated during cleanup.
    """
    try:
        process = subprocess.Popen(
            command,
            start_new_session=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Could not start {name}: executable not found: {command[0]}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(
            f"Could not start {name}: {exc}"
        ) from exc

    return ManagedProcess(process=process, name=name)


def terminate_process(
    managed: Optional[ManagedProcess],
    force_after: float = 3.0,
) -> None:
    """Terminate a managed process group cleanly, then forcefully if needed."""
    if managed is None:
        return

    process = managed.process

    if process.poll() is not None:
        return

    print(f"Stopping {managed.name}...")

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        process.wait(timeout=force_after)
        return
    except subprocess.TimeoutExpired:
        pass

    print(f"{managed.name} did not terminate; killing it.")

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return

    try:
        process.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        pass


def check_launcher(path: str) -> None:
    """Check that a required launcher exists and is executable."""
    if not os.path.isfile(path):
        raise RuntimeError(f"Required launcher not found: {path}")

    if not os.access(path, os.X_OK):
        raise RuntimeError(f"Required launcher is not executable: {path}")


def start_server() -> ManagedProcess:
    """Start the student's Exchange Server."""
    check_launcher(SERVER_LAUNCHER)

    return start_process(
        [
            SERVER_LAUNCHER,
            HOST,
            str(PORT),
        ],
        "Exchange Server",
    )


def wait_for_server(
    server: ManagedProcess,
    timeout: float = SERVER_START_TIMEOUT,
) -> None:
    """
    Wait until the Exchange Server has bound/listened on the required port.

    We use a short TCP probe. The probe is reset immediately after connecting
    so that it does not leave a normal FIN/TIME_WAIT entry behind. The probe
    is not part of any experiment.
    """
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if server.process.poll() is not None:
            raise RuntimeError(
                "Exchange Server terminated unexpectedly "
                f"(exit code {server.process.returncode})."
            )

        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.settimeout(0.3)
            probe.connect((HOST, PORT))

            # Avoid leaving a normal close/TIME_WAIT state from the probe.
            set_abortive_close(probe)
            probe.close()
            return
        except OSError:
            probe.close()
            time.sleep(SERVER_START_POLL_INTERVAL)

    raise RuntimeError(
        f"Exchange Server did not become ready on {HOST}:{PORT}."
    )


def begin_experiment(number: int, title: str) -> ManagedProcess:
    print(f"=== Experiment {number}: {title} ===")
    print()

    server = start_server()
    print(f"Started Exchange Server (PID {server.process.pid}).")

    wait_for_server(server)

    print("Exchange Server is listening.")
    print()

    return server


# ---------------------------------------------------------------------------
# TCP client helpers
# ---------------------------------------------------------------------------

def connect_client(
    name: str,
    timeout: float = 3.0,
) -> socket.socket:
    """Create a TCP connection to the Exchange Server."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((HOST, PORT))
        print(
            f"{name}: connected from "
            f"{sock.getsockname()[0]}:{sock.getsockname()[1]}"
        )
        return sock
    except OSError as exc:
        raise RuntimeError(
            f"{name}: could not connect to Exchange Server: {exc}"
        ) from exc


def send_all(sock: socket.socket, data: bytes) -> None:
    """Send all bytes in data, handling partial writes."""
    view = memoryview(data)

    while view:
        try:
            sent = sock.send(view)
        except OSError as exc:
            raise RuntimeError(f"send() failed: {exc}") from exc

        if sent <= 0:
            raise RuntimeError("send() made no progress")

        view = view[sent:]


def send_text(sock: socket.socket, message: str) -> None:
    """Send one complete application-level protocol message."""
    send_all(sock, message.encode("utf-8"))


def recv_line(
    sock: socket.socket,
    timeout: float = 1.0,
) -> Optional[str]:
    """
    Receive one newline-terminated application message.

    This helper belongs to the harness only and does not prescribe the
    students' implementation strategy.
    """
    previous_timeout = sock.gettimeout()
    sock.settimeout(timeout)

    data = bytearray()

    try:
        while True:
            try:
                chunk = sock.recv(1)
            except socket.timeout:
                return None

            if not chunk:
                return None

            data.extend(chunk)

            if chunk == b"\n":
                return data.decode(
                    "utf-8",
                    errors="replace",
                ).rstrip("\n")
    except OSError:
        return None
    finally:
        sock.settimeout(previous_timeout)


def drain_socket(sock: socket.socket) -> list[str]:
    """
    Drain currently available complete lines without blocking.

    Used to prevent harness-generated Trader connections from becoming the
    source of backpressure themselves.
    """
    messages: list[str] = []
    previous_timeout = sock.gettimeout()

    try:
        sock.settimeout(0.01)

        while True:
            message = recv_line(sock, timeout=0.01)
            if message is None:
                break
            messages.append(message)

    finally:
        sock.settimeout(previous_timeout)

    return messages


def set_abortive_close(sock: socket.socket) -> None:
    """
    Configure close() to perform an abortive TCP termination (RST).
    """
    sock.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_LINGER,
        struct.pack("ii", 1, 0),
    )


def orderly_shutdown(sock: socket.socket) -> None:
    """Close the local sending direction while leaving the socket open."""
    try:
        sock.shutdown(socket.SHUT_WR)
    except OSError:
        pass


def close_socket(sock: Optional[socket.socket]) -> None:
    if sock is None:
        return

    try:
        sock.close()
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Small protocol helpers used by experiments 7 and 8
# ---------------------------------------------------------------------------

def login_trader(
    sock: socket.socket,
    username: str,
) -> None:
    send_text(sock, f"LOGIN {username}\n")

    # The protocol should generate some response. We do not make the
    # experiment depend on the exact ordering of additional notifications.
    recv_line(sock, timeout=2.0)


def submit_buy_sell_pair(
    buyer: socket.socket,
    seller: socket.socket,
    instrument: str = "JNST",
    quantity: int = 1,
    price: int = 238,
) -> None:
    """
    Submit a matching pair of orders.

    The exact matching rule is part of the assignment protocol:
    same instrument + opposite side + identical price.
    """
    send_text(
        buyer,
        f"BUY {instrument} {quantity} {price}\n",
    )
    send_text(
        seller,
        f"SELL {instrument} {quantity} {price}\n",
    )

    # Drain trader responses so the trader sockets themselves do not become
    # congested during experiments involving a slow market-data receiver.
    drain_socket(buyer)
    drain_socket(seller)


# ---------------------------------------------------------------------------
# Experiment 1
# ---------------------------------------------------------------------------

def experiment_1() -> None:
    """Listening and Connected Sockets."""
    server = begin_experiment(
        1,
        "Listening and Connected Sockets",
    )

    client = None

    try:
        client = connect_client("Experiment client")

        print()
        print("The client connection is now established and idle.")
        print("Investigate the current system state.")
        print()
        print("Press Ctrl-C when you are finished.")

        while True:
            if server.process.poll() is not None:
                print("Exchange Server terminated unexpectedly.")
                break

            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nExperiment finished.")

    finally:
        close_socket(client)
        terminate_process(server)


# ---------------------------------------------------------------------------
# Experiment 2
# ---------------------------------------------------------------------------

def experiment_2() -> None:
    """Observing TCP Connection States."""
    server = begin_experiment(
        2,
        "Observing TCP Connection States",
    )

    client = None

    try:
        client = connect_client("Experiment client")

        print()
        print("Phase 1: the client is connected and idle.")
        print("Investigate the connection now.")
        print()

        wait_seconds(10.0)

        print("Phase 2: closing the client connection.")
        close_socket(client)
        client = None

        print("Investigate the connection again.")
        print()
        print("The Exchange Server will remain running for 15 seconds.")

        wait_seconds(15.0)

        print("\nExperiment finished.")

    except KeyboardInterrupt:
        print("\nExperiment interrupted.")

    finally:
        close_socket(client)
        terminate_process(server)


# ---------------------------------------------------------------------------
# Experiment 3
# ---------------------------------------------------------------------------

def experiment_3() -> None:
    """TCP as a Byte Stream."""
    server = begin_experiment(
        3,
        "TCP as a Byte Stream",
    )

    client = None

    try:
        client = connect_client("Experiment client")

        pieces = [
            b"LOGIN ",
            b"experiment",
            b"_trader",
            b"\n",
        ]

        print()
        print(
            "Sending one application-level message using several "
            "separate TCP writes..."
        )
        print()

        for piece in pieces:
            send_all(client, piece)
            print(f"Sent {len(piece)} bytes.")
            time.sleep(0.2)

        print()
        print("Investigate what the Exchange Server received.")
        print("Press Ctrl-C when you are finished.")

        while True:
            if server.process.poll() is not None:
                print("Exchange Server terminated unexpectedly.")
                break

            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nExperiment finished.")

    finally:
        close_socket(client)
        terminate_process(server)


# ---------------------------------------------------------------------------
# Experiment 4
# ---------------------------------------------------------------------------

def experiment_4() -> None:
    """One Client Should Not Stall the Others."""
    server = begin_experiment(
        4,
        "One Client Should Not Stall the Others",
    )

    client_1 = None
    client_2 = None

    try:
        client_1 = connect_client("Client 1")

        print()
        print("Client 1 will send an incomplete application message.")
        send_text(client_1, "LOGIN blocked_client")
        print("Client 1 will now remain silent.")
        print()

        wait_seconds(2.0)

        client_2 = connect_client("Client 2")

        print("Client 2 will send a complete application message.")
        start = time.monotonic()

        send_text(client_2, "LOGIN active_client\n")

        response = recv_line(client_2, timeout=5.0)
        elapsed = time.monotonic() - start

        print()
        print(f"Client 2 response: {response!r}")
        print(f"Elapsed time: {elapsed:.3f} seconds")
        print()
        print("Investigate the behavior of both TCP connections.")
        print("Press Ctrl-C when you are finished.")

        while True:
            if server.process.poll() is not None:
                print("Exchange Server terminated unexpectedly.")
                break

            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nExperiment finished.")

    finally:
        close_socket(client_1)
        close_socket(client_2)
        terminate_process(server)


# ---------------------------------------------------------------------------
# Experiment 5 (optional)
# ---------------------------------------------------------------------------

def experiment_5() -> None:
    """
    Multiple Clients and I/O Multiplexing.

    This remains available as an optional experiment. The harness establishes
    five connections; only clients 1, 3, and 5 send application messages.
    """
    server = begin_experiment(
        5,
        "Multiple Clients and I/O Multiplexing (Optional)",
    )

    clients: list[socket.socket] = []

    try:
        print("Creating five simultaneous TCP connections...")
        print()

        for i in range(5):
            clients.append(
                connect_client(f"Client {i + 1}")
            )

        print()
        print("Clients 1, 3, and 5 will send application messages.")
        print("Clients 2 and 4 will remain idle.")
        print()

        for i in (0, 2, 4):
            send_text(
                clients[i],
                f"LOGIN client_{i + 1}\n",
            )
            print(f"Client {i + 1}: sent application data.")
            time.sleep(0.5)

        print()
        print("The experiment is now in the observation phase.")
        print("Investigate which connections were active.")
        print("Press Ctrl-C when you are finished.")

        wait_seconds(15.0)

    except KeyboardInterrupt:
        print("\nExperiment interrupted.")

    finally:
        for client in clients:
            close_socket(client)

        terminate_process(server)


# ---------------------------------------------------------------------------
# Experiment 6
# ---------------------------------------------------------------------------

def experiment_6() -> None:
    """FIN vs. RST: Orderly and Abrupt Connection Termination."""
    server = begin_experiment(
        6,
        "FIN vs. RST: Orderly and Abrupt Connection Termination",
    )

    orderly_client = None
    abrupt_client = None

    try:
        print("Part A: orderly connection termination (FIN).")
        orderly_client = connect_client("Orderly client")

        print("The client will shut down its sending direction.")
        orderly_shutdown(orderly_client)

        print("Investigate the TCP traffic and connection state.")
        wait_seconds(10.0)

        close_socket(orderly_client)
        orderly_client = None

        print()
        print("Part B: abortive connection termination (RST).")
        abrupt_client = connect_client("Abrupt client")

        print("The client will now close abortively.")
        set_abortive_close(abrupt_client)
        close_socket(abrupt_client)
        abrupt_client = None

        print("Investigate the TCP traffic and connection state.")
        print()
        print("The Exchange Server will remain running for 15 seconds.")

        wait_seconds(15.0)

        print("\nExperiment finished.")

    except KeyboardInterrupt:
        print("\nExperiment interrupted.")

    finally:
        close_socket(orderly_client)
        close_socket(abrupt_client)
        terminate_process(server)


# ---------------------------------------------------------------------------
# Experiment 7
# ---------------------------------------------------------------------------

def experiment_7() -> None:
    """
    Backpressure and the Slow Receiver.

    Two Market-Data clients subscribe to JNST:
        - normal client continuously drains data;
        - slow client deliberately does not read its receive socket.

    Two Trader Clients repeatedly submit matching BUY/SELL pairs to generate
    real trades through the assignment protocol.
    """
    server = begin_experiment(
        7,
        "Backpressure and the Slow Receiver",
    )

    normal_md = None
    slow_md = None
    buyer = None
    seller = None

    try:
        normal_md = connect_client("Normal Market-Data Client")
        slow_md = connect_client("Slow Market-Data Client")
        buyer = connect_client("Buyer Trader")
        seller = connect_client("Seller Trader")

        login_trader(buyer, "experiment_buyer")
        login_trader(seller, "experiment_seller")

        send_text(normal_md, "SUBSCRIBE JNST\n")
        send_text(slow_md, "SUBSCRIBE JNST\n")

        # Consume subscription responses if present.
        drain_socket(normal_md)
        drain_socket(slow_md)

        print()
        print("Both Market-Data Clients are subscribed to JNST.")
        print("The normal client will keep reading.")
        print("The slow client will not read market-data updates.")
        print()
        print(f"Generating up to {TRADE_COUNT} matching trades...")
        print()

        normal_md.settimeout(0.01)

        successful_trades = 0

        for i in range(TRADE_COUNT):
            try:
                submit_buy_sell_pair(
                    buyer,
                    seller,
                    instrument="JNST",
                    quantity=1,
                    price=238,
                )
            except RuntimeError:
                print(
                    f"Traffic generation stopped at trade {i + 1}."
                )
                break

            successful_trades += 1

            # Keep consuming the normal client's data so it does not become
            # a second slow receiver.
            while True:
                try:
                    data = normal_md.recv(65536)
                    if not data:
                        break
                except socket.timeout:
                    break
                except OSError:
                    break

            if TRADE_INTERVAL > 0:
                time.sleep(TRADE_INTERVAL)

        print()
        print(f"Generated {successful_trades} matching trades.")
        print("The slow client has intentionally not consumed its data.")
        print()
        print("Investigate both Market-Data TCP connections.")
        print("Press Ctrl-C when you are finished.")

        wait_seconds(OBSERVATION_TIME)

    except KeyboardInterrupt:
        print("\nExperiment interrupted.")

    finally:
        close_socket(normal_md)
        close_socket(slow_md)
        close_socket(buyer)
        close_socket(seller)
        terminate_process(server)


# ---------------------------------------------------------------------------
# Experiment 8
# ---------------------------------------------------------------------------

def experiment_8() -> None:
    """
    Unexpected Client Disconnection.

    A separate helper process acts as a Market-Data Client. It subscribes to
    JNST and remains connected. The harness generates real trades so that the
    server is actively communicating with it.

    The helper process is then killed unexpectedly. A separate Market-Data
    connection remains active so that students can compare the two TCP
    connections.
    """
    server = begin_experiment(
        8,
        "Unexpected Client Disconnection",
    )

    disappearing_process = None
    surviving_md = None
    buyer = None
    seller = None

    helper_code = r"""
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((host, port))
sock.sendall(b"SUBSCRIBE JNST\n")

# Consume the initial response, if any, without printing anything.
sock.settimeout(1.0)
try:
    while b"\n" not in globals().get("_dummy", b""):
        data = sock.recv(4096)
        if not data:
            break
        if b"\n" in data:
            break
except Exception:
    pass

sock.settimeout(None)

# Remain connected. The parent experiment process will terminate us.
while True:
    time.sleep(60)
"""

    try:
        surviving_md = connect_client(
            "Surviving Market-Data Client"
        )
        send_text(surviving_md, "SUBSCRIBE JNST\n")
        drain_socket(surviving_md)

        buyer = connect_client("Buyer Trader")
        seller = connect_client("Seller Trader")

        login_trader(buyer, "experiment_buyer")
        login_trader(seller, "experiment_seller")

        # Start a real client process so that its unexpected disappearance
        # represents an actual process failure rather than merely closing a
        # socket in this parent process.
        disappearing_process = start_process(
            [
                sys.executable,
                "-u",
                "-c",
                helper_code,
                HOST,
                str(PORT),
            ],
            "Disappearing Market-Data Client",
        )

        # Give the helper time to connect and subscribe.
        time.sleep(1.0)

        print()
        print("Both Market-Data Clients should now be connected.")
        print("Generating traffic before the disconnection...")
        print()

        for _ in range(PRE_DISCONNECT_TRADES):
            submit_buy_sell_pair(
                buyer,
                seller,
                instrument="JNST",
                quantity=1,
                price=238,
            )

            # Keep the surviving subscriber draining.
            try:
                surviving_md.settimeout(0.01)
                while True:
                    data = surviving_md.recv(65536)
                    if not data:
                        break
            except (socket.timeout, OSError):
                pass

            time.sleep(0.1)

        print()
        print("The Market-Data Client process will now disappear unexpectedly.")

        # SIGKILL prevents the helper application from choosing its own
        # shutdown procedure. The kernel still closes its descriptor when the
        # process disappears.
        try:
            os.killpg(
                disappearing_process.process.pid,
                signal.SIGKILL,
            )
        except ProcessLookupError:
            pass

        try:
            disappearing_process.process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            pass

        print("The client process has disappeared.")
        print("Generating additional traffic.")
        print()
        print("Investigate the terminated connection and the surviving one.")
        print("Press Ctrl-C when you are finished.")

        for _ in range(POST_DISCONNECT_TRADES):
            try:
                submit_buy_sell_pair(
                    buyer,
                    seller,
                    instrument="JNST",
                    quantity=1,
                    price=238,
                )
            except RuntimeError:
                break

            try:
                surviving_md.settimeout(0.01)
                while True:
                    data = surviving_md.recv(65536)
                    if not data:
                        break
            except (socket.timeout, OSError):
                pass

            time.sleep(POST_DISCONNECT_INTERVAL)

        wait_seconds(OBSERVATION_TIME)

        print("\nExperiment finished.")

    except KeyboardInterrupt:
        print("\nExperiment interrupted.")

    finally:
        if disappearing_process is not None:
            terminate_process(
                disappearing_process,
                force_after=1.0,
            )

        close_socket(surviving_md)
        close_socket(buyer)
        close_socket(seller)
        terminate_process(server)


# ---------------------------------------------------------------------------
# Generic wait helper
# ---------------------------------------------------------------------------

def wait_seconds(seconds: float) -> None:
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise


# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------

EXPERIMENTS = {
    1: experiment_1,
    2: experiment_2,
    3: experiment_3,
    4: experiment_4,
    5: experiment_5,
    6: experiment_6,
    7: experiment_7,
    8: experiment_8,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run experiments for The Socket Exchange assignment."
        )
    )

    parser.add_argument(
        "experiment_number",
        type=int,
        choices=sorted(EXPERIMENTS),
        help="experiment number to run",
    )

    args = parser.parse_args()

    try:
        EXPERIMENTS[args.experiment_number]()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(130)
    except RuntimeError as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
