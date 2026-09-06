#!/bin/sh
# Bonus experiment setup: prepare a FreeBSD host to hold 70,000 concurrent
# idle TCP connections, with server and client_generator on the same host.
#
#     sudo sh bonus/setup-freebsd.sh          # apply now (lost on reboot)
#     sudo sh bonus/setup-freebsd.sh --persist # apply now AND survive reboot
#
# Without --persist nothing under /etc is touched.

set -e

if [ "$(id -u)" != "0" ]; then
    echo "must run as root (try: sudo sh $0 $*)" >&2
    exit 1
fi

DIR=$(dirname "$0")
PERSIST=no
[ "$1" = "--persist" ] && PERSIST=yes

echo "--- applying sysctls ---"
sysctl kern.maxfiles=200000
sysctl kern.maxfilesperproc=100000
sysctl kern.ipc.maxsockets=200000
sysctl kern.ipc.soacceptqueue=1024

echo "--- adding lo0 aliases ---"
# Idempotent: skip an address lo0 already owns, so re-running is harmless.
for n in 2 3 4 5; do
    if ifconfig lo0 | grep -q "inet 127.0.0.$n "; then
        echo "127.0.0.$n already present"
    else
        ifconfig lo0 alias "127.0.0.$n" netmask 255.255.255.255
        echo "127.0.0.$n added"
    fi
done

if [ "$PERSIST" = "yes" ]; then
    echo "--- installing persistent config ---"
    if grep -q '^kern.maxfiles=200000' /etc/sysctl.conf 2>/dev/null; then
        echo "/etc/sysctl.conf already configured"
    else
        cat "$DIR/sysctl.conf" >> /etc/sysctl.conf
        echo "appended to /etc/sysctl.conf"
    fi

    if grep -q '^ifconfig_lo0_alias0' /etc/rc.conf 2>/dev/null; then
        echo "/etc/rc.conf already configured"
    else
        cat "$DIR/rc.conf" >> /etc/rc.conf
        echo "appended to /etc/rc.conf"
    fi
fi

echo
echo "--- verification ---"
sysctl kern.maxfiles kern.maxfilesperproc kern.ipc.maxsockets kern.ipc.soacceptqueue
ifconfig lo0 | grep 'inet '
echo
echo "NOTE: a process must be started AFTER this runs to inherit the raised"
echo "      RLIMIT_NOFILE ceiling -- restart exchange_server if it is running."
