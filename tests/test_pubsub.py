"""
tests/test_pubsub.py - Integration tests for the SSL pub/sub broker.
Runs the broker as a subprocess and tests all protocol commands over TLS.

Usage:
  python3 tests/test_pubsub.py
"""

import ssl
import socket
import subprocess
import sys
import os
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from config import BROKER_HOST, BROKER_PORT, CA_CERT


def make_ssl_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if os.path.exists(CA_CERT):
        ctx.load_verify_locations(CA_CERT)
        ctx.check_hostname = False
    else:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def connect():
    ctx = make_ssl_context()
    raw = socket.create_connection((BROKER_HOST, BROKER_PORT))
    s = ctx.wrap_socket(raw, server_hostname=BROKER_HOST if ctx.check_hostname else None)
    s.settimeout(2.0)
    return s


def send(s, msg):
    s.sendall((msg + "\n").encode())


def recv_line(s):
    buf = ""
    while "\n" not in buf:
        buf += s.recv(1024).decode()
    return buf.strip()


def drain(s, timeout=0.5):
    lines = []
    s.settimeout(timeout)
    buf = ""
    try:
        while True:
            chunk = s.recv(1024).decode()
            if not chunk:
                break
            buf += chunk
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                if line.strip():
                    lines.append(line.strip())
    except Exception:
        pass
    s.settimeout(2.0)
    return lines


PASS = "\033[92m[OK]\033[0m  "
FAIL = "\033[91m[FAIL]\033[0m"

results = []


def check(name, cond, detail=""):
    results.append((name, cond, detail))
    icon = PASS if cond else FAIL
    print("  {} {:<42} {}".format(icon, name, detail))


def run_tests():
    print("\n\033[1m=== SSL Pub/Sub - Integration Tests ===\033[0m\n")

    # T1: TLS handshake
    try:
        s = connect()
        cipher = s.cipher()
        check("TLS handshake", True, "{} / {}".format(cipher[1], cipher[0]))
        s.close()
    except Exception as e:
        check("TLS handshake", False, str(e))
        print("\n[!] Cannot connect to broker. Aborting.")
        return

    # T2: Subscribe
    sub = connect()
    send(sub, "SUBSCRIBE sports")
    r = recv_line(sub)
    check("SUBSCRIBE", "OK" in r, r)

    # T3: Duplicate subscribe
    send(sub, "SUBSCRIBE sports")
    r = recv_line(sub)
    check("Duplicate SUBSCRIBE -> ERROR", "ERROR" in r, r)

    # T4: LIST
    lister = connect()
    send(lister, "LIST")
    r = recv_line(lister)
    check("LIST shows topic", "sports" in r, r)
    send(lister, "QUIT"); lister.close()

    # T5: Publish + NOTIFY delivery
    pub = connect()
    send(pub, "PUBLISH sports Bengaluru FC scores!")
    r = recv_line(pub)
    check("PUBLISH ACK", "OK" in r and "1 subscriber" in r, r)

    notifs = drain(sub)
    hit = next((l for l in notifs if l.startswith("NOTIFY") and "Bengaluru" in l), None)
    check("NOTIFY delivered to subscriber", hit is not None, hit or "(nothing)")

    # T6: Publisher does not receive own NOTIFY
    pub_notifs = drain(pub)
    self_echo = any(l.startswith("NOTIFY") for l in pub_notifs)
    check("No self-echo to publisher", not self_echo,
          "(none)" if not self_echo else str(pub_notifs))

    # T7: Unsubscribe
    sub.settimeout(2.0)
    send(sub, "UNSUBSCRIBE sports")
    r = recv_line(sub)
    check("UNSUBSCRIBE", "OK" in r, r)

    # T8: No NOTIFY after unsub
    send(pub, "PUBLISH sports After unsub message")
    recv_line(pub)
    notifs2 = drain(sub, timeout=0.4)
    no_notif = not any(l.startswith("NOTIFY") for l in notifs2)
    check("No NOTIFY after UNSUBSCRIBE", no_notif,
          "(none)" if no_notif else str(notifs2))

    # T9: Unsubscribe from non-subscribed topic
    send(sub, "UNSUBSCRIBE weather")
    r = recv_line(sub)
    check("UNSUBSCRIBE non-subscribed -> ERROR", "ERROR" in r, r)

    # T10: Unknown command
    send(sub, "HELLO world")
    r = recv_line(sub)
    check("Unknown command -> ERROR", "ERROR" in r, r)

    # T11: Multi-topic isolation
    sub.settimeout(2.0)
    send(sub, "SUBSCRIBE weather")
    recv_line(sub)
    send(pub, "PUBLISH sports Sports message")
    recv_line(pub)
    notifs3 = drain(sub, timeout=0.4)
    no_cross = not any("NOTIFY" in l for l in notifs3)
    check("Topic isolation (weather sub, sports pub)", no_cross,
          "(correct - no cross delivery)" if no_cross else str(notifs3))

    # Cleanup
    send(pub, "QUIT"); pub.close()
    send(sub, "QUIT"); sub.close()

    # Summary
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    print("\n  {}/{} tests passed".format(passed, total), end="  ")
    if passed == total:
        print("\033[92m- All passed\033[0m")
    else:
        print("\033[91m- {} failed\033[0m".format(total - passed))
        sys.exit(1)


if __name__ == "__main__":
    broker = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "src", "broker.py")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src")),
    )
    time.sleep(0.7)
    try:
        run_tests()
    finally:
        broker.terminate()
        broker.wait()
