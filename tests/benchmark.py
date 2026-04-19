"""
tests/benchmark.py - Performance Evaluation for SSL Pub/Sub Broker

Measures:
  1. Message latency        (round-trip time per publish-notify cycle)
  2. Throughput             (messages delivered per second)
  3. Concurrent client load (N subscribers + 1 publisher simultaneously)

Usage:
  python3 tests/benchmark.py
  python3 tests/benchmark.py --clients 20 --messages 200
"""

import ssl
import socket
import threading
import subprocess
import sys
import os
import time
import argparse
import statistics

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from config import BROKER_HOST, BROKER_PORT, CA_CERT

TOPIC = "bench"


def make_ctx():
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
    ctx = make_ctx()
    raw = socket.create_connection((BROKER_HOST, BROKER_PORT))
    s = ctx.wrap_socket(raw, server_hostname=BROKER_HOST if ctx.check_hostname else None)
    return s


def sendl(s, msg):
    s.sendall((msg + "\n").encode())


def recv_line(s, timeout=3.0):
    s.settimeout(timeout)
    buf = ""
    while "\n" not in buf:
        chunk = s.recv(4096).decode()
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf.split("\n")[0].strip()


# ------------------------------------------------------------------ #
#  Test 1 - Latency (single subscriber)                                #
# ------------------------------------------------------------------ #

def test_latency(n_messages):
    print("\n\033[1m[1] Latency Test\033[0m  ({} messages, 1 subscriber)".format(n_messages))

    sub = connect()
    sendl(sub, "SUBSCRIBE {}".format(TOPIC))
    recv_line(sub)

    pub = connect()

    latencies = []
    for i in range(n_messages):
        payload = "PING {}".format(i)
        t_send = time.perf_counter()
        sendl(pub, "PUBLISH {} {}".format(TOPIC, payload))
        recv_line(pub)
        sub.settimeout(3.0)
        buf = ""
        while "NOTIFY" not in buf:
            buf += sub.recv(4096).decode()
        t_recv = time.perf_counter()
        latencies.append((t_recv - t_send) * 1000)

    sendl(pub, "QUIT"); pub.close()
    sendl(sub, "QUIT"); sub.close()

    print("  Min     : {:.2f} ms".format(min(latencies)))
    print("  Max     : {:.2f} ms".format(max(latencies)))
    print("  Mean    : {:.2f} ms".format(statistics.mean(latencies)))
    print("  Median  : {:.2f} ms".format(statistics.median(latencies)))
    print("  Stdev   : {:.2f} ms".format(statistics.stdev(latencies)))
    return latencies


# ------------------------------------------------------------------ #
#  Test 2 - Throughput                                                  #
# ------------------------------------------------------------------ #

def test_throughput(n_messages, n_subscribers):
    print("\n\033[1m[2] Throughput Test\033[0m  ({} messages, {} subscribers)".format(n_messages, n_subscribers))

    subs = []
    for _ in range(n_subscribers):
        s = connect()
        sendl(s, "SUBSCRIBE {}".format(TOPIC))
        recv_line(s)
        subs.append(s)

    pub = connect()

    received = [0] * n_subscribers
    barriers = [threading.Event() for _ in range(n_subscribers)]

    def listener(idx, s):
        count = 0
        s.settimeout(15.0)
        buf = ""
        try:
            while count < n_messages:
                chunk = s.recv(4096).decode()
                if not chunk:
                    break
                buf += chunk
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if line.strip().startswith("NOTIFY"):
                        count += 1
        except Exception:
            pass
        received[idx] = count
        barriers[idx].set()

    threads = [threading.Thread(target=listener, args=(i, subs[i]), daemon=True)
               for i in range(n_subscribers)]
    for t in threads:
        t.start()

    t0 = time.perf_counter()
    for i in range(n_messages):
        sendl(pub, "PUBLISH {} msg-{}".format(TOPIC, i))
        recv_line(pub)
    t1 = time.perf_counter()

    for b in barriers:
        b.wait(timeout=15.0)
    t2 = time.perf_counter()

    total_delivered = sum(received)
    expected = n_messages * n_subscribers
    pub_rate = n_messages / (t1 - t0)
    delivery_rate = total_delivered / (t2 - t0)

    sendl(pub, "QUIT"); pub.close()
    for s in subs:
        try:
            sendl(s, "QUIT"); s.close()
        except Exception:
            pass

    print("  Messages published  : {}".format(n_messages))
    print("  Subscribers         : {}".format(n_subscribers))
    print("  Expected deliveries : {}".format(expected))
    print("  Actual deliveries   : {}".format(total_delivered))
    print("  Delivery rate       : {:.1f} msg/s".format(delivery_rate))
    print("  Publish rate        : {:.1f} msg/s".format(pub_rate))
    loss = expected - total_delivered
    print("  Loss                : {} msgs  ({:.1f}%)".format(loss, loss / expected * 100))


# ------------------------------------------------------------------ #
#  Test 3 - Concurrent clients                                          #
# ------------------------------------------------------------------ #

def test_concurrent(n_clients, n_messages):
    print("\n\033[1m[3] Concurrent Client Test\033[0m  ({} subscribers connecting simultaneously)".format(n_clients))

    errors = []
    connected = []
    lock = threading.Lock()

    def connect_and_sub():
        try:
            s = connect()
            sendl(s, "SUBSCRIBE {}".format(TOPIC))
            r = recv_line(s)
            with lock:
                if "OK" in r:
                    connected.append(s)
                else:
                    errors.append(r)
        except Exception as e:
            with lock:
                errors.append(str(e))

    threads = [threading.Thread(target=connect_and_sub, daemon=True) for _ in range(n_clients)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    t1 = time.perf_counter()

    print("  Clients attempted   : {}".format(n_clients))
    print("  Successfully subbed : {}".format(len(connected)))
    print("  Errors              : {}".format(len(errors)))
    print("  Connect time        : {:.0f} ms total".format((t1 - t0) * 1000))

    pub = connect()
    received = [0] * len(connected)
    barriers = [threading.Event() for _ in connected]

    def listener(idx, s):
        count = 0
        s.settimeout(10.0)
        buf = ""
        try:
            while count < n_messages:
                chunk = s.recv(4096).decode()
                if not chunk:
                    break
                buf += chunk
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if line.strip().startswith("NOTIFY"):
                        count += 1
        except Exception:
            pass
        received[idx] = count
        barriers[idx].set()

    ts = [threading.Thread(target=listener, args=(i, connected[i]), daemon=True)
          for i in range(len(connected))]
    for t in ts:
        t.start()

    for i in range(n_messages):
        sendl(pub, "PUBLISH {} concurrent-msg-{}".format(TOPIC, i))
        recv_line(pub)

    for b in barriers:
        b.wait(timeout=10.0)

    total = sum(received)
    expected = n_messages * len(connected)
    print("  Messages sent       : {}".format(n_messages))
    print("  Total deliveries    : {} / {}".format(total, expected))
    if expected > 0:
        print("  Delivery success    : {:.1f}%".format(total / expected * 100))

    sendl(pub, "QUIT"); pub.close()
    for s in connected:
        try:
            sendl(s, "QUIT"); s.close()
        except Exception:
            pass


# ------------------------------------------------------------------ #
#  Main                                                                 #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pub/Sub Broker Benchmark")
    parser.add_argument("--clients", type=int, default=10, help="Number of concurrent subscribers")
    parser.add_argument("--messages", type=int, default=100, help="Messages per test")
    args = parser.parse_args()

    broker = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "src", "broker.py")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src")),
    )
    time.sleep(0.7)

    print("\n\033[1m=========================================\033[0m")
    print("\033[1m  SSL Pub/Sub Broker - Performance Report\033[0m")
    print("\033[1m=========================================\033[0m")

    try:
        test_latency(args.messages)
        test_throughput(args.messages, n_subscribers=min(args.clients, 5))
        test_concurrent(args.clients, args.messages)
    finally:
        broker.terminate()
        broker.wait()

    print("\n\033[92m[+] Benchmark complete.\033[0m\n")
