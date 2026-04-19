"""
broker.py - SSL/TLS Pub/Sub Broker with tiered access.

Protocol:
  LOGIN <username> <password>     identify as premium user
  SUBSCRIBE <topic>
  UNSUBSCRIBE <topic>
  PUBLISH <topic> <message...>
  LIST                            list all topics with tier info
  LIST_SUBS <topic>               list subscribers on a topic
  QUIT
"""

import ssl
import socket
import threading
import logging
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (
    BROKER_HOST, BROKER_PORT, BUFFER_SIZE, SERVER_CERT, SERVER_KEY,
    PREMIUM_USERS, PREMIUM_TOPICS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BROKER] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("broker")


class Broker:
    def __init__(self):
        self.subscriptions = defaultdict(set)
        self.clients = {}   # conn -> {addr, name, topics, tier, username}
        self.lock = threading.Lock()
        self._msg_count = 0

    def _make_ssl_context(self):
        if not os.path.exists(SERVER_CERT) or not os.path.exists(SERVER_KEY):
            log.error("SSL certificates not found. Run: python3 certs/gen_certs.py")
            sys.exit(1)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=SERVER_CERT, keyfile=SERVER_KEY)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        return ctx

    def handle_client(self, conn, addr):
        name = "{}:{}".format(addr[0], addr[1])
        try:
            cipher = conn.cipher()
            cipher_name = cipher[0] if cipher else "?"
        except Exception:
            cipher_name = "?"
        log.info("TLS connection established <- {}  [cipher: {}]".format(name, cipher_name))

        with self.lock:
            self.clients[conn] = {
                "addr": addr, "name": name, "topics": set(),
                "tier": "free", "username": None,
            }

        try:
            buf = ""
            while True:
                try:
                    data = conn.recv(BUFFER_SIZE)
                except ssl.SSLError as e:
                    log.warning("SSL error from {}: {}".format(name, e))
                    break
                except (ConnectionResetError, OSError):
                    break
                if not data:
                    break
                buf += data.decode("utf-8", errors="replace")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if line:
                        self._dispatch(conn, line)
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            log.warning("Connection error from {}: {}".format(name, e))
        finally:
            self._cleanup(conn)
            log.info("Disconnected: {}".format(name))

    def _dispatch(self, conn, line):
        parts = line.split(" ", 2)
        cmd = parts[0].upper()

        if cmd == "LOGIN" and len(parts) >= 2:
            # parts[1] = "username password..." (everything after LOGIN)
            creds = line.split(" ", 1)[1].split(" ", 1)
            username = creds[0]
            password = creds[1] if len(creds) > 1 else ""
            self._login(conn, username, password)
        elif cmd == "SUBSCRIBE" and len(parts) >= 2:
            self._subscribe(conn, parts[1])
        elif cmd == "UNSUBSCRIBE" and len(parts) >= 2:
            self._unsubscribe(conn, parts[1])
        elif cmd == "PUBLISH" and len(parts) >= 3:
            self._publish(conn, parts[1], parts[2])
        elif cmd == "LIST":
            self._list(conn)
        elif cmd == "LIST_SUBS" and len(parts) >= 2:
            self._list_subs(conn, parts[1])
        elif cmd == "QUIT":
            self._send(conn, "OK bye")
            try:
                conn.close()
            except OSError:
                pass
        else:
            self._send(conn, "ERROR unknown command: {!r}".format(line))

    def _login(self, conn, username, password):
        if username in PREMIUM_USERS and PREMIUM_USERS[username] == password:
            with self.lock:
                info = self.clients.get(conn)
                if not info:
                    return
                info["tier"] = "premium"
                info["username"] = username
                name = info["name"]
            self._send(conn, "OK logged in as premium user {!r}".format(username))
            log.info("{} authenticated as PREMIUM user {!r}".format(name, username))
        else:
            self._send(conn, "ERROR invalid credentials")
            log.warning("Failed login attempt for username {!r}".format(username))

    def _subscribe(self, conn, topic):
        with self.lock:
            info = self.clients.get(conn)
            if not info:
                return
            tier = info["tier"]

            if topic in PREMIUM_TOPICS and tier != "premium":
                self._send(conn, "ERROR topic {!r} is premium-only. Use LOGIN first.".format(topic))
                log.info("{} denied premium subscription to {!r}".format(info["name"], topic))
                return

            if topic in info["topics"]:
                self._send(conn, "ERROR already subscribed to {!r}".format(topic))
                return
            self.subscriptions[topic].add(conn)
            info["topics"].add(topic)
            client_name = info["name"]
            client_tier = info["tier"]

        self._send(conn, "OK subscribed to {!r}".format(topic))
        log.info("{} [{}] subscribed -> {!r}".format(client_name, client_tier, topic))

    def _unsubscribe(self, conn, topic):
        with self.lock:
            info = self.clients.get(conn)
            if not info or topic not in info["topics"]:
                self._send(conn, "ERROR not subscribed to {!r}".format(topic))
                return
            self.subscriptions[topic].discard(conn)
            info["topics"].discard(topic)
            client_name = info["name"]
        self._send(conn, "OK unsubscribed from {!r}".format(topic))
        log.info("{} unsubscribed <- {!r}".format(client_name, topic))

    def _publish(self, conn, topic, message):
        with self.lock:
            info = self.clients.get(conn, {})
            tier = info.get("tier", "free")
            pub_name = info.get("name", "?")

            if topic in PREMIUM_TOPICS and tier != "premium":
                self._send(conn, "ERROR cannot publish to premium topic {!r} without login".format(topic))
                log.info("{} denied premium publish to {!r}".format(pub_name, topic))
                return

            subscribers = set(self.subscriptions.get(topic, set()))

        delivered = 0
        for sub in subscribers:
            if sub is not conn:
                if self._send(sub, "NOTIFY {} {}".format(topic, message)):
                    delivered += 1

        self._send(conn, "OK published to {} subscriber(s) on {!r}".format(delivered, topic))
        with self.lock:
            self._msg_count += 1
        log.info("{} [{}] published on {!r} -> {} sub(s): {!r}".format(
            pub_name, tier, topic, delivered, message))

    def _list(self, conn):
        with self.lock:
            active = {t: len(s) for t, s in self.subscriptions.items() if s}

        all_topics = {}
        for t, n in active.items():
            tag = "PREMIUM" if t in PREMIUM_TOPICS else "free"
            all_topics[t] = "{}({})[{}]".format(t, n, tag)
        for t in PREMIUM_TOPICS:
            if t not in active:
                all_topics[t] = "{}(0)[PREMIUM]".format(t)

        if all_topics:
            detail = ", ".join(all_topics[t] for t in sorted(all_topics))
            self._send(conn, "OK topics: {}".format(detail))
        else:
            self._send(conn, "OK no active topics")

    def _list_subs(self, conn, topic):
        with self.lock:
            subs = self.subscriptions.get(topic, set())
            if not subs:
                self._send(conn, "OK no subscribers on {!r}".format(topic))
                return
            tiers = []
            for s in subs:
                client = self.clients.get(s, {})
                uname = client.get("username") or "anon"
                tier = client.get("tier", "free")
                tiers.append("{}[{}]".format(uname, tier))
        self._send(conn, "OK subs on {!r}: {}".format(topic, ", ".join(tiers)))

    def _send(self, conn, message):
        try:
            conn.sendall((message + "\n").encode("utf-8"))
            return True
        except (BrokenPipeError, OSError, ssl.SSLError):
            return False

    def _cleanup(self, conn):
        with self.lock:
            info = self.clients.pop(conn, {})
            for topic in info.get("topics", []):
                self.subscriptions[topic].discard(conn)
        try:
            conn.close()
        except OSError:
            pass

    def start(self):
        ctx = self._make_ssl_context()
        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            raw.bind((BROKER_HOST, BROKER_PORT))
        except OSError as e:
            log.error("Failed to bind to {}:{} - {}".format(BROKER_HOST, BROKER_PORT, e))
            sys.exit(1)
        raw.listen()

        with ctx.wrap_socket(raw, server_side=True) as server:
            log.info("TLS broker listening on {}:{}".format(BROKER_HOST, BROKER_PORT))
            log.info("TLS minimum version : {}".format(server.context.minimum_version.name))
            log.info("Premium topics: {}".format(", ".join(sorted(PREMIUM_TOPICS))))
            log.info("Waiting for publishers and subscribers...")
            try:
                while True:
                    try:
                        conn, addr = server.accept()
                    except ssl.SSLError as e:
                        log.warning("TLS handshake failed: {}".format(e))
                        continue
                    except OSError as e:
                        log.warning("Accept failed: {}".format(e))
                        continue
                    t = threading.Thread(
                        target=self.handle_client,
                        args=(conn, addr),
                        daemon=True,
                    )
                    t.start()
            except KeyboardInterrupt:
                log.info("Broker shutting down (Ctrl+C)")


if __name__ == "__main__":
    Broker().start()
