"""
subscriber.py - SSL/TLS Pub/Sub Subscriber Client with login support.
"""

import ssl
import socket
import threading
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BROKER_HOST, BROKER_PORT, BUFFER_SIZE, CA_CERT

HELP = """
Commands:
  login <user> <pass>   Log in as premium user (unlocks premium topics)
  sub <topic>           Subscribe to a topic
  unsub <topic>         Unsubscribe from a topic
  list                  List active topics on broker
  subs <topic>          List subscribers on a topic
  quit                  Disconnect and exit
  help                  Show this message
"""


class Subscriber:
    def __init__(self):
        self.sock = None
        self.running = False

    def _make_ssl_context(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        if os.path.exists(CA_CERT):
            ctx.load_verify_locations(CA_CERT)
            ctx.check_hostname = False
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _listen(self):
        buf = ""
        try:
            while self.running:
                try:
                    data = self.sock.recv(BUFFER_SIZE)
                except socket.timeout:
                    continue
                except ssl.SSLWantReadError:
                    continue
                except ssl.SSLError as e:
                    print("\n[!] SSL error in listener: {}".format(e))
                    break
                except (ConnectionResetError, OSError) as e:
                    print("\n[!] Connection error in listener: {}".format(e))
                    break

                if not data:
                    print("\n[!] Broker closed the connection.")
                    self.running = False
                    break

                buf += data.decode("utf-8", errors="replace")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if line:
                        self._handle(line)
        except Exception as e:
            print("\n[!] Listener crashed: {}".format(e))
            traceback.print_exc()
        finally:
            self.running = False

    def _handle(self, line):
        if line.startswith("NOTIFY "):
            parts = line.split(" ", 2)
            if len(parts) == 3:
                _, topic, msg = parts
                sys.stdout.write("\n[{}] {}\n>>> ".format(topic, msg))
                sys.stdout.flush()
        elif line.startswith("OK"):
            sys.stdout.write("\n[OK]  {}\n>>> ".format(line[3:]))
            sys.stdout.flush()
        elif line.startswith("ERROR"):
            sys.stdout.write("\n[ERR] {}\n>>> ".format(line[6:]))
            sys.stdout.flush()
        else:
            sys.stdout.write("\n   {}\n>>> ".format(line))
            sys.stdout.flush()

    def _send(self, msg):
        try:
            self.sock.sendall((msg + "\n").encode("utf-8"))
        except (OSError, ssl.SSLError) as e:
            print("[!] Send failed: {}".format(e))

    def run(self, initial_topics=None):
        ctx = self._make_ssl_context()
        try:
            raw = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=10)
        except (ConnectionRefusedError, OSError) as e:
            print("[!] Cannot connect to {}:{} - {}".format(BROKER_HOST, BROKER_PORT, e))
            return

        try:
            self.sock = ctx.wrap_socket(
                raw,
                server_hostname=BROKER_HOST if ctx.check_hostname else None,
            )
        except ssl.SSLError as e:
            print("[!] TLS handshake failed: {}".format(e))
            raw.close()
            return

        self.sock.settimeout(None)

        self.running = True
        cipher = self.sock.cipher()
        print("[+] Connected to {}:{}  [{} / {}]".format(
            BROKER_HOST, BROKER_PORT, cipher[1], cipher[0]
        ))
        print(HELP)

        threading.Thread(target=self._listen, daemon=True).start()

        for topic in (initial_topics or []):
            self._send("SUBSCRIBE {}".format(topic))

        try:
            while self.running:
                try:
                    line = input(">>> ").strip()
                except EOFError:
                    break
                if not line:
                    continue
                parts = line.split(None, 2)
                cmd = parts[0].lower()

                if cmd == "login" and len(parts) >= 3:
                    rest = line.split(None, 1)[1]
                    user_pass = rest.split(None, 1)
                    if len(user_pass) == 2:
                        self._send("LOGIN {} {}".format(user_pass[0], user_pass[1]))
                    else:
                        print("Usage: login <user> <pass>")
                elif cmd == "sub" and len(parts) == 2:
                    self._send("SUBSCRIBE {}".format(parts[1]))
                elif cmd == "unsub" and len(parts) == 2:
                    self._send("UNSUBSCRIBE {}".format(parts[1]))
                elif cmd == "list":
                    self._send("LIST")
                elif cmd == "subs" and len(parts) == 2:
                    self._send("LIST_SUBS {}".format(parts[1]))
                elif cmd == "quit":
                    break
                elif cmd == "help":
                    print(HELP)
                else:
                    print("Unknown command. Type 'help'.")
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            try:
                self._send("QUIT")
                self.sock.close()
            except OSError:
                pass
            print("\nDisconnected.")


if __name__ == "__main__":
    Subscriber().run(initial_topics=sys.argv[1:])
