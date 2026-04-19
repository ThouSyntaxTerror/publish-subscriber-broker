"""
publisher.py - SSL/TLS Pub/Sub Publisher Client

Commands:
  login <user> <pass>       Log in as premium user
  pub <topic> <message>     Publish
  list                      List all topics (shows premium tags)
  subs <topic>              List subscribers on a topic
  quit
"""

import ssl
import socket
import threading
import sys
import os
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BROKER_HOST, BROKER_PORT, BUFFER_SIZE, CA_CERT

HELP = """
Commands:
  login <user> <pass>       Log in as premium user
  pub <topic> <message>     Publish a message to a topic
  list                      List all active topics (with tier)
  subs <topic>              List subscribers on a topic
  quit                      Disconnect and exit
  help                      Show this message
"""


class Publisher:
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
                    print("\n[!] SSL error: {}".format(e))
                    break
                except (ConnectionResetError, OSError) as e:
                    print("\n[!] Connection error: {}".format(e))
                    break

                if not data:
                    self.running = False
                    break

                buf += data.decode("utf-8", errors="replace")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith("OK"):
                        sys.stdout.write("\n[OK]  {}\n>>> ".format(line[3:]))
                        sys.stdout.flush()
                    elif line.startswith("ERROR"):
                        sys.stdout.write("\n[ERR] {}\n>>> ".format(line[6:]))
                        sys.stdout.flush()
                    else:
                        sys.stdout.write("\n   {}\n>>> ".format(line))
                        sys.stdout.flush()
        except Exception as e:
            print("\n[!] Listener crashed: {}".format(e))
            traceback.print_exc()
        finally:
            self.running = False

    def _send(self, msg):
        try:
            self.sock.sendall((msg + "\n").encode("utf-8"))
        except (OSError, ssl.SSLError) as e:
            print("[!] Send failed: {}".format(e))

    def run(self, one_shot_topic=None, one_shot_msg=None):
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

        threading.Thread(target=self._listen, daemon=True).start()

        if one_shot_topic and one_shot_msg:
            self._send("PUBLISH {} {}".format(one_shot_topic, one_shot_msg))
            time.sleep(0.3)
            self.running = False
            try:
                self._send("QUIT")
                self.sock.close()
            except OSError:
                pass
            return

        print(HELP)
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
                    # parts[2] may have "pass" or "pass with spaces" — use split of raw line
                    rest = line.split(None, 1)[1]
                    user_pass = rest.split(None, 1)
                    if len(user_pass) == 2:
                        self._send("LOGIN {} {}".format(user_pass[0], user_pass[1]))
                    else:
                        print("Usage: login <user> <pass>")
                elif cmd == "pub" and len(parts) >= 3:
                    self._send("PUBLISH {} {}".format(parts[1], parts[2]))
                elif cmd == "pub":
                    print("Usage: pub <topic> <message>")
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
    if len(sys.argv) == 3:
        Publisher().run(one_shot_topic=sys.argv[1], one_shot_msg=sys.argv[2])
    else:
        Publisher().run()
