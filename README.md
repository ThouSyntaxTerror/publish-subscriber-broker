# Secure Publish-Subscribe Notification Service

A publish-subscribe message broker built from scratch using Python TCP sockets with **SSL/TLS encryption**. A central broker manages topic subscriptions and delivers messages to all registered subscribers in real time.

**Socket Programming Mini Project — PES University, Bengaluru**

---

## Requirements

- **Python 3.7+** (tested up to 3.12)
- `cryptography` library (for cert generation, one-time)
- Works on Ubuntu, Windows, and macOS

---

## Features

- SSL/TLS encrypted communication (TLS 1.2 minimum) on every connection
- Topic-based message routing — publishers and subscribers fully decoupled
- Multiple concurrent clients via thread-per-client model on the broker
- Async notification delivery — subscribers receive `NOTIFY` frames without polling
- Custom plaintext wire protocol over TCP
- Self-signed certificate generator included
- Full integration test suite (12 tests) and performance benchmark suite

---

## Project Structure

```
pubsub-project/
├── src/
│   ├── config.py        # Host, port, SSL cert paths (edit for multi-device)
│   ├── broker.py        # Central TLS broker server
│   ├── publisher.py     # TLS publisher client
│   └── subscriber.py    # TLS subscriber client
├── certs/
│   ├── gen_certs.py     # Self-signed certificate generator
│   ├── server.crt       # TLS certificate (generated)
│   └── server.key       # TLS private key (generated, broker only)
├── tests/
│   ├── test_pubsub.py   # Integration tests (12 tests)
│   └── benchmark.py     # Performance evaluation
├── docs/
│   ├── protocol.md      # Wire protocol specification
│   └── architecture.md  # System design & component diagram
├── .gitignore
└── README.md
```

---

## Setup

### 1. Install dependency (one-time)

```bash
pip install cryptography
```

### 2. Generate SSL certificates (one-time)

```bash
python3 certs/gen_certs.py
```

This creates `certs/server.crt` and `certs/server.key`.

---

## Running (Single Machine — 3 Terminals)

**Terminal 1 — Broker**
```bash
python3 src/broker.py
```

**Terminal 2 — Subscriber**
```bash
python3 src/subscriber.py
>>> sub sports
>>> sub weather
```

**Terminal 3 — Publisher**
```bash
python3 src/publisher.py
>>> pub sports Goal! Bengaluru FC 2-1
>>> pub weather Heavy rain alert tonight
```

---

## Multi-Device Setup (LAN / VMware Bridged)

1. Run `ip a` on the broker machine to get its LAN IP (e.g. `192.168.1.5`)
2. Copy `certs/server.crt` to all other machines (same path)
3. Edit `src/config.py` on **all machines**:
   ```python
   BROKER_HOST = "192.168.1.5"
   ```
4. Start broker on the broker machine, connect from others

> **VMware note:** Set Network Adapter to **Bridged** mode on all VMs, then run `sudo dhclient` to get a LAN IP. Allow port: `sudo ufw allow 9000/tcp`

---

## Running Tests

```bash
python3 tests/test_pubsub.py
```

Expected output: `12/12 tests passed`

---

## Running the Benchmark

```bash
python3 tests/benchmark.py                          # defaults: 10 clients, 100 msgs
python3 tests/benchmark.py --clients 20 --messages 200
```

Reports latency (min/mean/median/stdev), throughput (msg/s), and concurrent client delivery rate.

---

## Commands

| Command | Description |
|---|---|
| `sub <topic>` | Subscribe to a topic |
| `unsub <topic>` | Unsubscribe from a topic |
| `pub <topic> <message>` | Publish a message |
| `list` | List active topics with subscriber counts |
| `quit` | Disconnect |

---

## Wire Protocol

See [`docs/protocol.md`](docs/protocol.md) for the full specification.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for component diagram and design decisions.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: cryptography` | Run `pip install cryptography` |
| `SSL certificates not found` | Run `python3 certs/gen_certs.py` |
| `Connection refused` | Broker not started, or wrong IP in `config.py` |
| `Address already in use` | Another process on port 9000 — `lsof -i :9000` then `kill <pid>` |
| Cannot connect across VMs | Set VMware adapter to **Bridged** mode, run `sudo dhclient` |
| Port blocked | Run `sudo ufw allow 9000/tcp` on broker machine (Ubuntu) |
