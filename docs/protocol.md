# Wire Protocol Specification

All messages are **UTF-8 encoded**, **newline-delimited** (`\n`) plaintext over a **TLS-encrypted TCP stream** (TLS 1.2 minimum).

---

## Transport

- Protocol  : TCP + TLS (minimum TLS 1.2)
- Port       : 9000 (configurable in `src/config.py`)
- Encoding   : UTF-8
- Framing    : Newline-delimited (`\n`)

## Client → Broker

| Message | Description |
|---|---|
| `SUBSCRIBE <topic>` | Subscribe to a topic |
| `UNSUBSCRIBE <topic>` | Unsubscribe from a topic |
| `PUBLISH <topic> <message>` | Publish message to all subscribers of topic |
| `LIST` | Request all active topics with subscriber counts |
| `QUIT` | Graceful disconnect |

## Broker → Client

| Message | Description |
|---|---|
| `OK <info>` | Success acknowledgement |
| `ERROR <reason>` | Command rejected |
| `NOTIFY <topic> <message>` | Async push to subscribers on delivery |

---

## Example Session

```
[TLS handshake complete]

C→B: SUBSCRIBE sports
B→C: OK subscribed to 'sports'

C→B: LIST
B→C: OK topics: sports(1), weather(2)

[Another client publishes]
B→C: NOTIFY sports Bengaluru FC scores!

C→B: UNSUBSCRIBE sports
B→C: OK unsubscribed from 'sports'

C→B: QUIT
B→C: OK bye
```

---

## Error Cases

| Scenario | Response |
|---|---|
| Subscribe to already-subscribed topic | `ERROR already subscribed to 'topic'` |
| Unsubscribe from non-subscribed topic | `ERROR not subscribed to 'topic'` |
| Malformed / unknown command | `ERROR unknown command: 'xyz'` |
| TLS handshake failure | Connection dropped before any exchange |

---

## Notes

- Publisher does **not** receive its own `NOTIFY` (no self-echo).
- `NOTIFY` frames are delivered asynchronously — clients handle them via a dedicated listener thread running concurrently with the command input loop.
- The broker cleans up all subscriptions automatically on client disconnect (graceful or abrupt).
