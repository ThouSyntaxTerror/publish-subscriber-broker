# config.py — Central configuration
# For multi-device: set BROKER_HOST to the broker machine's LAN IP

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CERT_DIR = os.path.join(BASE_DIR, "certs")

# ---- Network ----
BROKER_HOST = "10.251.127.208" #change to LAN IP for multi-device (e.g. "192.168.1.5")
BROKER_PORT = 9000
BUFFER_SIZE = 4096

# ---- SSL ----
SERVER_CERT = os.path.join(CERT_DIR, "server.crt")
SERVER_KEY  = os.path.join(CERT_DIR, "server.key")
CA_CERT     = os.path.join(CERT_DIR, "server.crt")  # self-signed: cert acts as its own CA

# ---- User tiers ----
# Premium users — can access premium topics after LOGIN
# Format: "username": "password"
PREMIUM_USERS = {
    "alice":   "alice123",
    "bob":     "bob456",
    "manoj":   "manoj789",
}

# Normal (free) users — optional, for demo purposes
# Normal users don't need to log in; anyone connecting is a "free" user by default.
NORMAL_USERS = {
    "guest":   "guest",
    "test":    "test",
}

# Premium topics — only users logged in as PREMIUM can sub/pub here
PREMIUM_TOPICS = {
    "premium.stocks",
    "premium.vip_alerts",
    "premium.breaking_news",
}
