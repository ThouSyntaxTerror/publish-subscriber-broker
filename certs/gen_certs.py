"""
gen_certs.py - Generate a self-signed SSL certificate for the broker.
Run once before starting the server:
    python3 certs/gen_certs.py

Produces:
    certs/server.crt   (certificate - shared with clients)
    certs/server.key   (private key  - broker only)

Requires: pip install cryptography
"""

import datetime
import ipaddress
import os
import sys

CERT_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
except ImportError:
    print("[!] Missing dependency. Run: pip install cryptography")
    sys.exit(1)


def generate():
    print("[*] Generating 2048-bit RSA key pair...")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "IN"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Karnataka"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "Bengaluru"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "PES University"),
        x509.NameAttribute(NameOID.COMMON_NAME, "pubsub-broker"),
    ])

    # Use timezone-aware datetimes (works on all Python 3.7+)
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
    except AttributeError:
        # very old python fallback
        now = datetime.datetime.utcnow()

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.DNSName("pubsub-broker"),
                x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    cert_path = os.path.join(CERT_DIR, "server.crt")
    key_path = os.path.join(CERT_DIR, "server.key")

    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))

    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass  # Windows may not support chmod the same way

    print("[+] Certificate : {}".format(cert_path))
    print("[+] Private key : {}".format(key_path))
    print("[+] Done. Valid for 365 days.")


if __name__ == "__main__":
    generate()
