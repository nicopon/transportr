"""Known-answer vector of TransportR's end-to-end encryption.

Written from the specification alone, "End-to-End Encryption" and "Wire Format (v2)" in
TransportRArchitecture.md, never from the C# code, so that a value both compute the same is the
specification's and not one implementation's.

Nothing is installed for you: create the venv by hand, once.

    python3 -m venv <venv> && <venv>/bin/pip install cryptography
    <venv>/bin/python -I end_to_end_kat.py                           # prints the vector
    <venv>/bin/python -I end_to_end_kat.py <EndToEndCipherTests.cs>  # also checks the test carries each value

Inputs, all fixed: both ephemeral private keys, the TransferId, both client ids, the PSK, one short
plaintext under a non-zero BatchId. Outputs, without a PSK then with one: the Ping's payload, the
Pong's, one sealed batch (its 30-byte header, then its payload) and both receipts.
"""

import hmac
import hashlib
import struct
import sys

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDFExpand
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

# ---- Inputs ------------------------------------------------------------------------------------

SENDER_PRIVATE = bytes.fromhex("6c0b4b6a2d1f3e5a7c9e8d7b6a5f4e3d2c1b0a99887766554433221100ffeedd")
RECEIVER_PRIVATE = bytes.fromhex("1d2c3b4a59687786950a1b2c3d4e5f60718293a4b5c6d7e8f901122334455667")
TRANSFER_ID = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"
SENDER_ID = "a0a1a2a3-b0b1-c0c1-d0d1-d2d3d4d5d6d7"
RECEIVER_ID = "e0e1e2e3-f0f1-0010-1112-131415161718"
PSK = bytes(range(32))
PLAINTEXT = b"TransportR known-answer batch"
BATCH_ID = 0x01020304
COUNT = 3

# ---- What the specification fixes -------------------------------------------------------------

PROTOCOL_ID = b"transportr e2e v1 P-256 HKDF-SHA256 AES-256-GCM"
LABEL_BATCH = b"transportr e2e batch"
LABEL_CONFIRMATION = b"transportr e2e confirmation"
LABEL_SENDER_RECEIPT = b"transportr e2e sender receipt"
LABEL_RECEIVER_RECEIPT = b"transportr e2e receiver receipt"
HEADER_VERSION = 2
SEALED_BATCH = 0x09
TAG_SIZE = 16


def guid_bytes(text):
    """The 16 bytes Guid.ToByteArray() gives: the first three fields of the text form (4, 2 and 2
    bytes) little-endian, the last 8 bytes in order."""
    fields = text.split("-")
    raw = [bytes.fromhex(f) for f in fields]
    return raw[0][::-1] + raw[1][::-1] + raw[2][::-1] + raw[3] + raw[4]


def private_key(d):
    assert len(d) == 32
    return ec.derive_private_key(int.from_bytes(d, "big"), ec.SECP256R1())


def share(key):
    """The uncompressed point of SEC 1: 0x04, then X and Y, 32 bytes each, big-endian."""
    point = key.public_key().public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
    assert len(point) == 65 and point[0] == 0x04
    return point


def transcript(mode, sender_share, receiver_share):
    t = (
        bytes([len(PROTOCOL_ID)]) + PROTOCOL_ID
        + bytes([mode])
        + guid_bytes(TRANSFER_ID) + guid_bytes(SENDER_ID) + guid_bytes(RECEIVER_ID)
        + sender_share + receiver_share
    )
    assert len(t) == 227
    return t


def hkdf_extract(salt, ikm):
    """RFC 5869: PRK = HMAC-Hash(salt, IKM)."""
    return hmac.new(salt, ikm, hashlib.sha256).digest()


def hkdf_expand(prk, info):
    return HKDFExpand(algorithm=hashes.SHA256(), length=32, info=info).derive(prk)


def hmac_sha256(key, data):
    return hmac.new(key, data, hashlib.sha256).digest()


def header(type_, payload_length, batch_id, count):
    """Version, Type, PayloadLen, BatchId, Count, TransferId: integers little-endian."""
    return struct.pack("<BBiii", HEADER_VERSION, type_, payload_length, batch_id, count) + guid_bytes(TRANSFER_ID)


def receipt(key, last_batch_id):
    return hmac_sha256(key, guid_bytes(TRANSFER_ID) + struct.pack("<i", last_batch_id))


def vector(psk):
    sender_key = private_key(SENDER_PRIVATE)
    receiver_key = private_key(RECEIVER_PRIVATE)
    sender_share = share(sender_key)
    receiver_share = share(receiver_key)

    # The X coordinate, 32 bytes big-endian, as both ends compute it.
    secret = sender_key.exchange(ec.ECDH(), receiver_key.public_key())
    assert secret == receiver_key.exchange(ec.ECDH(), sender_key.public_key()) and len(secret) == 32

    mode = 0x01 if psk is not None else 0x00
    t = transcript(mode, sender_share, receiver_share)
    prk = hkdf_extract(psk if psk is not None else bytes(32), secret)
    batch_key = hkdf_expand(prk, LABEL_BATCH + t)
    confirmation_key = hkdf_expand(prk, LABEL_CONFIRMATION + t)
    sender_receipt_key = hkdf_expand(prk, LABEL_SENDER_RECEIPT + t)
    receiver_receipt_key = hkdf_expand(prk, LABEL_RECEIVER_RECEIPT + t)

    ping = sender_share
    pong = receiver_share + hmac_sha256(confirmation_key, t)

    sealed_header = header(SEALED_BATCH, len(PLAINTEXT) + TAG_SIZE, BATCH_ID, COUNT)
    nonce = struct.pack("<i", BATCH_ID) + bytes(8)
    sealed_payload = AESGCM(batch_key).encrypt(nonce, PLAINTEXT, sealed_header)
    assert len(sealed_payload) == len(PLAINTEXT) + TAG_SIZE

    return [
        ("Ping", ping),
        ("Pong", pong),
        ("SealedHeader", sealed_header),
        ("SealedPayload", sealed_payload),
        ("SenderReceipt", receipt(sender_receipt_key, BATCH_ID)),
        ("ReceiverReceipt", receipt(receiver_receipt_key, BATCH_ID)),
    ]


def main():
    values = []
    for mode_name, psk in (("WithoutPreSharedKey", None), ("WithPreSharedKey", PSK)):
        for name, value in vector(psk):
            values.append((f"{mode_name}.{name}", value.hex().upper()))

    for name, value in values:
        print(f"{name} = {value}")

    if len(sys.argv) > 1:
        with open(sys.argv[1], encoding="utf-8") as f:
            test = f.read().upper()
        inputs = [
            ("SenderPrivateKey", SENDER_PRIVATE.hex()), ("ReceiverPrivateKey", RECEIVER_PRIVATE.hex()),
            ("TransferId", TRANSFER_ID), ("SenderId", SENDER_ID), ("ReceiverId", RECEIVER_ID),
            ("PreSharedKey", PSK.hex()), ("Plaintext", PLAINTEXT.decode("ascii")),
            ("BatchId", f"0x{BATCH_ID:08X}"), ("Count", f"Count = {COUNT};"),
        ]
        missing = [name for name, value in inputs + values if value.upper() not in test]
        for name in missing:
            print(f"MISSING from {sys.argv[1]}: {name}", file=sys.stderr)
        print("C# test carries every value" if not missing else f"{len(missing)} value(s) missing")
        sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
