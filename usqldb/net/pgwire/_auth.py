#
# usqldb -- PostgreSQL 17-compatible catalog layer for UQA
#
# Copyright (c) 2023-2026 Cognica, Inc.
#

"""PostgreSQL authentication methods.

Supports trust, cleartext password, MD5 password, and SCRAM-SHA-256
authentication, matching the methods available in PostgreSQL 17.

Each authenticator is a stateful, per-connection object.  The
:meth:`Authenticator.step` coroutine drives the authentication
handshake one message at a time.

Usage from the connection handler::

    auth = create_authenticator(method, username, credentials)
    response, done = await auth.initial()
    send(response)
    while not done:
        client_data = await read_auth_message()
        response, done = await auth.step(client_data)
        send(response)
    send(encode_auth_ok())
"""

from __future__ import annotations

import base64
import enum
import hashlib
import hmac
import secrets

from usqldb.net.pgwire._codec import MessageCodec
from usqldb.net.pgwire._errors import InvalidPassword


class AuthMethod(enum.Enum):
    """Supported PostgreSQL authentication methods."""

    TRUST = "trust"
    CLEARTEXT = "password"
    MD5 = "md5"
    SCRAM_SHA_256 = "scram-sha-256"


# ======================================================================
# Authenticator base
# ======================================================================


class Authenticator:
    """Base class for authentication method implementations.

    Subclasses implement :meth:`initial` (the first server message)
    and :meth:`step` (processing subsequent client messages).  Both
    return ``(response_bytes, is_complete)``.

    When *is_complete* is ``True``, the caller should send the
    response (if non-empty) followed by ``AuthenticationOk``.
    """

    def __init__(self, username: str, password: str | None) -> None:
        self._username = username
        self._password = password

    async def initial(self) -> tuple[bytes, bool]:
        """Produce the first server authentication message."""
        raise NotImplementedError

    async def step(self, data: bytes) -> tuple[bytes, bool]:
        """Process a client authentication message."""
        raise NotImplementedError


# ======================================================================
# Trust
# ======================================================================


class TrustAuthenticator(Authenticator):
    """Trust authentication -- no password required."""

    async def initial(self) -> tuple[bytes, bool]:
        return b"", True

    async def step(self, data: bytes) -> tuple[bytes, bool]:
        return b"", True


# ======================================================================
# Cleartext password
# ======================================================================


class CleartextAuthenticator(Authenticator):
    """Cleartext password authentication."""

    async def initial(self) -> tuple[bytes, bool]:
        return MessageCodec.encode_auth_cleartext(), False

    async def step(self, data: bytes) -> tuple[bytes, bool]:
        # data is the raw payload of the PasswordMessage ('p').
        # Extract the null-terminated password string.
        if data.endswith(b"\x00"):
            received = data[:-1].decode("utf-8")
        else:
            received = data.decode("utf-8")

        if self._password is None or not hmac.compare_digest(received, self._password):
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )
        return b"", True


# ======================================================================
# MD5 password
# ======================================================================


class MD5Authenticator(Authenticator):
    """MD5 password authentication.

    The client sends ``"md5" + md5(md5(password + username) + salt)``.
    """

    def __init__(self, username: str, password: str | None) -> None:
        super().__init__(username, password)
        self._salt = secrets.token_bytes(4)

    async def initial(self) -> tuple[bytes, bool]:
        return MessageCodec.encode_auth_md5(self._salt), False

    async def step(self, data: bytes) -> tuple[bytes, bool]:
        if data.endswith(b"\x00"):
            received = data[:-1].decode("utf-8")
        else:
            received = data.decode("utf-8")

        if self._password is None:
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )

        # Compute expected hash: md5(md5(password + username) + salt)
        inner = hashlib.md5(
            (self._password + self._username).encode("utf-8")
        ).hexdigest()
        expected = "md5" + hashlib.md5(inner.encode("utf-8") + self._salt).hexdigest()

        if not hmac.compare_digest(received, expected):
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )
        return b"", True


# ======================================================================
# SCRAM-SHA-256
# ======================================================================


class ScramSHA256Authenticator(Authenticator):
    """SCRAM-SHA-256 authentication per RFC 5802 / RFC 7677.

    Three-step handshake:
    1. initial() -> AuthenticationSASL listing "SCRAM-SHA-256"
    2. step(SASLInitialResponse) -> AuthenticationSASLContinue
    3. step(SASLResponse) -> AuthenticationSASLFinal
    """

    _ITERATIONS = 4096  # PostgreSQL 17 default

    def __init__(self, username: str, password: str | None) -> None:
        super().__init__(username, password)
        self._server_nonce = ""
        self._combined_nonce = ""
        self._salt = b""
        self._client_first_bare = ""
        self._server_first = ""
        self._stored_key = b""
        self._server_key = b""
        self._phase = 0

    async def initial(self) -> tuple[bytes, bool]:
        return MessageCodec.encode_auth_sasl(["SCRAM-SHA-256"]), False

    async def step(self, data: bytes) -> tuple[bytes, bool]:
        if self._phase == 0:
            return self._handle_client_first(data)
        if self._phase == 1:
            return self._handle_client_final(data)
        raise InvalidPassword(
            f'password authentication failed for user "{self._username}"'
        )

    def _handle_client_first(self, data: bytes) -> tuple[bytes, bool]:
        """Process SASLInitialResponse (client-first-message)."""
        # data has been re-parsed: it is the raw SASLInitialResponse payload.
        # Format: mechanism\0 int32(length) client-first-message
        # The codec's decode_sasl_initial_response gives us mechanism + data.
        # But we receive the raw 'p' payload here.  The connection handler
        # calls codec.decode_sasl_initial_response first and passes us
        # just the SCRAM client-first-message bytes.
        client_first_msg = data.decode("utf-8")

        # Parse gs2-header and client-first-bare.
        # Expected format: "n,,n=<user>,r=<client_nonce>"
        # (or "y,," if client supports channel binding but not using it)
        parts = client_first_msg.split(",", 2)
        if len(parts) < 3:
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )

        gs2_cbind_flag = parts[0]
        # We only support "n" (no channel binding) and "y" (client supports
        # but not using).
        if gs2_cbind_flag not in ("n", "y"):
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )

        # parts[1] is the authzid (empty for PostgreSQL)
        self._client_first_bare = parts[2]  # everything after "n,,"

        # Extract client nonce from client-first-bare.
        attrs: dict[str, str] = {}
        for attr in self._client_first_bare.split(","):
            if "=" in attr:
                key = attr[0]
                val = attr[2:]
                attrs[key] = val

        client_nonce = attrs.get("r", "")
        if not client_nonce:
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )

        # Generate server nonce and derive keys.
        self._server_nonce = base64.b64encode(secrets.token_bytes(24)).decode("ascii")
        self._combined_nonce = client_nonce + self._server_nonce

        if self._password is None:
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )

        # Derive SCRAM keys from password.
        self._salt = secrets.token_bytes(16)
        salted_password = hashlib.pbkdf2_hmac(
            "sha256",
            _saslprep(self._password).encode("utf-8"),
            self._salt,
            self._ITERATIONS,
        )
        client_key = _hmac_sha256(salted_password, b"Client Key")
        self._stored_key = _sha256(client_key)
        self._server_key = _hmac_sha256(salted_password, b"Server Key")

        # Build server-first-message.
        salt_b64 = base64.b64encode(self._salt).decode("ascii")
        self._server_first = (
            f"r={self._combined_nonce},s={salt_b64},i={self._ITERATIONS}"
        )

        self._phase = 1
        return (
            MessageCodec.encode_auth_sasl_continue(self._server_first.encode("utf-8")),
            False,
        )

    def _handle_client_final(self, data: bytes) -> tuple[bytes, bool]:
        """Process SASLResponse (client-final-message)."""
        client_final_msg = data.decode("utf-8")

        # Parse client-final-message: c=<channel_binding>,r=<nonce>,p=<proof>
        attrs: dict[str, str] = {}
        raw_parts: list[str] = []
        for part in client_final_msg.split(","):
            raw_parts.append(part)
            if "=" in part:
                key = part[0]
                val = part[2:]
                attrs[key] = val

        # Verify nonce.
        if attrs.get("r") != self._combined_nonce:
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )

        # Verify channel binding: must be base64("n,,") = "biws"
        cb = attrs.get("c", "")
        expected_cb = base64.b64encode(b"n,,").decode("ascii")
        if cb != expected_cb:
            # Also accept base64("y,,") for clients that support but
            # don't use channel binding.
            expected_cb_y = base64.b64encode(b"y,,").decode("ascii")
            if cb != expected_cb_y:
                raise InvalidPassword(
                    f'password authentication failed for user "{self._username}"'
                )

        # Extract client proof.
        client_proof_b64 = attrs.get("p", "")
        if not client_proof_b64:
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )
        client_proof = base64.b64decode(client_proof_b64)

        # Build client-final-without-proof (everything before ",p=...").
        # Find the last ",p=" in the message.
        proof_idx = client_final_msg.rfind(",p=")
        client_final_without_proof = client_final_msg[:proof_idx]

        # AuthMessage = client-first-bare + "," + server-first + "," +
        #               client-final-without-proof
        auth_message = (
            f"{self._client_first_bare},{self._server_first},"
            f"{client_final_without_proof}"
        )
        auth_message_bytes = auth_message.encode("utf-8")

        # Verify ClientProof.
        client_signature = _hmac_sha256(self._stored_key, auth_message_bytes)
        recovered_key = _xor_bytes(client_proof, client_signature)
        if _sha256(recovered_key) != self._stored_key:
            raise InvalidPassword(
                f'password authentication failed for user "{self._username}"'
            )

        # Compute ServerSignature.
        server_signature = _hmac_sha256(self._server_key, auth_message_bytes)
        server_final = "v=" + base64.b64encode(server_signature).decode("ascii")

        self._phase = 2
        return (
            MessageCodec.encode_auth_sasl_final(server_final.encode("utf-8")),
            True,
        )


# ======================================================================
# SCRAM helper functions
# ======================================================================


def _hmac_sha256(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _xor_bytes(a: bytes, b: bytes) -> bytes:
    return bytes(x ^ y for x, y in zip(a, b))


def _saslprep(password: str) -> str:
    """Minimal SASLprep normalization (NFC) for ASCII-safe passwords.

    Full SASLprep (RFC 4013) requires a complete Unicode profile.
    PostgreSQL clients typically send ASCII or NFC-normalized passwords.
    This covers the common case.
    """
    import unicodedata

    return unicodedata.normalize("NFC", password)


# ======================================================================
# Factory
# ======================================================================


def create_authenticator(
    method: str,
    username: str,
    credentials: dict[str, str] | None,
) -> Authenticator:
    """Create an authenticator for the given method and user."""
    password = credentials.get(username) if credentials else None

    if method == AuthMethod.TRUST.value:
        return TrustAuthenticator(username, password)
    if method == AuthMethod.CLEARTEXT.value:
        return CleartextAuthenticator(username, password)
    if method == AuthMethod.MD5.value:
        return MD5Authenticator(username, password)
    if method == AuthMethod.SCRAM_SHA_256.value:
        return ScramSHA256Authenticator(username, password)

    raise ValueError(f"Unknown authentication method: {method!r}")
