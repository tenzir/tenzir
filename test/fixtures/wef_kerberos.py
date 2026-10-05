"""Kerberos for testing `accept_wef`: a throwaway KDC and a GSSAPI client.

The KDC is MIT Kerberos, started on demand in a temporary directory with the
realm `TENZIR.TEST`. The client calls the MIT GSSAPI library through `ctypes`,
so the tests need no Python bindings, only the MIT Kerberos tools and library.

Windows authenticates the machine account of a host, such as `WIN10$`, to the
service principal `HTTP/<collector>` with Kerberos, or with SPNEGO when it uses
the `Negotiate` scheme. It then encrypts every message with the established
security context. This module does the same.
"""

from __future__ import annotations

import atexit
import base64
import ctypes
import ctypes.util
import os
import re
import shutil
import socket
import struct
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path

REALM = "TENZIR.TEST"
SERVICE = f"HTTP/localhost@{REALM}"
BOUNDARY = "Encrypted Boundary"

_GSS_C_INDEFINITE = 0xFFFFFFFF
_GSS_C_INITIATE = 1
_GSS_C_MUTUAL_FLAG = 2
_GSS_C_REPLAY_FLAG = 4
_GSS_C_SEQUENCE_FLAG = 8
_GSS_C_CONF_FLAG = 16
_GSS_C_INTEG_FLAG = 32
_GSS_S_CONTINUE_NEEDED = 1
_GSS_C_GSS_CODE = 1
_GSS_C_MECH_CODE = 2
_GSS_IOV_BUFFER_TYPE_DATA = 1
_GSS_IOV_BUFFER_TYPE_HEADER = 2
_GSS_IOV_BUFFER_TYPE_PADDING = 9
_GSS_IOV_BUFFER_FLAG_ALLOCATE = 0x10000

# Object identifiers in DER encoding without tag and length.
_KRB5_MECHANISM = b"\x2a\x86\x48\x86\xf7\x12\x01\x02\x02"
_KRB5_PRINCIPAL_NAME = b"\x2a\x86\x48\x86\xf7\x12\x01\x02\x02\x01"
_SPNEGO_MECHANISM = b"\x2b\x06\x01\x05\x05\x02"


class KerberosUnavailable(Exception):
    """The MIT Kerberos tools or library are missing."""


def _tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise KerberosUnavailable(f"`{name}` from MIT Kerberos is not on PATH")
    return path


def _gssapi_library() -> str:
    """Finds the MIT GSSAPI library, preferring the one of `krb5-config`."""
    explicit = os.environ.get("TENZIR_TEST_GSSAPI_LIBRARY")
    if explicit:
        return explicit
    krb5_config = shutil.which("krb5-config")
    if krb5_config is not None:
        flags = subprocess.run(
            [krb5_config, "--libs", "gssapi"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        for directory in re.findall(r"-L(\S+)", flags):
            for name in (
                "libgssapi_krb5.dylib",
                "libgssapi_krb5.so",
                "libgssapi_krb5.so.2",
            ):
                candidate = Path(directory) / name
                if candidate.exists():
                    return str(candidate)
    found = ctypes.util.find_library("gssapi_krb5")
    if found is None:
        raise KerberosUnavailable("the MIT GSSAPI library is not available")
    return found


# -- KDC ----------------------------------------------------------------------


class Kdc:
    """An MIT Kerberos KDC for the realm `TENZIR.TEST`."""

    def __init__(self, directory: Path, port: int = 0) -> None:
        self.directory = directory
        self.port = port
        self.krb5_conf = directory / "krb5.conf"
        self.kdc_conf = directory / "kdc.conf"
        self._process: subprocess.Popen[bytes] | None = None
        self._lock = threading.Lock()
        self._principals: set[str] = set()

    @property
    def env(self) -> dict[str, str]:
        """The environment for clients, collectors, and other processes that
        attach to this KDC."""
        return {
            "KRB5_CONFIG": str(self.krb5_conf),
            "KRB5_KDC_PROFILE": str(self.kdc_conf),
            "TENZIR_TEST_KDC_DIRECTORY": str(self.directory),
        }

    def start(self) -> None:
        kdc = _tool("krb5kdc")
        kdb5_util = _tool("kdb5_util")
        _tool("kadmin.local")
        self.directory.mkdir(parents=True, exist_ok=True)
        self.krb5_conf.write_text(
            "[libdefaults]\n"
            f"  default_realm = {REALM}\n"
            "  dns_lookup_kdc = false\n"
            "  dns_lookup_realm = false\n"
            "  dns_canonicalize_hostname = false\n"
            "  rdns = false\n"
            "  udp_preference_limit = 1\n"
            "[realms]\n"
            f"  {REALM} = {{\n"
            f"    kdc = 127.0.0.1:{self.port}\n"
            "  }\n"
        )
        self.kdc_conf.write_text(
            "[kdcdefaults]\n"
            f"  kdc_ports = {self.port}\n"
            f"  kdc_tcp_ports = {self.port}\n"
            "[realms]\n"
            f"  {REALM} = {{\n"
            f"    database_name = {self.directory / 'principal'}\n"
            f"    key_stash_file = {self.directory / 'stash'}\n"
            f"    acl_file = {self.directory / 'kadm5.acl'}\n"
            "  }\n"
            "[logging]\n"
            f"  kdc = FILE:{self.directory / 'kdc.log'}\n"
        )
        (self.directory / "kadm5.acl").write_text("")
        env = dict(os.environ, **self.env)
        subprocess.run(
            [kdb5_util, "create", "-s", "-r", REALM, "-P", "tenzir-test-master"],
            check=True,
            env=env,
            capture_output=True,
        )
        self._process = subprocess.Popen(
            [kdc, "-n", "-r", REALM],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise KerberosUnavailable(
                    f"krb5kdc exited: {self._process.stderr.read().decode()}"
                )
            try:
                with socket.create_connection(("127.0.0.1", self.port), 0.1):
                    return
            except OSError:
                time.sleep(0.05)
        raise KerberosUnavailable("krb5kdc did not start listening")

    def stop(self) -> None:
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
        shutil.rmtree(self.directory, ignore_errors=True)

    def _kadmin(self, query: str) -> None:
        # Processes that attach to the same KDC may hold the database lock.
        for attempt in range(20):
            result = subprocess.run(
                [_tool("kadmin.local"), "-r", REALM, "-q", query],
                env=dict(os.environ, **self.env),
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and "error" not in result.stderr.lower():
                return
            time.sleep(0.1 * (attempt + 1))
        raise RuntimeError(f"kadmin.local failed: {query}: {result.stderr}")

    def keytab(self, principal: str, path: Path) -> Path:
        """Exports the keys of a principal, creating it first if needed."""
        with self._lock:
            if principal not in self._principals:
                # Another process attached to the KDC may have created it.
                subprocess.run(
                    [
                        _tool("kadmin.local"),
                        "-r",
                        REALM,
                        "-q",
                        f"addprinc -randkey {principal}",
                    ],
                    env=dict(os.environ, **self.env),
                    capture_output=True,
                )
                self._principals.add(principal)
            # Exporting without new keys keeps earlier keytabs valid.
            self._kadmin(f"ktadd -k {path} -norandkey {principal}")
        return path


_kdc: Kdc | None = None
_kdc_lock = threading.Lock()


def shared_kdc() -> Kdc:
    """Returns the KDC of this process, starting it on first use.

    The GSSAPI library reads `KRB5_CONFIG` from the environment of the
    process, so all clients of a process share one KDC.
    """
    global _kdc
    with _kdc_lock:
        attached = os.environ.get("TENZIR_TEST_KDC_DIRECTORY")
        if _kdc is None and attached:
            # A fixture started the KDC for this test.
            _kdc = Kdc(Path(attached))
        if _kdc is None:
            from ._utils import find_free_port

            kdc = Kdc(Path(tempfile.mkdtemp(prefix="wef-kdc-")), find_free_port())
            try:
                kdc.start()
            except BaseException:
                kdc.stop()
                raise
            os.environ.update(kdc.env)
            atexit.register(kdc.stop)
            _kdc = kdc
        return _kdc


def principal_of(name: str) -> str:
    return name if "@" in name else f"{name}@{REALM}"


# -- GSSAPI client ------------------------------------------------------------


class _Buffer(ctypes.Structure):
    _fields_ = [("length", ctypes.c_size_t), ("value", ctypes.c_void_p)]


class _Oid(ctypes.Structure):
    _fields_ = [("length", ctypes.c_uint32), ("elements", ctypes.c_void_p)]


class _KeyValue(ctypes.Structure):
    _fields_ = [("key", ctypes.c_char_p), ("value", ctypes.c_char_p)]


class _KeyValueSet(ctypes.Structure):
    _fields_ = [("count", ctypes.c_uint32), ("elements", ctypes.POINTER(_KeyValue))]


class _Iov(ctypes.Structure):
    _fields_ = [("type", ctypes.c_uint32), ("buffer", _Buffer)]


class _Gssapi:
    def __init__(self) -> None:
        self.lib = ctypes.CDLL(_gssapi_library())
        for name in (
            "gss_acquire_cred_from",
            "gss_delete_sec_context",
            "gss_display_status",
            "gss_import_name",
            "gss_init_sec_context",
            "gss_release_buffer",
            "gss_release_cred",
            "gss_release_iov_buffer",
            "gss_release_name",
            "gss_unwrap_iov",
            "gss_wrap_iov",
        ):
            getattr(self.lib, name).restype = ctypes.c_uint32
        self._oids: list[tuple[ctypes.Array, _Oid]] = []
        self.krb5_mechanism = self._oid(_KRB5_MECHANISM)
        self.spnego_mechanism = self._oid(_SPNEGO_MECHANISM)
        self.principal_name = self._oid(_KRB5_PRINCIPAL_NAME)

    def _oid(self, der: bytes) -> ctypes.POINTER(_Oid):
        data = ctypes.create_string_buffer(der, len(der))
        oid = _Oid(len(der), ctypes.cast(data, ctypes.c_void_p))
        self._oids.append((data, oid))
        return ctypes.pointer(oid)

    def status(self, code: int, kind: int) -> str:
        messages = []
        context = ctypes.c_uint32(0)
        while True:
            minor = ctypes.c_uint32(0)
            message = _Buffer()
            major = self.lib.gss_display_status(
                ctypes.byref(minor),
                ctypes.c_uint32(code),
                ctypes.c_int(kind),
                None,
                ctypes.byref(context),
                ctypes.byref(message),
            )
            if major & 0xFFFF0000:
                break
            messages.append(ctypes.string_at(message.value, message.length).decode())
            self.lib.gss_release_buffer(ctypes.byref(minor), ctypes.byref(message))
            if context.value == 0:
                break
        return ", ".join(messages)

    def check(self, operation: str, major: int, minor: ctypes.c_uint32) -> None:
        if major & 0xFFFF0000:
            raise RuntimeError(
                f"{operation}: {self.status(major, _GSS_C_GSS_CODE)} "
                f"({self.status(minor.value, _GSS_C_MECH_CODE)})"
            )


_gssapi: _Gssapi | None = None


def _gss() -> _Gssapi:
    global _gssapi
    if _gssapi is None:
        _gssapi = _Gssapi()
    return _gssapi


class KerberosClient:
    """The client side of the Kerberos session of one connection."""

    def __init__(
        self,
        principal: str,
        keytab: Path,
        *,
        service: str = SERVICE,
        scheme: str = "kerberos",
    ) -> None:
        if scheme not in ("kerberos", "negotiate"):
            raise ValueError(f"unknown scheme {scheme!r}")
        self.scheme = scheme
        self.gss = _gss()
        lib = self.gss.lib
        minor = ctypes.c_uint32(0)
        self.name = self._import(principal)
        self.target = self._import(service)
        # Obtain a ticket from the client keytab into a private cache.
        elements = (_KeyValue * 2)(
            _KeyValue(b"client_keytab", str(keytab).encode()),
            _KeyValue(b"ccache", f"MEMORY:{uuid.uuid4()}".encode()),
        )
        store = _KeyValueSet(2, elements)
        self.credential = ctypes.c_void_p()
        major = lib.gss_acquire_cred_from(
            ctypes.byref(minor),
            self.name,
            ctypes.c_uint32(_GSS_C_INDEFINITE),
            None,
            ctypes.c_int(_GSS_C_INITIATE),
            ctypes.byref(store),
            ctypes.byref(self.credential),
            None,
            None,
        )
        self.gss.check("acquiring client credentials", major, minor)
        self.context = ctypes.c_void_p()
        self.established = False

    def _import(self, principal: str) -> ctypes.c_void_p:
        minor = ctypes.c_uint32(0)
        text = principal.encode()
        buffer = _Buffer(len(text), ctypes.cast(ctypes.c_char_p(text), ctypes.c_void_p))
        name = ctypes.c_void_p()
        major = self.gss.lib.gss_import_name(
            ctypes.byref(minor),
            ctypes.byref(buffer),
            self.gss.principal_name,
            ctypes.byref(name),
        )
        self.gss.check(f"importing {principal}", major, minor)
        return name

    @property
    def scheme_name(self) -> str:
        return "Kerberos" if self.scheme == "kerberos" else "Negotiate"

    @property
    def protocol(self) -> str:
        return (
            "application/HTTP-Kerberos-session-encrypted"
            if self.scheme == "kerberos"
            else "application/HTTP-SPNEGO-session-encrypted"
        )

    def step(self, token: bytes | None = None) -> bytes:
        """Advances the context and returns the token for the collector."""
        c = ctypes
        lib = self.gss.lib
        minor = c.c_uint32(0)
        data = c.create_string_buffer(token or b"", len(token or b""))
        input_token = _Buffer(len(token or b""), c.cast(data, c.c_void_p))
        output = _Buffer()
        flags = c.c_uint32(0)
        mechanism = (
            self.gss.krb5_mechanism
            if self.scheme == "kerberos"
            else self.gss.spnego_mechanism
        )
        major = lib.gss_init_sec_context(
            c.byref(minor),
            self.credential,
            c.byref(self.context),
            self.target,
            mechanism,
            c.c_uint32(
                _GSS_C_MUTUAL_FLAG
                | _GSS_C_REPLAY_FLAG
                | _GSS_C_SEQUENCE_FLAG
                | _GSS_C_CONF_FLAG
                | _GSS_C_INTEG_FLAG
            ),
            c.c_uint32(0),
            None,
            c.byref(input_token) if token else None,
            None,
            c.byref(output),
            c.byref(flags),
            None,
        )
        self.gss.check("initiating the security context", major, minor)
        self.established = not (major & _GSS_S_CONTINUE_NEEDED)
        result = c.string_at(output.value, output.length) if output.length else b""
        lib.gss_release_buffer(c.byref(minor), c.byref(output))
        return result

    def authorization(self) -> str:
        return f"{self.scheme_name} {base64.b64encode(self.step()).decode()}"

    def complete(self, www_authenticate: str | None) -> None:
        """Completes mutual authentication with the token of the collector."""
        if not www_authenticate:
            raise AssertionError("collector did not authenticate itself")
        scheme, _, token = www_authenticate.partition(" ")
        if scheme.lower() != self.scheme_name.lower():
            raise AssertionError(f"collector answered with scheme {scheme!r}")
        self.step(base64.b64decode(token))
        if not self.established:
            raise AssertionError("mutual authentication did not complete")

    def wrap(self, plaintext: bytes) -> bytes:
        """Encrypts a message into the payload of an encrypted body."""
        lib = self.gss.lib
        minor = ctypes.c_uint32(0)
        data = ctypes.create_string_buffer(plaintext, len(plaintext))
        iov = (_Iov * 3)(
            _Iov(
                _GSS_IOV_BUFFER_TYPE_HEADER | _GSS_IOV_BUFFER_FLAG_ALLOCATE, _Buffer()
            ),
            _Iov(
                _GSS_IOV_BUFFER_TYPE_DATA,
                _Buffer(len(plaintext), ctypes.cast(data, ctypes.c_void_p)),
            ),
            _Iov(
                _GSS_IOV_BUFFER_TYPE_PADDING | _GSS_IOV_BUFFER_FLAG_ALLOCATE, _Buffer()
            ),
        )
        confidential = ctypes.c_int(0)
        major = lib.gss_wrap_iov(
            ctypes.byref(minor),
            self.context,
            ctypes.c_int(1),
            ctypes.c_uint32(0),
            ctypes.byref(confidential),
            iov,
            ctypes.c_int(3),
        )
        self.gss.check("encrypting the request", major, minor)
        header = ctypes.string_at(iov[0].buffer.value, iov[0].buffer.length)
        padding = (
            ctypes.string_at(iov[2].buffer.value, iov[2].buffer.length)
            if iov[2].buffer.length
            else b""
        )
        sealed = data.raw[: len(plaintext)]
        lib.gss_release_iov_buffer(ctypes.byref(minor), iov, ctypes.c_int(3))
        return struct.pack("<I", len(header)) + header + sealed + padding

    def unwrap(self, payload: bytes) -> bytes:
        """Decrypts the payload of an encrypted body."""
        lib = self.gss.lib
        minor = ctypes.c_uint32(0)
        (size,) = struct.unpack("<I", payload[:4])
        header = ctypes.create_string_buffer(payload[4 : 4 + size], size)
        sealed = payload[4 + size :]
        data = ctypes.create_string_buffer(sealed, len(sealed))
        iov = (_Iov * 2)(
            _Iov(
                _GSS_IOV_BUFFER_TYPE_HEADER,
                _Buffer(size, ctypes.cast(header, ctypes.c_void_p)),
            ),
            _Iov(
                _GSS_IOV_BUFFER_TYPE_DATA,
                _Buffer(len(sealed), ctypes.cast(data, ctypes.c_void_p)),
            ),
        )
        confidential = ctypes.c_int(0)
        quality = ctypes.c_uint32(0)
        major = lib.gss_unwrap_iov(
            ctypes.byref(minor),
            self.context,
            ctypes.byref(confidential),
            ctypes.byref(quality),
            iov,
            ctypes.c_int(2),
        )
        self.gss.check("decrypting the response", major, minor)
        return data.raw[: iov[1].buffer.length]

    def content_type(self) -> str:
        return f'multipart/encrypted;protocol="{self.protocol}";boundary="{BOUNDARY}"'

    def encrypt_body(self, plaintext: bytes) -> bytes:
        """Builds a `multipart/encrypted` body like Windows does."""
        return (
            (
                f"--{BOUNDARY}\r\n"
                f"\tContent-Type: {self.protocol}\r\n"
                "\tOriginalContent: type=application/soap+xml;charset=UTF-16;"
                f"Length={len(plaintext)}\r\n"
                f"--{BOUNDARY}\r\n"
                "\tContent-Type: application/octet-stream\r\n"
            ).encode()
            + self.wrap(plaintext)
            + f"--{BOUNDARY}--\r\n".encode()
        )

    def decrypt_body(self, content_type: str, body: bytes) -> bytes:
        """Decrypts a `multipart/encrypted` body of the collector."""
        if self.protocol.lower() not in content_type.lower():
            raise AssertionError(f"unexpected content type {content_type!r}")
        match = re.search(r'boundary="([^"]+)"', content_type)
        if match is None:
            raise AssertionError(f"no boundary in {content_type!r}")
        part = f"--{match.group(1)}\r\n".encode()
        end = f"--{match.group(1)}--".encode()
        _, _, rest = body.partition(part)
        control, _, rest = rest.partition(part)
        if self.protocol.encode().lower() not in control.lower():
            raise AssertionError(f"unexpected control part {control!r}")
        _, _, rest = rest.partition(b"\r\n")
        rest = rest.removesuffix(b"\r\n")
        if not rest.endswith(end):
            raise AssertionError("encrypted body has no closing boundary")
        return self.unwrap(rest[: -len(end)])

    def close(self) -> None:
        lib = self.gss.lib
        minor = ctypes.c_uint32(0)
        if self.context:
            lib.gss_delete_sec_context(
                ctypes.byref(minor), ctypes.byref(self.context), None
            )
        for name in (self.name, self.target):
            lib.gss_release_name(ctypes.byref(minor), ctypes.byref(name))
        if self.credential:
            lib.gss_release_cred(ctypes.byref(minor), ctypes.byref(self.credential))
