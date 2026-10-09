"""A Windows Event Forwarding client for testing `accept_wef`.

The client speaks the source-initiated subscription protocol over mutual TLS
like a Windows forwarder does: it enumerates subscriptions at a subscription
manager URL, then sends heartbeats and event batches to the delivery address of
each subscription. Bodies are UTF-16LE SOAP envelopes, optionally compressed
with SLDC (ECMA-321).

With `kerberos`, the client authenticates its machine account with Kerberos
instead, over plain HTTP, and encrypts every message with the Kerberos session
of the connection, like Windows hosts in a domain do.

Both the declarative `wef` fixture and Python-runner tests use this module.
"""

from __future__ import annotations

import hashlib
import html
import http.client
import os
import re
import shlex
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import urlsplit
from xml.etree import ElementTree
from xml.sax.saxutils import escape, quoteattr

from . import wef_kerberos

NS = {
    "s": "http://www.w3.org/2003/05/soap-envelope",
    "a": "http://schemas.xmlsoap.org/ws/2004/08/addressing",
    "e": "http://schemas.xmlsoap.org/ws/2004/08/eventing",
    "n": "http://schemas.xmlsoap.org/ws/2004/09/enumeration",
    "w": "http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd",
    "p": "http://schemas.microsoft.com/wbem/wsman/1/wsman.xsd",
    "m": "http://schemas.microsoft.com/wbem/wsman/1/subscription",
    "auth": "http://schemas.microsoft.com/wbem/wsman/1/authentication",
    "c": "http://schemas.xmlsoap.org/ws/2002/12/policy",
}
ANONYMOUS = "http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous"
EVENT_NS = "http://schemas.microsoft.com/win/2004/08/events/event"
ACTION_ENUMERATE = "http://schemas.xmlsoap.org/ws/2004/09/enumeration/Enumerate"
ACTION_HEARTBEAT = "http://schemas.dmtf.org/wbem/wsman/1/wsman/Heartbeat"
ACTION_EVENTS = "http://schemas.dmtf.org/wbem/wsman/1/wsman/Events"
ACTION_EVENT = "http://schemas.dmtf.org/wbem/wsman/1/wsman/Event"
ACTION_END = "http://schemas.microsoft.com/wbem/wsman/1/wsman/End"
ACTION_SUBSCRIPTION_END = (
    "http://schemas.xmlsoap.org/ws/2004/08/eventing/SubscriptionEnd"
)
ACTION_ACK = "http://schemas.dmtf.org/wbem/wsman/1/wsman/Ack"
SUBSCRIPTION_MANAGER = "/wsman/SubscriptionManager/WEC"
SOAP_CONTENT_TYPE = "application/soap+xml;charset=UTF-16"

# -- PKI ----------------------------------------------------------------------


def _openssl(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["openssl", *args],
        cwd=cwd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


@dataclass
class Pki:
    """Certificates for a collector and its clients.

    A root CA issues the server certificate. It also issues the client
    certificates, or, with `intermediate`, an intermediate CA does, and clients
    send the intermediate CA along with their certificate.
    """

    directory: Path
    ca_name: str = "Tenzir WEF Test CA"
    intermediate: bool = False
    clients: dict[str, tuple[Path, Path]] = field(default_factory=dict)

    @property
    def ca_cert(self) -> Path:
        return self.directory / "ca.pem"

    @property
    def ca_key(self) -> Path:
        return self.directory / "ca-key.pem"

    @property
    def intermediate_cert(self) -> Path:
        return self.directory / "intermediate.pem"

    @property
    def intermediate_key(self) -> Path:
        return self.directory / "intermediate-key.pem"

    @property
    def server_cert(self) -> Path:
        return self.directory / "server.pem"

    @property
    def server_key(self) -> Path:
        return self.directory / "server-key.pem"

    @classmethod
    def create(
        cls,
        directory: Path,
        ca_name: str = "Tenzir WEF Test CA",
        *,
        intermediate: bool = False,
    ) -> Pki:
        directory.mkdir(parents=True, exist_ok=True)
        pki = cls(directory=directory, ca_name=ca_name, intermediate=intermediate)
        _openssl(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            f"/CN={ca_name}",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-keyout",
            str(pki.ca_key),
            "-out",
            str(pki.ca_cert),
            cwd=directory,
        )
        if intermediate:
            pki._issue(
                "intermediate",
                f"{ca_name} Intermediate",
                "basicConstraints=critical,CA:TRUE,pathlen:0\n",
                issuer=(pki.ca_cert, pki.ca_key),
                key_usage="keyCertSign,cRLSign",
            )
        pki._issue(
            "server",
            "localhost",
            "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
            issuer=(pki.ca_cert, pki.ca_key),
        )
        return pki

    def _issue(
        self,
        stem: str,
        common_name: str,
        extensions: str,
        *,
        issuer: tuple[Path, Path],
        key_usage: str = "digitalSignature,keyEncipherment",
    ) -> tuple[Path, Path]:
        cert = self.directory / f"{stem}.pem"
        key = self.directory / f"{stem}-key.pem"
        csr = self.directory / f"{stem}.csr"
        ext = self.directory / f"{stem}.ext"
        ext.write_text(
            extensions
            + f"keyUsage=critical,{key_usage}\n"
            + "authorityKeyIdentifier=keyid,issuer\n"
            + "subjectKeyIdentifier=hash\n"
        )
        _openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            f"/CN={common_name}",
            "-keyout",
            str(key),
            "-out",
            str(csr),
            cwd=self.directory,
        )
        _openssl(
            "x509",
            "-req",
            "-in",
            str(csr),
            "-CA",
            str(issuer[0]),
            "-CAkey",
            str(issuer[1]),
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            str(ext),
            "-out",
            str(cert),
            cwd=self.directory,
        )
        return cert, key

    def client(self, name: str) -> tuple[Path, Path]:
        """Returns the certificate chain and key of a client, issuing them
        first."""
        if name not in self.clients:
            stem = f"client-{len(self.clients)}"
            if self.intermediate:
                cert, key = self._issue(
                    stem,
                    name,
                    "extendedKeyUsage=clientAuth\n",
                    issuer=(self.intermediate_cert, self.intermediate_key),
                )
                chain = self.directory / f"{stem}-chain.pem"
                chain.write_text(cert.read_text() + self.intermediate_cert.read_text())
                self.clients[name] = (chain, key)
            else:
                self.clients[name] = self._issue(
                    stem,
                    name,
                    "extendedKeyUsage=clientAuth\n",
                    issuer=(self.ca_cert, self.ca_key),
                )
        return self.clients[name]

    def issuer_thumbprint(self) -> str:
        """The SHA-1 thumbprint of the CA that issues client certificates as
        lowercase hex."""
        issuer = self.intermediate_cert if self.intermediate else self.ca_cert
        der = ssl.PEM_cert_to_DER_cert(issuer.read_text())
        return hashlib.sha1(der).hexdigest()


def temporary_pki(prefix: str = "wef-pki-") -> Pki:
    return Pki.create(Path(tempfile.mkdtemp(prefix=prefix)))


def remove_pki(pki: Pki) -> None:
    shutil.rmtree(pki.directory, ignore_errors=True)


# -- SLDC ---------------------------------------------------------------------

_SLDC_RESET_1 = 0b1111111110101
_SLDC_RESET_2 = 0b1111111110110
_SLDC_END_OF_RECORD = 0b1111111110100
_SLDC_HISTORY = 1024
_SLDC_MAX_MATCH = 271


class _BitWriter:
    def __init__(self) -> None:
        self._value = 0
        self._bits = 0

    def write(self, value: int, bits: int) -> None:
        self._value = (self._value << bits) | (value & ((1 << bits) - 1))
        self._bits += bits

    def finish(self) -> bytes:
        padding = -self._bits % 8
        self.write(0, padding)
        return self._value.to_bytes(self._bits // 8, "big")


def _write_match_count(writer: _BitWriter, length: int) -> None:
    if length < 4:
        writer.write(length - 2, 2)
    elif length < 8:
        writer.write(0b10, 2)
        writer.write(length - 4, 2)
    elif length < 16:
        writer.write(0b110, 3)
        writer.write(length - 8, 3)
    elif length < 32:
        writer.write(0b1110, 4)
        writer.write(length - 16, 4)
    else:
        writer.write(0b1111, 4)
        writer.write(length - 32, 8)


def sldc_compress(data: bytes, scheme: int = 1) -> bytes:
    """Compresses one SLDC record.

    Scheme 1 replaces repeated byte sequences with copy pointers into a
    1024-byte history buffer. Scheme 2 passes bytes through.
    """
    writer = _BitWriter()
    if scheme == 2:
        writer.write(_SLDC_RESET_2, 13)
        for byte in data:
            writer.write(byte, 8)
            if byte == 0xFF:
                writer.write(0, 1)
    else:
        writer.write(_SLDC_RESET_1, 13)
        candidates: dict[bytes, list[int]] = {}
        i = 0
        while i < len(data):
            best_length = 0
            best_position = 0
            for position in reversed(candidates.get(data[i : i + 2], [])[-32:]):
                if i - position >= _SLDC_HISTORY:
                    continue
                length = 0
                while (
                    length < _SLDC_MAX_MATCH
                    and i + length < len(data)
                    and data[position + length] == data[i + length]
                ):
                    length += 1
                if length > best_length:
                    best_length, best_position = length, position
            step = best_length if best_length >= 2 else 1
            if best_length >= 2:
                writer.write(1, 1)
                _write_match_count(writer, best_length)
                writer.write(best_position % _SLDC_HISTORY, 10)
            else:
                writer.write(0, 1)
                writer.write(data[i], 8)
            for k in range(i, i + step):
                candidates.setdefault(data[k : k + 2], []).append(k)
            i += step
    writer.write(_SLDC_END_OF_RECORD, 13)
    return writer.finish()


# -- Envelopes ----------------------------------------------------------------


def new_message_id() -> str:
    # Windows sends uppercase UUIDs; collectors must echo them byte for byte.
    return f"uuid:{str(uuid.uuid4()).upper()}"


def encode_body(xml: str) -> bytes:
    # Pass lone surrogates through, like old Windows hosts do.
    return b"\xff\xfe" + xml.encode("utf-16-le", errors="surrogatepass")


def decode_body(body: bytes) -> str:
    if body.startswith(b"\xff\xfe"):
        return body[2:].decode("utf-16-le")
    return body.decode("utf-8")


def bookmark_xml(bookmark: Mapping[str, int] | str) -> str:
    """Builds a `BookmarkList` from channel names and record IDs."""
    if isinstance(bookmark, str):
        return bookmark
    entries = "".join(
        f"<Bookmark Channel={quoteattr(channel)} RecordId={quoteattr(str(record))}"
        f"{' IsCurrent=' + quoteattr('true') if index == len(bookmark) - 1 else ''}/>"
        for index, (channel, record) in enumerate(bookmark.items())
    )
    return f"<BookmarkList>{entries}</BookmarkList>"


def covering_bookmark(events: Iterable[str]) -> dict[str, int]:
    """Returns the bookmark that Windows sends with `events`, which holds the
    last record ID of every channel. Empty if no event names both."""
    bookmark: dict[str, int] = {}
    for event in events:
        channel = re.search(r"<Channel>([^<]*)</Channel>", event)
        record = re.search(r"<EventRecordID>(\d+)</EventRecordID>", event)
        if channel and record:
            name = html.unescape(channel[1])
            bookmark[name] = max(bookmark.get(name, 0), int(record[1]))
    return bookmark


def event_xml(
    event_id: int = 4624,
    record: int = 1,
    channel: str = "Security",
    computer: str = "win10.example.org",
    provider: str = "Microsoft-Windows-Security-Auditing",
    data: Mapping[str, str] | None = None,
) -> str:
    """Builds a Windows event in the `Raw` content format."""
    event_data = "".join(
        f"<Data Name={quoteattr(name)}>{escape(value)}</Data>"
        for name, value in (data or {}).items()
    )
    return (
        f"<Event xmlns='{EVENT_NS}'><System>"
        f"<Provider Name={quoteattr(provider)}/>"
        f"<EventID>{event_id}</EventID><Version>0</Version><Level>0</Level>"
        f"<Task>12544</Task><Opcode>0</Opcode>"
        f"<Keywords>0x8020000000000000</Keywords>"
        f"<TimeCreated SystemTime='2026-01-01T00:00:00.0000000Z'/>"
        f"<EventRecordID>{record}</EventRecordID>"
        f"<Channel>{escape(channel)}</Channel>"
        f"<Computer>{escape(computer)}</Computer><Security/></System>"
        f"<EventData>{event_data}</EventData></Event>"
    )


def envelope(
    action: str,
    to: str,
    *,
    message_id: str | None = None,
    machine_id: str | None = "win10.example.org",
    headers: str = "",
    body: str = "",
    max_envelope_size: int = 512000,
) -> tuple[str, str]:
    """Builds a request envelope and returns its message ID and XML."""
    message_id = message_id or new_message_id()
    machine = (
        f'<m:MachineID xmlns:m="http://schemas.microsoft.com/wbem/wsman/1/machineid" '
        f's:mustUnderstand="false">{escape(machine_id)}</m:MachineID>'
        if machine_id is not None
        else ""
    )
    xml = (
        f'<s:Envelope xmlns:s="{NS["s"]}" xmlns:a="{NS["a"]}" '
        f'xmlns:e="{NS["e"]}" xmlns:n="{NS["n"]}" xmlns:w="{NS["w"]}" '
        f'xmlns:p="{NS["p"]}"><s:Header>'
        f"<a:To>{escape(to)}</a:To>{machine}"
        f'<a:ReplyTo><a:Address s:mustUnderstand="true">{ANONYMOUS}</a:Address>'
        f"</a:ReplyTo>"
        f'<a:Action s:mustUnderstand="true">{action}</a:Action>'
        f'<w:MaxEnvelopeSize s:mustUnderstand="true">{max_envelope_size}'
        f"</w:MaxEnvelopeSize>"
        f"<a:MessageID>{message_id}</a:MessageID>"
        f'<w:Locale xml:lang="en-US" s:mustUnderstand="false"/>'
        f'<p:DataLocale xml:lang="en-US" s:mustUnderstand="false"/>'
        f'<p:SessionId s:mustUnderstand="false">{new_message_id()}</p:SessionId>'
        f'<p:OperationID s:mustUnderstand="false">{new_message_id()}</p:OperationID>'
        f'<p:SequenceId s:mustUnderstand="false">1</p:SequenceId>'
        f"<w:OperationTimeout>PT60.000S</w:OperationTimeout>"
        f"{headers}</s:Header><s:Body>{body}</s:Body></s:Envelope>"
    )
    return message_id, xml


# -- Responses ----------------------------------------------------------------


@dataclass
class Subscription:
    """A subscription as a client sees it in an `EnumerateResponse`."""

    name: str
    identifier: str
    version: str
    address: str
    authentication: str | None
    thumbprint: str | None
    bookmark: str | None
    query: str
    options: dict[str, str | None]
    heartbeat: str
    max_time: str
    max_envelope_size: int
    xml: ElementTree.Element

    @property
    def path(self) -> str:
        return urlsplit(self.address).path


def _text(element: ElementTree.Element | None) -> str | None:
    return None if element is None else (element.text or "")


def _inner_xml(element: ElementTree.Element | None) -> str | None:
    if element is None or len(element) == 0:
        return None if element is None else (element.text or "").strip() or None
    return "".join(ElementTree.tostring(child, encoding="unicode") for child in element)


def parse_subscriptions(root: ElementTree.Element) -> list[Subscription]:
    result = []
    items = root.find("s:Body/n:EnumerateResponse/w:Items", NS)
    if items is None:
        raise AssertionError("EnumerateResponse has no w:Items")
    xsi_nil = "{http://www.w3.org/2001/XMLSchema-instance}nil"
    for item in items.findall("m:Subscription", NS):
        subscribe = item.find("s:Envelope", NS)
        assert subscribe is not None
        header = subscribe.find("s:Header", NS)
        body = subscribe.find("s:Body/e:Subscribe", NS)
        assert header is not None and body is not None
        options = {
            option.get("Name", ""): (
                None if option.get(xsi_nil) == "true" else (option.text or "")
            )
            for option in header.findall("w:OptionSet/w:Option", NS)
        }
        notify = body.find("e:Delivery/e:NotifyTo", NS)
        assert notify is not None
        authentication = notify.find(
            "c:Policy/c:ExactlyOne/c:All/auth:Authentication", NS
        )
        filter_ = body.find("w:Filter", NS)
        result.append(
            Subscription(
                name=options.get("SubscriptionName") or "",
                identifier=_text(notify.find("a:ReferenceParameters/e:Identifier", NS))
                or "",
                version=_text(notify.find("a:ReferenceParameters/Version", NS)) or "",
                address=_text(notify.find("a:Address", NS)) or "",
                authentication=(
                    None if authentication is None else authentication.get("Profile")
                ),
                thumbprint=_text(
                    notify.find(
                        "c:Policy/c:ExactlyOne/c:All/auth:Authentication/"
                        "auth:ClientCertificate/auth:Thumbprint",
                        NS,
                    )
                ),
                bookmark=_inner_xml(body.find("w:Bookmark", NS)),
                query=_inner_xml(filter_) or "",
                options=options,
                heartbeat=_text(body.find("e:Delivery/w:Heartbeats", NS)) or "",
                max_time=_text(body.find("e:Delivery/w:MaxTime", NS)) or "",
                max_envelope_size=int(
                    _text(body.find("e:Delivery/w:MaxEnvelopeSize", NS)) or 0
                ),
                xml=item,
            )
        )
    return result


@dataclass
class Response:
    status: int
    body: bytes
    content_type: str
    text: str = ""
    xml: ElementTree.Element | None = None

    @property
    def action(self) -> str | None:
        if self.xml is None:
            return None
        return _text(self.xml.find("s:Header/a:Action", NS))

    @property
    def relates_to(self) -> str | None:
        if self.xml is None:
            return None
        return _text(self.xml.find("s:Header/a:RelatesTo", NS))


# -- Client -------------------------------------------------------------------


class KerberosRejected(OSError):
    """The collector rejected the Kerberos authentication of a connection."""

    def __init__(self, status: int) -> None:
        super().__init__(f"Kerberos authentication failed with HTTP {status}")
        self.status = status


class WefClient:
    """A Windows forwarder that talks to one collector.

    The client trusts the server certificate from `pki` and authenticates with
    a client certificate from `client_pki` or `pki`. Alternatively, `kerberos`
    names the authentication scheme: `kerberos` or `negotiate`.
    """

    def __init__(
        self,
        host: str,
        port: int,
        pki: Pki | None,
        name: str = "win10.example.org",
        *,
        machine_id: str | None = None,
        timeout: float = 10,
        hostname: str = "localhost",
        kerberos: str | None = None,
        client_pki: Pki | None = None,
        tls_max_version: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.name = name
        self.machine_id = machine_id if machine_id is not None else name
        self.timeout = timeout
        self.hostname = hostname
        self.kerberos = kerberos
        self.subscriptions: dict[str, Subscription] = {}
        self._connection: http.client.HTTPConnection | None = None
        self._session: wef_kerberos.KerberosClient | None = None
        if kerberos:
            kdc = wef_kerberos.shared_kdc()
            self._principal = wef_kerberos.principal_of(name)
            self._keytab = kdc.keytab(
                self._principal, kdc.directory / f"client-{uuid.uuid4()}.keytab"
            )
        else:
            assert pki is not None
            cert, key = (client_pki or pki).client(name)
            self._context = ssl.create_default_context(cafile=str(pki.ca_cert))
            self._context.load_cert_chain(str(cert), str(key))
            if tls_max_version is not None:
                self._context.maximum_version = _TLS_VERSIONS[tls_max_version]

    @property
    def scheme(self) -> str:
        return "http" if self.kerberos else "https"

    def _connect(self) -> http.client.HTTPConnection:
        if self._connection is None:
            if self.kerberos:
                self._connection = http.client.HTTPConnection(
                    self.host, self.port, timeout=self.timeout
                )
            else:
                self._connection = http.client.HTTPSConnection(
                    self.host, self.port, context=self._context, timeout=self.timeout
                )
            self._connection.connect()
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._session is not None:
            self._session.close()
            self._session = None

    def _authenticate(self, path: str) -> None:
        """Authenticates the connection, as Windows does before it sends."""
        session = wef_kerberos.KerberosClient(
            self._principal, self._keytab, scheme=self.kerberos or "kerberos"
        )
        connection = self._connect()
        connection.request(
            "POST",
            path,
            body=b"",
            headers={"Authorization": session.authorization(), "Content-Length": "0"},
        )
        raw = connection.getresponse()
        raw.read()
        if raw.status != 200:
            session.close()
            raise KerberosRejected(raw.status)
        session.complete(raw.getheader("WWW-Authenticate"))
        self._session = session

    def wait_until_listening(
        self,
        deadline: float = 60,
        process: subprocess.Popen[str] | None = None,
        stop: threading.Event | None = None,
    ) -> bool:
        """Waits until the collector accepts TLS connections.

        Fails early with the collector's errors if `process` exits. Returns
        `False` if `stop` is set before the collector listens.
        """
        end = time.monotonic() + deadline
        last: Exception | None = None
        while time.monotonic() < end:
            if stop is not None and stop.is_set():
                return False
            if process is not None and process.poll() is not None:
                raise AssertionError(
                    f"collector exited with {process.returncode}: "
                    f"{process.stderr.read() if process.stderr else ''}"
                )
            try:
                self._connect()
                return True
            except (OSError, ssl.SSLError) as exc:
                last = exc
                self.close()
                time.sleep(0.05)
        message = f"collector did not start listening: {last}"
        if process is not None and process.poll() is None:
            # Stop the collector to show what it reported in the meantime.
            process.kill()
            _, stderr = process.communicate(timeout=10)
            message += f"\ncollector stderr:\n{stderr}"
        raise AssertionError(message)

    def post(
        self,
        path: str,
        body: bytes,
        *,
        content_encoding: str | None = None,
        content_type: str = SOAP_CONTENT_TYPE,
        method: str = "POST",
        encrypt: bool = True,
    ) -> Response:
        headers = {"Content-Type": content_type}
        if content_encoding:
            headers["Content-Encoding"] = content_encoding
        try:
            if self.kerberos and self._session is None:
                self._authenticate(path)
            if self._session is not None and body and encrypt:
                headers["Content-Type"] = self._session.content_type()
                body = self._session.encrypt_body(body)
            connection = self._connect()
            connection.request(method, path, body=body, headers=headers)
            raw = connection.getresponse()
            data = raw.read()
        except (OSError, http.client.HTTPException):
            self.close()
            raise
        response = Response(
            status=raw.status,
            body=data,
            content_type=raw.getheader("Content-Type") or "",
        )
        if data and self._session is not None:
            data = self._session.decrypt_body(response.content_type, data)
        if raw.getheader("Connection", "").lower() == "close":
            self.close()
        if data:
            response.text = decode_body(data)
            response.xml = ElementTree.fromstring(response.text)
        return response

    def send(
        self,
        path: str,
        xml: str,
        *,
        compression: str | None = None,
    ) -> Response:
        body = encode_body(xml)
        encoding = None
        if compression in ("sldc", "sldc1"):
            body, encoding = sldc_compress(body, 1), "SLDC"
        elif compression == "sldc2":
            body, encoding = sldc_compress(body, 2), "SLDC"
        elif compression == "claimed":
            # Windows sometimes declares SLDC for an uncompressed body.
            encoding = "SLDC"
        return self.post(path, body, content_encoding=encoding)

    def manager_url(self, uri: str = SUBSCRIPTION_MANAGER) -> str:
        return f"{self.scheme}://{self.hostname}:{self.port}{uri}"

    def enumerate(
        self, uri: str = SUBSCRIPTION_MANAGER, *, max_envelope_size: int = 512000
    ) -> Response:
        message_id, xml = envelope(
            ACTION_ENUMERATE,
            self.manager_url(uri),
            machine_id=self.machine_id,
            max_envelope_size=max_envelope_size,
            body="<n:Enumerate><w:OptimizeEnumeration/>"
            "<w:MaxElements>32000</w:MaxElements></n:Enumerate>",
        )
        response = self.send(uri, xml)
        _check_relates_to(response, message_id)
        if response.status == 200 and response.xml is not None:
            self.subscriptions = {
                subscription.name: subscription
                for subscription in parse_subscriptions(response.xml)
            }
        return response

    def _delivery(
        self,
        action: str,
        subscription: Subscription | str,
        *,
        identifier: str | None = None,
        version: str | None = None,
        headers: str = "",
        body: str = "",
        compression: str | None = None,
    ) -> Response:
        if isinstance(subscription, str):
            subscription = self.subscriptions[subscription]
        identifier = identifier if identifier is not None else subscription.identifier
        version = version if version is not None else subscription.version
        references = ""
        if identifier:
            references += f"<e:Identifier>{escape(identifier)}</e:Identifier>"
        if version:
            references += f"<Version>{escape(version)}</Version>"
        message_id, xml = envelope(
            action,
            subscription.address,
            machine_id=self.machine_id,
            headers=references + headers,
            body=body,
        )
        response = self.send(subscription.path, xml, compression=compression)
        _check_relates_to(response, message_id)
        return response

    def heartbeat(self, subscription: Subscription | str, **kwargs) -> Response:
        return self._delivery(
            ACTION_HEARTBEAT,
            subscription,
            headers="<w:AckRequested/>",
            body="<w:Events></w:Events>",
            **kwargs,
        )

    def events(
        self,
        subscription: Subscription | str,
        events: Iterable[str],
        *,
        bookmark: Mapping[str, int] | str | bool | None = None,
        body: str | None = None,
        **kwargs,
    ) -> Response:
        """Sends a batch of events, or `body` instead of the `w:Events`.

        Like Windows, the batch carries a bookmark, which defaults to the one
        that covers `events`. With `bookmark=False`, it carries none.
        """
        events = list(events)
        if bookmark is None:
            bookmark = covering_bookmark(events) or False
        headers = ""
        if bookmark is not False:
            headers += f"<w:Bookmark>{bookmark_xml(bookmark)}</w:Bookmark>"
        headers += "<w:AckRequested/>"
        if body is None:
            body = "<w:Events>{}</w:Events>".format(
                "".join(
                    f'<w:Event Action="{ACTION_EVENT}"><![CDATA[{event}]]></w:Event>'
                    for event in events
                )
            )
        return self._delivery(
            ACTION_EVENTS,
            subscription,
            headers=headers,
            body=body,
            **kwargs,
        )

    def subscription_end(
        self,
        subscription: Subscription | str,
        *,
        status: str = "http://schemas.xmlsoap.org/ws/2004/08/eventing/SourceCancelling",
        fault: str = "Windows Event Forward Plugin failed to read events.",
    ) -> Response:
        if isinstance(subscription, str):
            subscription = self.subscriptions[subscription]
        body = (
            "<e:SubscriptionEnd><e:SubscriptionManager>"
            f"<a:Address>{escape(subscription.address)}</a:Address>"
            "</e:SubscriptionManager>"
            f"<e:Status>{escape(status)}</e:Status>"
            '<f:WSManFault xmlns:f="http://schemas.microsoft.com/wbem/wsman/1/wsmanfault" '
            f'Code="1717" Machine="{escape(self.machine_id)}"><f:Message>'
            '<f:ProviderFault provider="Unknown provider" path="Unknown path">'
            '<t:ProviderError xmlns:t="http://schemas.microsoft.com/wbem/wsman/1/windows/EventLog">'
            f"{escape(fault)}</t:ProviderError></f:ProviderFault></f:Message>"
            "</f:WSManFault></e:SubscriptionEnd>"
        )
        return self._delivery(
            ACTION_SUBSCRIPTION_END,
            subscription,
            headers="<w:AckRequested/>",
            body=body,
        )

    def end(self, subscription: Subscription | str) -> Response:
        if isinstance(subscription, str):
            subscription = self.subscriptions[subscription]
        _, xml = envelope(ACTION_END, ANONYMOUS, machine_id=None)
        # Windows declares SLDC for this message without compressing it.
        return self.send(subscription.path, xml, compression="claimed")


# -- Collector ----------------------------------------------------------------


def free_port() -> int:
    """Returns a port that the kernel considers free.

    Python tests run in separate processes, so they cannot share the
    deterministic allocator of the fixtures.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


_TLS_VERSIONS = {
    "1.2": ssl.TLSVersion.TLSv1_2,
    "1.3": ssl.TLSVersion.TLSv1_3,
}


DEFAULT_QUERY = (
    "<QueryList><Query Id='0'><Select Path='Security'>*</Select></Query></QueryList>"
)
DEFAULT_SUBSCRIPTIONS = f'[{{id: "security", query: "{DEFAULT_QUERY}"}}]'


def collector_pipeline(
    port: int,
    pki: Pki,
    *,
    subscriptions: str = DEFAULT_SUBSCRIPTIONS,
    options: str = "",
    rest: str = "",
) -> str:
    """Returns a pipeline that runs `accept_wef` for the test PKI."""
    return (
        f'accept_wef "127.0.0.1:{port}",\n'
        f'  tls={{certfile: "{pki.server_cert}", keyfile: "{pki.server_key}", '
        f'client_ca: "{pki.ca_cert}", require_client_cert: true}},\n'
        f"  subscriptions={subscriptions}{options}\n{rest}"
    )


def kerberos_collector_pipeline(
    port: int,
    keytab: Path,
    *,
    subscriptions: str = DEFAULT_SUBSCRIPTIONS,
    options: str = "",
    rest: str = "",
) -> str:
    """Returns a pipeline that runs `accept_wef` with Kerberos."""
    return (
        f'accept_wef "127.0.0.1:{port}",\n'
        f'  kerberos={{keytab: "{keytab}", '
        f'principal: "{wef_kerberos.SERVICE}"}},\n'
        f"  subscriptions={subscriptions}{options}\n{rest}"
    )


def start_collector(
    pipeline: str, state_directory: Path, env: dict[str, str] | None = None
) -> subprocess.Popen[str]:
    """Runs a pipeline with `tenzir`, keeping its state in `state_directory`.

    `env` adds environment variables for the collector.
    """
    # Without a replay cache, parallel tests do not share files in /var/tmp.
    env = dict(
        os.environ,
        TENZIR_STATE_DIRECTORY=str(state_directory),
        KRB5RCACHETYPE="none",
        **(env or {}),
    )
    return subprocess.Popen(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--nova=true", pipeline],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _check_relates_to(response: Response, message_id: str) -> None:
    if response.status != 200 or response.xml is None:
        return
    if response.relates_to != message_id:
        raise AssertionError(
            f"RelatesTo {response.relates_to!r} does not match the request "
            f"MessageID {message_id!r}"
        )
