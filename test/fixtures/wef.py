"""Declarative Windows Event Forwarding scenarios for `accept_wef`.

The fixture creates a test PKI and runs a sequence of steps as simulated
Windows clients against the pipeline under test. Each step is a mapping with a
single action key:

```yaml
fixtures:
  - wef:
      clients: [win10.example.org]
      steps:
        - enumerate:
            expect: {subscriptions: [security]}
        - events:
            subscription: security
            events: [{id: 4624, record: 1}]
            bookmark: {Security: 1}
```

Actions:

- `enumerate`: Fetch the subscriptions. Options: `uri`, `machine_id`,
  `max_envelope_size` (the largest response that the client accepts).
  Expectations: `subscriptions` (names in order), `bookmarks` (by name, `null`
  for none), `options` (by name), `queries` (by name, compared as canonical
  XML), `address` (by name, `{port}` expands),
  `thumbprint` (`ca` for the CA that issues client certificates),
  `max_envelope_size`.
- `heartbeat`: Send a heartbeat for `subscription`, which names a subscription
  that this or another client enumerated.
- `events`: Send `events` for `subscription`, each an XML string or a mapping
  with `id`, `record`, `channel`, `computer`, `provider`, and `data`. Options:
  `bookmark` (channel to record ID, or XML; defaults to the last record ID per
  channel of the events, and `false` omits it), `compression` (`sldc1`,
  `sldc2`, or `claimed`), `body` (XML that replaces the `w:Events` element).
- `subscription_end`, `end`: End `subscription`.
- `raw`: Send a raw request. Options: `path`, `method`, `body` (XML, sent as
  UTF-16), `body_hex`, `body_size` (a body of that many bytes),
  `compression` (`sldc1` or `sldc2`, which sets `content_encoding`),
  `content_encoding`, `content_type`.
- `sleep`: Wait `seconds`.

Delivery steps accept `identifier` and `version` to override the reference
parameters. Every step accepts `client` (default: the first client) and
`expect.status` (default: 200). A successful heartbeat or events step must
return an `Ack` whose `RelatesTo` matches the request.

The pipeline typically ends with `head`, so the last step may see its
connection drop. Steps can opt into that with `may_drop: true`.

With `intermediate_ca: true`, an intermediate CA issues the client
certificates and clients send it along, while `WEF_CAFILE` holds only the root
CA. `tls_max_version` limits the TLS version that clients offer, e.g., `"1.2"`.

With `kerberos: true` (or `negotiate`), clients authenticate with Kerberos
against a KDC that the fixture runs, and the fixture sets `WEF_KEYTAB` and
`WEF_PRINCIPAL` for the collector instead. Client names are then machine
accounts, such as `WIN10$`. Enumerate steps can expect `authentication`
(`certificate` or `kerberos`).

Without steps, the fixture only provides the certificates.

The fixture sets `WEF_ENDPOINT`, `WEF_CERTFILE`, `WEF_KEYFILE`, and
`WEF_CAFILE` for the TLS configuration, and points `TENZIR_STATE_DIRECTORY` at
a fresh directory so that bookmarks do not leak between tests.
"""

from __future__ import annotations

import http.client
import shutil
import ssl
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from . import wef_client, wef_kerberos
from ._utils import find_free_port

_HOST = "127.0.0.1"
_CONNECT_DEADLINE = 30.0


@dataclass(frozen=True)
class WefOptions:
    clients: list[str] = field(default_factory=lambda: ["win10.example.org"])
    steps: list[dict[str, Any]] = field(default_factory=list)
    # Authenticate with Kerberos instead of certificates: `true` or `kerberos`
    # for the Kerberos scheme, `negotiate` for SPNEGO.
    kerberos: bool | str = False
    # Issue client certificates from an intermediate CA that is not part of
    # `WEF_CAFILE`, which holds only the root CA.
    intermediate_ca: bool = False
    # The highest TLS version that clients offer, e.g., `"1.2"` for Windows 10.
    tls_max_version: str | None = None

    @property
    def scheme(self) -> str | None:
        if self.kerberos is True:
            return "kerberos"
        return self.kerberos or None


class _StepFailure(Exception):
    pass


def _canonical(xml: str | None) -> str | None:
    if xml is None or not xml.startswith("<"):
        return xml
    return ElementTree.canonicalize(xml_data=xml, strip_text=True)


def _event(spec: Any) -> str:
    if isinstance(spec, str):
        return spec
    if not isinstance(spec, dict):
        raise _StepFailure(f"invalid event specification {spec!r}")
    arguments = dict(spec)
    if "id" in arguments:
        arguments["event_id"] = arguments.pop("id")
    return wef_client.event_xml(**arguments)


class _Scenario:
    def __init__(self, opts: WefOptions, port: int, pki: wef_client.Pki) -> None:
        self.opts = opts
        self.port = port
        self.pki = pki
        self.errors: list[str] = []
        self.stop = threading.Event()
        self.clients: dict[str, wef_client.WefClient] = {}

    def client(self, name: str | None) -> wef_client.WefClient:
        name = name or self.opts.clients[0]
        if name not in self.clients:
            self.clients[name] = wef_client.WefClient(
                _HOST,
                self.port,
                self.pki,
                name,
                kerberos=self.opts.scheme,
                tls_max_version=self.opts.tls_max_version,
            )
        return self.clients[name]

    def run(self) -> None:
        # Without steps, the fixture only provides certificates.
        if not self.opts.steps:
            return
        try:
            listening = self.client(None).wait_until_listening(
                _CONNECT_DEADLINE, stop=self.stop
            )
        except AssertionError as exc:
            self.errors.append(str(exc))
            return
        if not listening:
            # The pipeline ended before it listened, e.g., with an error.
            return
        for index, step in enumerate(self.opts.steps):
            if self.stop.is_set():
                return
            if not isinstance(step, dict) or len(step) != 1:
                self.errors.append(f"step {index}: expected a single action")
                return
            [(action, params)] = step.items()
            params = dict(params or {})
            last = index == len(self.opts.steps) - 1
            may_drop = params.pop("may_drop", last)
            try:
                self.step(action, params)
            except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
                if may_drop:
                    return
                self.errors.append(f"step {index} ({action}): {exc!r}")
                return
            except (_StepFailure, AssertionError, KeyError) as exc:
                self.errors.append(f"step {index} ({action}): {exc}")
                return
        for client in self.clients.values():
            client.close()

    def subscription(
        self, client: wef_client.WefClient, name: str
    ) -> wef_client.Subscription:
        """Looks up a subscription that `client` or any other client saw."""
        for candidate in (client, *self.clients.values()):
            if name in candidate.subscriptions:
                return candidate.subscriptions[name]
        raise _StepFailure(f"no client enumerated subscription `{name}`")

    def step(self, action: str, params: dict[str, Any]) -> None:
        if action == "sleep":
            self.stop.wait(float(params.get("seconds", 0)))
            return
        client = self.client(params.pop("client", None))
        expect = dict(params.pop("expect", None) or {})
        status = expect.pop("status", 200)
        try:
            response = self.act(client, action, params)
        except wef_client.KerberosRejected as rejected:
            response = wef_client.Response(
                status=rejected.status, body=b"", content_type=""
            )
        if response.status != status:
            raise _StepFailure(f"expected status {status}, got {response.status}")
        if action in ("heartbeat", "events") and status == 200:
            if response.action != wef_client.ACTION_ACK:
                raise _StepFailure(f"expected an Ack, got {response.action!r}")
        if action == "enumerate" and status == 200:
            self.check_enumerate(client, expect)
            expect = {}
        if expect:
            raise _StepFailure(f"unknown expectations {sorted(expect)}")

    def act(
        self, client: wef_client.WefClient, action: str, params: dict[str, Any]
    ) -> wef_client.Response:
        if action == "enumerate":
            if "machine_id" in params:
                client.machine_id = params.pop("machine_id")
            response = client.enumerate(
                params.pop("uri", wef_client.SUBSCRIPTION_MANAGER),
                max_envelope_size=int(params.pop("max_envelope_size", 512000)),
            )
        elif action == "heartbeat":
            subscription = self.subscription(client, params.pop("subscription"))
            response = client.heartbeat(subscription, **params)
            params = {}
        elif action == "events":
            events = [_event(event) for event in params.pop("events", [])]
            subscription = self.subscription(client, params.pop("subscription"))
            response = client.events(subscription, events, **params)
            params = {}
        elif action == "subscription_end":
            response = client.subscription_end(
                self.subscription(client, params.pop("subscription"))
            )
        elif action == "end":
            response = client.end(self.subscription(client, params.pop("subscription")))
        elif action == "raw":
            response = self.raw(client, params)
            params = {}
        else:
            raise _StepFailure(f"unknown action `{action}`")
        if params:
            raise _StepFailure(f"unknown options {sorted(params)}")
        return response

    def raw(
        self, client: wef_client.WefClient, params: dict[str, Any]
    ) -> wef_client.Response:
        path = params.pop("path", wef_client.SUBSCRIPTION_MANAGER)
        if "body_size" in params:
            body = b"x" * int(params.pop("body_size"))
        elif "body_hex" in params:
            body = bytes.fromhex(str(params.pop("body_hex")))
        else:
            body = wef_client.encode_body(str(params.pop("body", "")))
            if body == wef_client.encode_body(""):
                body = b""
        kwargs: dict[str, Any] = {}
        if "compression" in params:
            scheme = {"sldc1": 1, "sldc2": 2}[params.pop("compression")]
            body = wef_client.sldc_compress(body, scheme)
            kwargs["content_encoding"] = "SLDC"
        for key in ("content_encoding", "content_type", "method"):
            if key in params:
                kwargs[key] = params.pop(key)
        if params:
            raise _StepFailure(f"unknown options {sorted(params)}")
        return client.post(path, body, **kwargs)

    def check_enumerate(
        self, client: wef_client.WefClient, expect: dict[str, Any]
    ) -> None:
        subscriptions = client.subscriptions
        if "subscriptions" in expect:
            names = list(subscriptions)
            if names != expect.pop("subscriptions"):
                raise _StepFailure(f"unexpected subscriptions {names}")
        for name, bookmark in (expect.pop("bookmarks", None) or {}).items():
            actual = _canonical(subscriptions[name].bookmark)
            wanted = _canonical(
                None if bookmark is None else wef_client.bookmark_xml(bookmark)
            )
            if actual != wanted:
                raise _StepFailure(
                    f"subscription {name}: expected bookmark {wanted!r}, got {actual!r}"
                )
        for name, options in (expect.pop("options", None) or {}).items():
            for option, value in options.items():
                actual = subscriptions[name].options.get(option, "<missing>")
                if actual != value:
                    raise _StepFailure(
                        f"subscription {name}: expected option {option}={value!r}, "
                        f"got {actual!r}"
                    )
        for name, query in (expect.pop("queries", None) or {}).items():
            actual = _canonical(subscriptions[name].query)
            wanted = _canonical(query)
            if actual != wanted:
                raise _StepFailure(
                    f"subscription {name}: expected query {wanted!r}, got {actual!r}"
                )
        for name, address in (expect.pop("address", None) or {}).items():
            wanted = address.replace("{port}", str(self.port))
            if subscriptions[name].address != wanted:
                raise _StepFailure(
                    f"subscription {name}: expected address {wanted!r}, "
                    f"got {subscriptions[name].address!r}"
                )
        profiles = {
            "certificate": "http://schemas.dmtf.org/wbem/wsman/1/wsman/secprofile/https/mutual",
            "kerberos": "http://schemas.dmtf.org/wbem/wsman/1/wsman/secprofile/http/spnego-kerberos",
        }
        if "authentication" in expect:
            wanted = profiles[expect.pop("authentication")]
            for name, subscription in subscriptions.items():
                if subscription.authentication != wanted:
                    raise _StepFailure(
                        f"subscription {name}: expected profile {wanted!r}, "
                        f"got {subscription.authentication!r}"
                    )
        if "max_envelope_size" in expect:
            wanted = expect.pop("max_envelope_size")
            for name, subscription in subscriptions.items():
                if subscription.max_envelope_size != wanted:
                    raise _StepFailure(
                        f"subscription {name}: expected an envelope size of "
                        f"{wanted}, got {subscription.max_envelope_size}"
                    )
        if expect.pop("thumbprint", None) == "ca":
            for name, subscription in subscriptions.items():
                if subscription.thumbprint != self.pki.issuer_thumbprint():
                    raise _StepFailure(
                        f"subscription {name}: expected the CA thumbprint, "
                        f"got {subscription.thumbprint!r}"
                    )
        if expect:
            raise _StepFailure(f"unknown expectations {sorted(expect)}")


@fixture(name="wef", options=WefOptions)
def wef() -> FixtureHandle:
    opts = current_options("wef")
    if not isinstance(opts, WefOptions):
        raise TypeError("wef fixture options failed to parse")
    if not opts.clients:
        raise ValueError("wef fixture requires at least one client")
    directory = Path(tempfile.mkdtemp(prefix="wef-"))
    pki = wef_client.Pki.create(directory / "pki", intermediate=opts.intermediate_ca)
    env = {}
    if opts.scheme:
        try:
            kdc = wef_kerberos.shared_kdc()
        except wef_kerberos.KerberosUnavailable as exc:
            shutil.rmtree(directory, ignore_errors=True)
            raise FixtureUnavailable(str(exc)) from exc
        keytab = kdc.keytab(wef_kerberos.SERVICE, directory / "service.keytab")
        env.update(kdc.env)
        env.update(
            {
                "WEF_KEYTAB": str(keytab),
                "WEF_PRINCIPAL": wef_kerberos.SERVICE,
                # Without a replay cache, parallel tests do not share files.
                "KRB5RCACHETYPE": "none",
            }
        )
    else:
        for name in opts.clients:
            pki.client(name)
    state_directory = directory / "state"
    port = find_free_port()
    scenario = _Scenario(opts, port, pki)
    worker = threading.Thread(target=scenario.run, daemon=True)
    worker.start()

    def _teardown() -> None:
        scenario.stop.set()
        worker.join(timeout=10)
        try:
            if worker.is_alive():
                raise RuntimeError("wef fixture worker did not stop")
            if scenario.errors:
                raise AssertionError("; ".join(scenario.errors))
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    return FixtureHandle(
        env=env
        | {
            "WEF_ENDPOINT": f"{_HOST}:{port}",
            "WEF_CERTFILE": str(pki.server_cert),
            "WEF_KEYFILE": str(pki.server_key),
            "WEF_CAFILE": str(pki.ca_cert),
            "TENZIR_STATE_DIRECTORY": str(state_directory),
        },
        teardown=_teardown,
    )
