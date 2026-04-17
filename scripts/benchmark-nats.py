#!/usr/bin/env python3
"""Run local NATS JetStream throughput checks for Tenzir operators."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


NATS_IMAGE = "nats:2.12-alpine"
NATS_BOX_IMAGE = "natsio/nats-box:0.18.0"
MEBIBYTE = 1024 * 1024
MSG_RATE_PATTERN = re.compile(r"(?P<rate>\d[\d,]*(?:\.\d+)?)\s+msgs/sec")


@dataclass(frozen=True)
class Case:
    events: int
    payload_size: int


@dataclass(frozen=True)
class Measurement:
    case: Case
    run: int
    sender_seconds: float
    receiver_seconds: float

    @property
    def payload_bytes(self) -> int:
        return self.case.events * self.case.payload_size

    @property
    def sender_eps(self) -> float:
        return self.case.events / self.sender_seconds

    @property
    def receiver_eps(self) -> float:
        return self.case.events / self.receiver_seconds

    @property
    def sender_mib_s(self) -> float:
        return self.payload_bytes / self.sender_seconds / MEBIBYTE

    @property
    def receiver_mib_s(self) -> float:
        return self.payload_bytes / self.receiver_seconds / MEBIBYTE


@dataclass(frozen=True)
class NativeMeasurement:
    case: Case
    run: int
    sender_eps: float
    receiver_eps: float

    @property
    def payload_bytes(self) -> int:
        return self.case.events * self.case.payload_size

    @property
    def sender_mib_s(self) -> float:
        return self.payload_bytes * self.sender_eps / self.case.events / MEBIBYTE

    @property
    def receiver_mib_s(self) -> float:
        return self.payload_bytes * self.receiver_eps / self.case.events / MEBIBYTE


@dataclass(frozen=True)
class NativeBaseline:
    sender_eps: float
    receiver_eps: float

    def sender_percent(self, eps: float) -> float:
        return eps / self.sender_eps * 100.0

    def receiver_percent(self, eps: float) -> float:
        return eps / self.receiver_eps * 100.0


def run(
    cmd: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(cmd),
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip() or "no output"
        raise RuntimeError(
            f"command failed with exit code {result.returncode}: "
            f"{' '.join(cmd)}\n{detail}"
        )
    return result


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_nats(port: int, *, timeout_seconds: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0) as sock:
                sock.recv(16)
                return
        except OSError as exc:
            last_error = exc
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for NATS startup: {last_error}")


def start_nats(port: int) -> str:
    name = f"tenzir-bench-nats-{uuid.uuid4().hex[:8]}"
    result = run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            f"{port}:4222",
            NATS_IMAGE,
            "-js",
            "-sd",
            "/tmp/nats/jetstream",
            "-p",
            "4222",
        ]
    )
    container_id = result.stdout.strip()
    if not container_id:
        raise RuntimeError("docker did not return a NATS container id")
    return container_id


def stop_container(container_id: str) -> None:
    run(["docker", "stop", container_id], check=False)


def run_nats_cli(
    args: Sequence[str],
    *,
    url: str,
    container_id: str,
) -> str:
    local_nats = shutil.which("nats")
    if local_nats is not None:
        result = run([local_nats, "--server", url, *args])
        return f"{result.stdout}\n{result.stderr}"
    result = run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            f"container:{container_id}",
            NATS_BOX_IMAGE,
            "nats",
            "--server",
            "nats://127.0.0.1:4222",
            *args,
        ]
    )
    return f"{result.stdout}\n{result.stderr}"


def create_stream(
    *,
    stream: str,
    subjects: str,
    url: str,
    container_id: str,
) -> None:
    run_nats_cli(
        [
            "stream",
            "add",
            stream,
            "--subjects",
            subjects,
            "--storage",
            "file",
            "--retention",
            "limits",
            "--discard",
            "old",
            "--defaults",
        ],
        url=url,
        container_id=container_id,
    )


def write_dataset(path: Path, case: Case) -> int:
    payload = "x" * case.payload_size
    row = json.dumps({"message": payload}, separators=(",", ":")).encode()
    line = row + b"\n"
    chunk_size = 4096
    with path.open("wb") as output:
        whole_chunks, remainder = divmod(case.events, chunk_size)
        chunk = line * chunk_size
        for _ in range(whole_chunks):
            output.write(chunk)
        if remainder:
            output.write(line * remainder)
    return path.stat().st_size


def write_sender_tql(
    *,
    path: Path,
    input_path: Path,
    subject: str,
    url: str,
    max_pending: int,
) -> None:
    path.write_text(
        "\n".join(
            [
                f"from_file {json.dumps(str(input_path))} {{",
                "  read_ndjson",
                "}",
                (
                    f"to_nats {json.dumps(subject)}, message=message, "
                    f"url={json.dumps(url)}, _max_pending={max_pending}"
                ),
                "",
            ]
        ),
        encoding="utf-8",
    )


def write_receiver_tql(
    *,
    path: Path,
    subject: str,
    durable: str,
    url: str,
    events: int,
    batch_size: int,
    queue_capacity: int,
) -> None:
    path.write_text(
        "\n".join(
            [
                (
                    f"from_nats {json.dumps(subject)}, url={json.dumps(url)}, "
                    f"durable={json.dumps(durable)}, count={events}, "
                    f"_batch_size={batch_size}, _queue_capacity={queue_capacity} {{"
                ),
                "  read_lines",
                "}",
                "discard",
                "",
            ]
        ),
        encoding="utf-8",
    )


def run_tenzir(tenzir: Path, tql: Path) -> float:
    env = os.environ.copy()
    env.setdefault("TENZIR_CONSOLE_FORMAT", "none")
    started = time.perf_counter()
    run(
        [
            str(tenzir),
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
            "--neo",
            "-f",
            str(tql),
        ],
        env=env,
    )
    return time.perf_counter() - started


def extract_msg_rate(output: str, labels: Sequence[str]) -> float:
    lines = output.splitlines()
    for line in lines:
        normalized = line.lower()
        if any(label in normalized for label in labels):
            if match := MSG_RATE_PATTERN.search(line):
                return float(match.group("rate").replace(",", ""))
    for line in lines:
        if match := MSG_RATE_PATTERN.search(line):
            return float(match.group("rate").replace(",", ""))
    raise RuntimeError(f"failed to parse native NATS benchmark output:\n{output}")


def run_native_once(
    *,
    case: Case,
    run_index: int,
    url: str,
    container_id: str,
    native_batch: int,
    subject_prefix: str,
) -> NativeMeasurement:
    subject = f"{subject_prefix}.native.{run_index}"
    pub_output = run_nats_cli(
        [
            "bench",
            "js",
            "pub",
            subject,
            "--create",
            "--purge",
            "--msgs",
            str(case.events),
            "--size",
            str(case.payload_size),
            "--batch",
            str(native_batch),
            "--no-progress",
        ],
        url=url,
        container_id=container_id,
    )
    fetch_output = run_nats_cli(
        [
            "bench",
            "js",
            "fetch",
            "--acks",
            "all",
            "--msgs",
            str(case.events),
            "--size",
            str(case.payload_size),
            "--batch",
            str(native_batch),
            "--clients",
            "1",
            "--no-progress",
        ],
        url=url,
        container_id=container_id,
    )
    return NativeMeasurement(
        case=case,
        run=run_index,
        sender_eps=extract_msg_rate(pub_output, ("pub stats", "publisher stats")),
        receiver_eps=extract_msg_rate(
            fetch_output,
            ("sub stats", "aggregated stats", "consumer"),
        ),
    )


def benchmark_native(
    *,
    case: Case,
    case_index: int,
    args: argparse.Namespace,
    url: str,
    container_id: str,
) -> NativeBaseline | None:
    if args.no_native:
        return None
    measurements = [
        run_native_once(
            case=case,
            run_index=run_index,
            url=url,
            container_id=container_id,
            native_batch=args.native_batch,
            subject_prefix=f"{args.native_subject_prefix}.case{case_index}",
        )
        for run_index in range(1, args.native_runs + 1)
    ]
    for measurement in measurements:
        print(
            "  "
            f"native_run={measurement.run} "
            f"send_eps={format_rate(measurement.sender_eps)} "
            f"send_mib_s={measurement.sender_mib_s:.1f} "
            f"recv_eps={format_rate(measurement.receiver_eps)} "
            f"recv_mib_s={measurement.receiver_mib_s:.1f}",
            flush=True,
        )
    baseline = NativeBaseline(
        sender_eps=statistics.median(item.sender_eps for item in measurements),
        receiver_eps=statistics.median(item.receiver_eps for item in measurements),
    )
    print(
        "  "
        f"native_summary send_eps={format_rate(baseline.sender_eps)} "
        f"send_mib_s={case.payload_size * baseline.sender_eps / MEBIBYTE:.1f} "
        f"recv_eps={format_rate(baseline.receiver_eps)} "
        f"recv_mib_s={case.payload_size * baseline.receiver_eps / MEBIBYTE:.1f}",
        flush=True,
    )
    return baseline


def parse_case(raw: str) -> Case:
    try:
        events_raw, payload_raw = raw.split(":", maxsplit=1)
        events = int(events_raw)
        payload_size = int(payload_raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "cases must use the EVENTS:PAYLOAD_BYTES format"
        ) from exc
    if events <= 0:
        raise argparse.ArgumentTypeError("case events must be positive")
    if payload_size <= 0:
        raise argparse.ArgumentTypeError("case payload size must be positive")
    return Case(events=events, payload_size=payload_size)


def format_rate(value: float) -> str:
    return f"{value:,.0f}"


def print_measurement(
    measurement: Measurement,
    *,
    native: NativeBaseline | None,
) -> None:
    native_suffix = ""
    if native is not None:
        native_suffix = (
            f" send_native={native.sender_percent(measurement.sender_eps):.1f}%"
            f" recv_native={native.receiver_percent(measurement.receiver_eps):.1f}%"
        )
    print(
        "  "
        f"run={measurement.run} "
        f"send={measurement.sender_seconds:.3f}s "
        f"send_eps={format_rate(measurement.sender_eps)} "
        f"send_mib_s={measurement.sender_mib_s:.1f} "
        f"recv={measurement.receiver_seconds:.3f}s "
        f"recv_eps={format_rate(measurement.receiver_eps)} "
        f"recv_mib_s={measurement.receiver_mib_s:.1f}"
        f"{native_suffix}",
        flush=True,
    )


def print_summary(
    case: Case,
    measurements: list[Measurement],
    *,
    native: NativeBaseline | None,
) -> None:
    sender_eps = statistics.median(item.sender_eps for item in measurements)
    sender_mib_s = statistics.median(item.sender_mib_s for item in measurements)
    receiver_eps = statistics.median(item.receiver_eps for item in measurements)
    receiver_mib_s = statistics.median(item.receiver_mib_s for item in measurements)
    native_suffix = ""
    if native is not None:
        native_suffix = (
            f" send_native={native.sender_percent(sender_eps):.1f}%"
            f" recv_native={native.receiver_percent(receiver_eps):.1f}%"
        )
    print(
        "summary "
        f"events={case.events:,} "
        f"payload={case.payload_size}B "
        f"send_eps={format_rate(sender_eps)} "
        f"send_mib_s={sender_mib_s:.1f} "
        f"recv_eps={format_rate(receiver_eps)} "
        f"recv_mib_s={receiver_mib_s:.1f}"
        f"{native_suffix}",
        flush=True,
    )


def benchmark_case(
    *,
    case: Case,
    case_index: int,
    args: argparse.Namespace,
    url: str,
    container_id: str,
    workdir: Path,
) -> list[Measurement]:
    input_path = workdir / f"input-{case.events}-{case.payload_size}.ndjson"
    input_bytes = write_dataset(input_path, case)
    payload_mib = case.events * case.payload_size / MEBIBYTE
    input_mib = input_bytes / MEBIBYTE
    print(
        f"case events={case.events:,} payload={case.payload_size}B "
        f"payload={payload_mib:.1f} MiB input={input_mib:.1f} MiB",
        flush=True,
    )
    native = benchmark_native(
        case=case,
        case_index=case_index,
        args=args,
        url=url,
        container_id=container_id,
    )

    measurements: list[Measurement] = []
    total_runs = args.warmups + args.runs
    for logical_run in range(total_runs):
        measured = logical_run >= args.warmups
        run_number = logical_run - args.warmups + 1
        subject = f"{args.subject_prefix}.case{case_index}.run{logical_run}"
        durable = f"tenzir_bench_{uuid.uuid4().hex}"
        sender_tql = workdir / f"send-{case_index}-{logical_run}.tql"
        receiver_tql = workdir / f"recv-{case_index}-{logical_run}.tql"
        write_sender_tql(
            path=sender_tql,
            input_path=input_path,
            subject=subject,
            url=url,
            max_pending=args.max_pending,
        )
        write_receiver_tql(
            path=receiver_tql,
            subject=subject,
            durable=durable,
            url=url,
            events=case.events,
            batch_size=args.batch_size,
            queue_capacity=args.queue_capacity,
        )
        sender_seconds = run_tenzir(args.tenzir, sender_tql)
        receiver_seconds = run_tenzir(args.tenzir, receiver_tql)
        measurement = Measurement(
            case=case,
            run=run_number,
            sender_seconds=sender_seconds,
            receiver_seconds=receiver_seconds,
        )
        if measured:
            measurements.append(measurement)
            print_measurement(measurement, native=native)
        else:
            print(
                f"  warmup send={sender_seconds:.3f}s recv={receiver_seconds:.3f}s",
                flush=True,
            )
    print_summary(case, measurements, native=native)
    return measurements


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tenzir",
        type=Path,
        default=Path("build/xcode/release/bin/tenzir"),
        help="path to the tenzir binary",
    )
    parser.add_argument(
        "--case",
        dest="cases",
        type=parse_case,
        action="append",
        default=[],
        help="benchmark case as EVENTS:PAYLOAD_BYTES; may be repeated",
    )
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--queue-capacity", type=int, default=8192)
    parser.add_argument("--max-pending", type=int, default=8192)
    parser.add_argument(
        "--native-runs",
        type=int,
        default=1,
        help="number of native NATS benchmark runs per case",
    )
    parser.add_argument(
        "--native-batch",
        type=int,
        default=500,
        help="batch size for native nats bench JetStream commands",
    )
    parser.add_argument(
        "--no-native",
        action="store_true",
        help="skip native nats bench gold-standard runs",
    )
    parser.add_argument("--subject-prefix", default=f"tenzir.bench.{uuid.uuid4().hex}")
    parser.add_argument(
        "--native-subject-prefix",
        default=f"native.bench.{uuid.uuid4().hex}",
        help="subject prefix for native nats bench runs",
    )
    args = parser.parse_args(argv)
    if not args.cases:
        args.cases = [Case(events=100_000, payload_size=256)]
    if args.runs <= 0:
        parser.error("--runs must be positive")
    if args.warmups < 0:
        parser.error("--warmups must be non-negative")
    if args.native_runs <= 0:
        parser.error("--native-runs must be positive")
    if args.native_batch <= 0:
        parser.error("--native-batch must be positive")
    if not args.tenzir.exists():
        parser.error(f"tenzir binary does not exist: {args.tenzir}")
    args.tenzir = args.tenzir.resolve()
    return args


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    if shutil.which("docker") is None:
        raise RuntimeError("docker is required")

    port = find_free_port()
    url = f"nats://127.0.0.1:{port}"
    stream = f"TENZIR_BENCH_{uuid.uuid4().hex[:8].upper()}"
    subjects = f"{args.subject_prefix}.>"
    container_id = start_nats(port)
    print(f"nats={url} stream={stream} subjects={subjects}", flush=True)
    try:
        wait_for_nats(port)
        create_stream(
            stream=stream,
            subjects=subjects,
            url=url,
            container_id=container_id,
        )
        with tempfile.TemporaryDirectory(prefix="tenzir-nats-bench-") as raw_dir:
            workdir = Path(raw_dir)
            all_measurements: list[Measurement] = []
            for index, case in enumerate(args.cases, start=1):
                all_measurements.extend(
                    benchmark_case(
                        case=case,
                        case_index=index,
                        args=args,
                        url=url,
                        container_id=container_id,
                        workdir=workdir,
                    )
                )
        print(f"completed {len(all_measurements)} measured run(s)", flush=True)
        return 0
    finally:
        stop_container(container_id)


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        raise SystemExit(130) from None
