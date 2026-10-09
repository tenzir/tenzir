//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Generated from the Effect API definitions in `platform/services/shared/api`.
// Do not edit. Run `bun tools/codegen/main.ts` in the platform checkout.

#pragma once

// Hand-written. The generator knows nothing about how bytes travel, and
// this is all a caller of the API needs.
#include <tenzir/async/task.hpp>
#include <tenzir/controller/transport.hpp>
#include <tenzir/detail/default_formatter.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>
#include <tenzir/time.hpp>
#include <tenzir/unit.hpp>
#include <tenzir/variant.hpp>

#include <cstdint>
#include <functional>
#include <simdjson.h>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace tenzir {

// ── Types ───────────────────────────────────────────────────────────────────

/// A path-safe identifier
/// Values must match `^[A-Za-z0-9][A-Za-z0-9_-]*$`.
class DeploymentId {
public:
  static auto make(std::string value) -> Result<DeploymentId, ParseError>;

  auto to_string() const -> std::string_view {
    return value_;
  }

  friend auto operator==(DeploymentId const&, DeploymentId const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, DeploymentId const& value) -> void {
    hash_append(h, value.to_string());
  }

private:
  explicit DeploymentId(std::string value) : value_{std::move(value)} {
  }

  std::string value_;
};

template <>
inline constexpr auto enable_default_formatter<DeploymentId> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::DeploymentId> {
  auto operator()(tenzir::DeploymentId const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class DeploymentPath {
public:
  explicit DeploymentPath(DeploymentId deployment_id)
    : deployment_id{std::move(deployment_id)} {
  }

  DeploymentId deployment_id;

  friend auto operator==(DeploymentPath const&, DeploymentPath const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, DeploymentPath const& value)
    -> void {
    hash_append(h, value.deployment_id);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::DeploymentPath> {
  auto operator()(tenzir::DeploymentPath const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// A UUID
/// Values must match
/// `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`.
class WorkspaceId {
public:
  static auto make(std::string value) -> Result<WorkspaceId, ParseError>;

  auto to_string() const -> std::string_view {
    return value_;
  }

  friend auto operator==(WorkspaceId const&, WorkspaceId const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, WorkspaceId const& value) -> void {
    hash_append(h, value.to_string());
  }

private:
  explicit WorkspaceId(std::string value) : value_{std::move(value)} {
  }

  std::string value_;
};

template <>
inline constexpr auto enable_default_formatter<WorkspaceId> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::WorkspaceId> {
  auto operator()(tenzir::WorkspaceId const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class DeploymentInfo {
public:
  DeploymentInfo(DeploymentId id, Option<std::string> key_digest,
                 WorkspaceId workspace_id, std::string name,
                 Option<std::string> version, Option<time> last_seen)
    : id{std::move(id)},
      key_digest{std::move(key_digest)},
      workspace_id{std::move(workspace_id)},
      name{std::move(name)},
      version{std::move(version)},
      last_seen{std::move(last_seen)} {
  }

  DeploymentId id;
  /// `keyDigest`, may be absent, may be null.
  Option<std::string> key_digest;
  WorkspaceId workspace_id;
  std::string name;
  /// `version`, may be absent, may be null.
  Option<std::string> version;
  /// `lastSeen`, may be absent, may be null.
  Option<time> last_seen;

  friend auto operator==(DeploymentInfo const&, DeploymentInfo const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, DeploymentInfo const& value)
    -> void {
    hash_append(h, value.id, value.key_digest, value.workspace_id, value.name,
                value.version, value.last_seen);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::DeploymentInfo> {
  auto operator()(tenzir::DeploymentInfo const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// Counts batches across all concurrent pipeline senders per boot. A retry
/// repeats the identical batch and (boot, seq)
class TelemetryCount {
public:
  static auto make(std::int64_t value) -> Result<TelemetryCount, ParseError>;

  auto value() const -> std::int64_t {
    return value_;
  }

  friend auto operator==(TelemetryCount const&, TelemetryCount const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, TelemetryCount const& value)
    -> void {
    hash_append(h, value.value());
  }

private:
  explicit TelemetryCount(std::int64_t value) : value_{std::move(value)} {
  }

  std::int64_t value_;
};

template <>
inline constexpr auto enable_default_formatter<TelemetryCount> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::TelemetryCount> {
  auto operator()(tenzir::TelemetryCount const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// Telemetry batches or bounded diagnostic/transition entries lost since the
/// previous acknowledged report
class TenzirTelemetryTelemetryCount {
public:
  static auto make(std::int64_t value)
    -> Result<TenzirTelemetryTelemetryCount, ParseError>;

  auto value() const -> std::int64_t {
    return value_;
  }

  friend auto operator==(TenzirTelemetryTelemetryCount const&,
                         TenzirTelemetryTelemetryCount const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto
  hash_append(HashAlgorithm& h, TenzirTelemetryTelemetryCount const& value)
    -> void {
    hash_append(h, value.value());
  }

private:
  explicit TenzirTelemetryTelemetryCount(std::int64_t value)
    : value_{std::move(value)} {
  }

  std::int64_t value_;
};

template <>
inline constexpr auto enable_default_formatter<TenzirTelemetryTelemetryCount>
  = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::TenzirTelemetryTelemetryCount> {
  auto operator()(tenzir::TenzirTelemetryTelemetryCount const& value) const
    -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// A path-safe identifier
/// Values must match `^[A-Za-z0-9][A-Za-z0-9_-]*$`.
class PipelineId {
public:
  static auto make(std::string value) -> Result<PipelineId, ParseError>;

  auto to_string() const -> std::string_view {
    return value_;
  }

  friend auto operator==(PipelineId const&, PipelineId const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, PipelineId const& value) -> void {
    hash_append(h, value.to_string());
  }

private:
  explicit PipelineId(std::string value) : value_{std::move(value)} {
  }

  std::string value_;
};

template <>
inline constexpr auto enable_default_formatter<PipelineId> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::PipelineId> {
  auto operator()(tenzir::PipelineId const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class TelemetryCount2 {
public:
  static auto make(std::int64_t value) -> Result<TelemetryCount2, ParseError>;

  auto value() const -> std::int64_t {
    return value_;
  }

  friend auto operator==(TelemetryCount2 const&, TelemetryCount2 const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, TelemetryCount2 const& value)
    -> void {
    hash_append(h, value.value());
  }

private:
  explicit TelemetryCount2(std::int64_t value) : value_{std::move(value)} {
  }

  std::int64_t value_;
};

template <>
inline constexpr auto enable_default_formatter<TelemetryCount2> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::TelemetryCount2> {
  auto operator()(tenzir::TelemetryCount2 const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// What the sources or the sinks of one connector type moved. Two of the same
/// type are summed
class ConnectorFlow {
public:
  ConnectorFlow(std::string connector, TelemetryCount2 events,
                TelemetryCount2 bytes)
    : connector{std::move(connector)},
      events{std::move(events)},
      bytes{std::move(bytes)} {
  }

  std::string connector;
  TelemetryCount2 events;
  TelemetryCount2 bytes;

  friend auto operator==(ConnectorFlow const&, ConnectorFlow const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, ConnectorFlow const& value)
    -> void {
    hash_append(h, value.connector, value.events, value.bytes);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::ConnectorFlow> {
  auto operator()(tenzir::ConnectorFlow const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class PipelineSample {
public:
  PipelineSample(PipelineId pipeline_id, TelemetryCount2 run, time window,
                 std::vector<ConnectorFlow> sources,
                 std::vector<ConnectorFlow> sinks)
    : pipeline_id{std::move(pipeline_id)},
      run{std::move(run)},
      window{window},
      sources{std::move(sources)},
      sinks{std::move(sinks)} {
  }

  PipelineId pipeline_id;
  TelemetryCount2 run;
  time window;
  std::vector<ConnectorFlow> sources;
  std::vector<ConnectorFlow> sinks;

  friend auto operator==(PipelineSample const&, PipelineSample const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, PipelineSample const& value)
    -> void {
    hash_append(h, value.pipeline_id, value.run, value.window, value.sources,
                value.sinks);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::PipelineSample> {
  auto operator()(tenzir::PipelineSample const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// What passed one point of a pipeline in a window. Counters cover that window
/// alone and are never running totals
class Flow {
public:
  Flow(TelemetryCount2 events, TelemetryCount2 bytes)
    : events{std::move(events)}, bytes{std::move(bytes)} {
  }

  TelemetryCount2 events;
  TelemetryCount2 bytes;

  friend auto operator==(Flow const&, Flow const&) -> bool = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, Flow const& value) -> void {
    hash_append(h, value.events, value.bytes);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::Flow> {
  auto operator()(tenzir::Flow const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// Summed over the window
class TelemetrySeconds {
public:
  static auto make(double value) -> Result<TelemetrySeconds, ParseError>;

  auto value() const -> double {
    return value_;
  }

  friend auto operator==(TelemetrySeconds const&, TelemetrySeconds const&)
    -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, TelemetrySeconds const& value)
    -> void {
    hash_append(h, value.value());
  }

private:
  explicit TelemetrySeconds(double value) : value_{std::move(value)} {
  }

  double value_;
};

template <>
inline constexpr auto enable_default_formatter<TelemetrySeconds> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::TelemetrySeconds> {
  auto operator()(tenzir::TelemetrySeconds const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// The largest seen in the window
class TelemetryCount3 {
public:
  static auto make(std::int64_t value) -> Result<TelemetryCount3, ParseError>;

  auto value() const -> std::int64_t {
    return value_;
  }

  friend auto operator==(TelemetryCount3 const&, TelemetryCount3 const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, TelemetryCount3 const& value)
    -> void {
    hash_append(h, value.value());
  }

private:
  explicit TelemetryCount3(std::int64_t value) : value_{std::move(value)} {
  }

  std::int64_t value_;
};

template <>
inline constexpr auto enable_default_formatter<TelemetryCount3> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::TelemetryCount3> {
  auto operator()(tenzir::TelemetryCount3 const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class TelemetrySmallCount {
public:
  static auto make(std::int64_t value)
    -> Result<TelemetrySmallCount, ParseError>;

  auto value() const -> std::int64_t {
    return value_;
  }

  friend auto operator==(TelemetrySmallCount const&, TelemetrySmallCount const&)
    -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, TelemetrySmallCount const& value)
    -> void {
    hash_append(h, value.value());
  }

private:
  explicit TelemetrySmallCount(std::int64_t value) : value_{std::move(value)} {
  }

  std::int64_t value_;
};

template <>
inline constexpr auto enable_default_formatter<TelemetrySmallCount> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::TelemetrySmallCount> {
  auto operator()(tenzir::TelemetrySmallCount const& value) const
    -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class OperatorSample {
public:
  OperatorSample(PipelineId pipeline_id, TelemetryCount2 run,
                 std::string operator_id, std::string name, time window,
                 Flow input, Flow output, TelemetrySeconds cpu_seconds,
                 TelemetryCount3 input_bytes, TelemetryCount2 input_capacity,
                 TelemetrySmallCount tasks)
    : pipeline_id{std::move(pipeline_id)},
      run{std::move(run)},
      operator_id{std::move(operator_id)},
      name{std::move(name)},
      window{window},
      input{std::move(input)},
      output{std::move(output)},
      cpu_seconds{std::move(cpu_seconds)},
      input_bytes{std::move(input_bytes)},
      input_capacity{std::move(input_capacity)},
      tasks{std::move(tasks)} {
  }

  PipelineId pipeline_id;
  TelemetryCount2 run;
  std::string operator_id;
  std::string name;
  time window;
  Flow input;
  Flow output;
  TelemetrySeconds cpu_seconds;
  TelemetryCount3 input_bytes;
  TelemetryCount2 input_capacity;
  TelemetrySmallCount tasks;

  friend auto operator==(OperatorSample const&, OperatorSample const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, OperatorSample const& value)
    -> void {
    hash_append(h, value.pipeline_id, value.run, value.operator_id, value.name,
                value.window, value.input, value.output, value.cpu_seconds,
                value.input_bytes, value.input_capacity, value.tasks);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::OperatorSample> {
  auto operator()(tenzir::OperatorSample const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// How many the deployment folded into this sample
class TenzirTelemetryTelemetrySmallCount {
public:
  static auto make(std::int64_t value)
    -> Result<TenzirTelemetryTelemetrySmallCount, ParseError>;

  auto value() const -> std::int64_t {
    return value_;
  }

  friend auto operator==(TenzirTelemetryTelemetrySmallCount const&,
                         TenzirTelemetryTelemetrySmallCount const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto
  hash_append(HashAlgorithm& h, TenzirTelemetryTelemetrySmallCount const& value)
    -> void {
    hash_append(h, value.value());
  }

private:
  explicit TenzirTelemetryTelemetrySmallCount(std::int64_t value)
    : value_{std::move(value)} {
  }

  std::int64_t value_;
};

template <>
inline constexpr auto
  enable_default_formatter<TenzirTelemetryTelemetrySmallCount>
  = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::TenzirTelemetryTelemetrySmallCount> {
  auto operator()(tenzir::TenzirTelemetryTelemetrySmallCount const& value) const
    -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

enum class Severity {
  note,
  warning,
  error,
};

auto to_string(Severity value) -> std::string_view;

class PushTelemetryPayloadDiagnosticsNotes {
public:
  PushTelemetryPayloadDiagnosticsNotes(std::string kind, std::string message)
    : kind{std::move(kind)}, message{std::move(message)} {
  }

  std::string kind;
  std::string message;

  friend auto operator==(PushTelemetryPayloadDiagnosticsNotes const&,
                         PushTelemetryPayloadDiagnosticsNotes const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h,
                          PushTelemetryPayloadDiagnosticsNotes const& value)
    -> void {
    hash_append(h, value.kind, value.message);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::PushTelemetryPayloadDiagnosticsNotes> {
  auto
  operator()(tenzir::PushTelemetryPayloadDiagnosticsNotes const& value) const
    -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class PushTelemetryPayloadDiagnosticsAnnotations {
public:
  PushTelemetryPayloadDiagnosticsAnnotations(bool primary, std::string text,
                                             TelemetryCount2 begin,
                                             TelemetryCount2 end)
    : primary{primary},
      text{std::move(text)},
      begin{std::move(begin)},
      end{std::move(end)} {
  }

  bool primary;
  std::string text;
  TelemetryCount2 begin;
  TelemetryCount2 end;

  friend auto operator==(PushTelemetryPayloadDiagnosticsAnnotations const&,
                         PushTelemetryPayloadDiagnosticsAnnotations const&)
    -> bool
    = default;

  template <class HashAlgorithm>
  friend auto
  hash_append(HashAlgorithm& h,
              PushTelemetryPayloadDiagnosticsAnnotations const& value) -> void {
    hash_append(h, value.primary, value.text, value.begin, value.end);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::PushTelemetryPayloadDiagnosticsAnnotations> {
  auto operator()(
    tenzir::PushTelemetryPayloadDiagnosticsAnnotations const& value) const
    -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class DiagnosticSample {
public:
  DiagnosticSample(
    PipelineId pipeline_id, TelemetryCount2 run, time first_seen,
    time last_seen, TenzirTelemetryTelemetrySmallCount occurrences,
    std::string fingerprint, Severity severity, std::string message,
    std::string rendered,
    std::vector<PushTelemetryPayloadDiagnosticsNotes> notes,
    std::vector<PushTelemetryPayloadDiagnosticsAnnotations> annotations)
    : pipeline_id{std::move(pipeline_id)},
      run{std::move(run)},
      first_seen{first_seen},
      last_seen{last_seen},
      occurrences{std::move(occurrences)},
      fingerprint{std::move(fingerprint)},
      severity{std::move(severity)},
      message{std::move(message)},
      rendered{std::move(rendered)},
      notes{std::move(notes)},
      annotations{std::move(annotations)} {
  }

  PipelineId pipeline_id;
  TelemetryCount2 run;
  time first_seen;
  time last_seen;
  TenzirTelemetryTelemetrySmallCount occurrences;
  std::string fingerprint;
  Severity severity;
  std::string message;
  std::string rendered;
  std::vector<PushTelemetryPayloadDiagnosticsNotes> notes;
  std::vector<PushTelemetryPayloadDiagnosticsAnnotations> annotations;

  friend auto operator==(DiagnosticSample const&, DiagnosticSample const&)
    -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, DiagnosticSample const& value)
    -> void {
    hash_append(h, value.pipeline_id, value.run, value.first_seen,
                value.last_seen, value.occurrences, value.fingerprint,
                value.severity, value.message, value.rendered, value.notes,
                value.annotations);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::DiagnosticSample> {
  auto operator()(tenzir::DiagnosticSample const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

enum class RunState {
  running,
  finished,
  failed,
  stopped,
};

auto to_string(RunState value) -> std::string_view;

class RunTransition {
public:
  RunTransition(PipelineId pipeline_id, TelemetryCount2 run, time at,
                RunState state, Option<std::string> error)
    : pipeline_id{std::move(pipeline_id)},
      run{std::move(run)},
      at{at},
      state{std::move(state)},
      error{std::move(error)} {
  }

  PipelineId pipeline_id;
  TelemetryCount2 run;
  time at;
  RunState state;
  /// `error`, may be null.
  Option<std::string> error;

  friend auto operator==(RunTransition const&, RunTransition const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, RunTransition const& value)
    -> void {
    hash_append(h, value.pipeline_id, value.run, value.at, value.state,
                value.error);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::RunTransition> {
  auto operator()(tenzir::RunTransition const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class TenzirTelemetryTelemetrySeconds {
public:
  static auto make(double value)
    -> Result<TenzirTelemetryTelemetrySeconds, ParseError>;

  auto value() const -> double {
    return value_;
  }

  friend auto operator==(TenzirTelemetryTelemetrySeconds const&,
                         TenzirTelemetryTelemetrySeconds const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto
  hash_append(HashAlgorithm& h, TenzirTelemetryTelemetrySeconds const& value)
    -> void {
    hash_append(h, value.value());
  }

private:
  explicit TenzirTelemetryTelemetrySeconds(double value)
    : value_{std::move(value)} {
  }

  double value_;
};

template <>
inline constexpr auto enable_default_formatter<TenzirTelemetryTelemetrySeconds>
  = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::TenzirTelemetryTelemetrySeconds> {
  auto operator()(tenzir::TenzirTelemetryTelemetrySeconds const& value) const
    -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class DeploymentSample {
public:
  DeploymentSample(time window, TenzirTelemetryTelemetrySeconds cpu_seconds,
                   TelemetryCount2 memory_bytes, TelemetryCount2 uptime_ms)
    : window{window},
      cpu_seconds{std::move(cpu_seconds)},
      memory_bytes{std::move(memory_bytes)},
      uptime_ms{std::move(uptime_ms)} {
  }

  time window;
  TenzirTelemetryTelemetrySeconds cpu_seconds;
  TelemetryCount2 memory_bytes;
  TelemetryCount2 uptime_ms;

  friend auto operator==(DeploymentSample const&, DeploymentSample const&)
    -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, DeploymentSample const& value)
    -> void {
    hash_append(h, value.window, value.cpu_seconds, value.memory_bytes,
                value.uptime_ms);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::DeploymentSample> {
  auto operator()(tenzir::DeploymentSample const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// Everything a deployment reports in one second. Pipeline samples come when
/// traffic changes; operator samples are opt-in and initially absent
class Batch {
public:
  Batch(std::string boot, TelemetryCount seq,
        TenzirTelemetryTelemetryCount dropped,
        std::vector<PipelineSample> pipelines,
        std::vector<OperatorSample> operators,
        std::vector<DiagnosticSample> diagnostics,
        std::vector<RunTransition> runs,
        std::vector<DeploymentSample> deployment)
    : boot{std::move(boot)},
      seq{std::move(seq)},
      dropped{std::move(dropped)},
      pipelines{std::move(pipelines)},
      operators{std::move(operators)},
      diagnostics{std::move(diagnostics)},
      runs{std::move(runs)},
      deployment{std::move(deployment)} {
  }

  std::string boot;
  TelemetryCount seq;
  TenzirTelemetryTelemetryCount dropped;
  std::vector<PipelineSample> pipelines;
  std::vector<OperatorSample> operators;
  std::vector<DiagnosticSample> diagnostics;
  std::vector<RunTransition> runs;
  std::vector<DeploymentSample> deployment;

  friend auto operator==(Batch const&, Batch const&) -> bool = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, Batch const& value) -> void {
    hash_append(h, value.boot, value.seq, value.dropped, value.pipelines,
                value.operators, value.diagnostics, value.runs,
                value.deployment);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::Batch> {
  auto operator()(tenzir::Batch const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class Liveness {
public:
  Liveness(std::string status, double uptime_ms)
    : status{std::move(status)}, uptime_ms{uptime_ms} {
  }

  std::string status;
  double uptime_ms;

  friend auto operator==(Liveness const&, Liveness const&) -> bool = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, Liveness const& value) -> void {
    hash_append(h, value.status, value.uptime_ms);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::Liveness> {
  auto operator()(tenzir::Liveness const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// A pipeline as the platform wants it to exist, named for people and defined
/// in TQL
class Pipeline {
public:
  Pipeline(std::string name, std::string definition)
    : name{std::move(name)}, definition{std::move(definition)} {
  }

  std::string name;
  std::string definition;

  friend auto operator==(Pipeline const&, Pipeline const&) -> bool = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, Pipeline const& value) -> void {
    hash_append(h, value.name, value.definition);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::Pipeline> {
  auto operator()(tenzir::Pipeline const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// The pipelines a deployment should be running
class Configuration {
public:
  explicit Configuration(std::unordered_map<PipelineId, Pipeline> pipelines)
    : pipelines{std::move(pipelines)} {
  }

  std::unordered_map<PipelineId, Pipeline> pipelines;

  friend auto operator==(Configuration const&, Configuration const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, Configuration const& value)
    -> void {
    hash_append(h, value.pipelines);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::Configuration> {
  auto operator()(tenzir::Configuration const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class IdentifiedPipeline {
public:
  IdentifiedPipeline(PipelineId id, std::string name, std::string definition)
    : id{std::move(id)},
      name{std::move(name)},
      definition{std::move(definition)} {
  }

  PipelineId id;
  std::string name;
  std::string definition;

  friend auto operator==(IdentifiedPipeline const&, IdentifiedPipeline const&)
    -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, IdentifiedPipeline const& value)
    -> void {
    hash_append(h, value.id, value.name, value.definition);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::IdentifiedPipeline> {
  auto operator()(tenzir::IdentifiedPipeline const& value) const
    -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

enum class PipelineState {
  running,
  finished,
  failed,
  stopped,
};

auto to_string(PipelineState value) -> std::string_view;

class PipelineStatus {
public:
  PipelineStatus(PipelineId id, std::string name, std::string definition,
                 PipelineState state, std::int64_t run,
                 Option<std::string> error)
    : id{std::move(id)},
      name{std::move(name)},
      definition{std::move(definition)},
      state{std::move(state)},
      run{std::move(run)},
      error{std::move(error)} {
  }

  PipelineId id;
  std::string name;
  std::string definition;
  PipelineState state;
  std::int64_t run;
  /// `error`, may be null.
  Option<std::string> error;

  friend auto operator==(PipelineStatus const&, PipelineStatus const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, PipelineStatus const& value)
    -> void {
    hash_append(h, value.id, value.name, value.definition, value.state,
                value.run, value.error);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::PipelineStatus> {
  auto operator()(tenzir::PipelineStatus const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class PipelinePath {
public:
  explicit PipelinePath(PipelineId pipeline_id)
    : pipeline_id{std::move(pipeline_id)} {
  }

  PipelineId pipeline_id;

  friend auto operator==(PipelinePath const&, PipelinePath const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, PipelinePath const& value) -> void {
    hash_append(h, value.pipeline_id);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::PipelinePath> {
  auto operator()(tenzir::PipelinePath const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

/// The id the platform gave an ad-hoc pipeline when it started it
/// Values must match
/// `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`.
class AdhocPipelineId {
public:
  static auto make(std::string value) -> Result<AdhocPipelineId, ParseError>;

  auto to_string() const -> std::string_view {
    return value_;
  }

  friend auto operator==(AdhocPipelineId const&, AdhocPipelineId const&) -> bool
    = default;

  template <class HashAlgorithm>
  friend auto hash_append(HashAlgorithm& h, AdhocPipelineId const& value)
    -> void {
    hash_append(h, value.to_string());
  }

private:
  explicit AdhocPipelineId(std::string value) : value_{std::move(value)} {
  }

  std::string value_;
};

template <>
inline constexpr auto enable_default_formatter<AdhocPipelineId> = true;

} // namespace tenzir

template <>
struct std::hash<tenzir::AdhocPipelineId> {
  auto operator()(tenzir::AdhocPipelineId const& value) const -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

class AdhocPipelineDefinition {
public:
  AdhocPipelineDefinition(AdhocPipelineId adhoc_pipeline_id,
                          std::string definition)
    : adhoc_pipeline_id{std::move(adhoc_pipeline_id)},
      definition{std::move(definition)} {
  }

  AdhocPipelineId adhoc_pipeline_id;
  std::string definition;

  friend auto
  operator==(AdhocPipelineDefinition const&, AdhocPipelineDefinition const&)
    -> bool
    = default;

  template <class HashAlgorithm>
  friend auto
  hash_append(HashAlgorithm& h, AdhocPipelineDefinition const& value) -> void {
    hash_append(h, value.adhoc_pipeline_id, value.definition);
  }
};

} // namespace tenzir

template <>
struct std::hash<tenzir::AdhocPipelineDefinition> {
  auto operator()(tenzir::AdhocPipelineDefinition const& value) const
    -> std::size_t {
    return tenzir::hash(value);
  }
};

namespace tenzir {

// ── Failures ────────────────────────────────────────────────────────────────

/// InternalServerError
class InternalServerError {
public:
  InternalServerError() = default;

  static constexpr auto status = std::uint16_t{500};
};

/// NotFound
class NotFound {
public:
  NotFound() = default;

  static constexpr auto status = std::uint16_t{404};
};

class Unauthorized {
public:
  explicit Unauthorized(std::string message) : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{403};

  std::string message;
};

class Unauthenticated {
public:
  explicit Unauthenticated(std::string message) : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{401};

  std::string message;
};

class CredentialConflict {
public:
  explicit CredentialConflict(std::string message)
    : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{400};

  std::string message;
};

class Validation {
public:
  explicit Validation(std::string message) : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{400};

  std::string message;
};

class TooLarge {
public:
  explicit TooLarge(std::string message) : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{413};

  std::string message;
};

/// ServiceUnavailable
class ServiceUnavailable {
public:
  ServiceUnavailable() = default;

  static constexpr auto status = std::uint16_t{503};
};

class InvalidPipeline {
public:
  explicit InvalidPipeline(std::string message) : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{400};

  std::string message;
};

class PipelineExists {
public:
  explicit PipelineExists(std::string message) : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{409};

  std::string message;
};

class PipelineNotFound {
public:
  explicit PipelineNotFound(std::string message) : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{404};

  std::string message;
};

class Busy {
public:
  explicit Busy(std::string message) : message{std::move(message)} {
  }

  static constexpr auto status = std::uint16_t{429};

  std::string message;
};

/// Everything calling adds, whatever the endpoint declares.
///
/// The API describes itself as if it were a direct call. Calling it adds
/// three things no declaration mentions: the transport can fail, the far
/// side can answer something no schema covers, and a body can fail to fit
/// the schema its status chose.
///
/// The three that are not declared anywhere come from `transport.hpp`, which
/// is where the failures no endpoint declares live.
using CallError = variant<ParseError, RequestError, ResponseError>;

// ── Deployments (deployment → platform) ─────────────────────────────────────

/// `GET /api/deployments/:deploymentId`
/// Reads a single deployment by id
class ReadDeployment {
public:
  explicit ReadDeployment(DeploymentPath path) : path{std::move(path)} {
  }

  DeploymentPath path;
};

using ReadDeploymentError
  = variant<InternalServerError, NotFound, Unauthorized, Unauthenticated,
            CredentialConflict, Validation>;

auto call(Transport& transport, ReadDeployment request)
  -> Task<Result<DeploymentInfo, variant<ReadDeploymentError, CallError>>>;

// ── Telemetry (deployment → platform) ───────────────────────────────────────

/// `POST /api/deployments/:deploymentId/telemetry`
/// Stores one batch of pipeline metrics, operator metrics, diagnostics and run
/// transitions from the deployment that sends it, and passes it on to anyone
/// watching live. An error may follow partial table writes. Retry the identical
/// batch promptly with the same boot and seq: each table and rollup retains up
/// to 100,000 block IDs for deduplication. Live publication is best effort and
/// may repeat after a backend restart.
class PushTelemetry {
public:
  PushTelemetry(DeploymentPath path, Batch payload)
    : path{std::move(path)}, payload{std::move(payload)} {
  }

  DeploymentPath path;
  Batch payload;
};

using PushTelemetryError
  = variant<TooLarge, InternalServerError, NotFound, ServiceUnavailable,
            Unauthorized, Unauthenticated, CredentialConflict, Validation>;

auto call(Transport& transport, PushTelemetry request)
  -> Task<Result<void, variant<PushTelemetryError, CallError>>>;

// ── Deployment (platform → deployment) ──────────────────────────────────────

/// `GET /health`
/// Answered as long as the deployment is running. The backend probes this
/// through the gateway, which is where `reachable` comes from.
class ReadHealth {
public:
  ReadHealth() = default;
};

/// `PUT /configuration`
/// Hands the deployment the whole state it should be in. Sent on every change
/// and again after a deployment reconnects, so it has to be safe to apply
/// twice. The deployment makes its pipelines match: one that is missing is
/// created and started, one whose name or definition differs is replaced and
/// started again, one that is not in the configuration is stopped and removed.
/// Answers with nothing: what the deployment made of it shows up in
/// `listPipelines`, not in a reply the platform would have to interpret.
class UpdateConfiguration {
public:
  explicit UpdateConfiguration(Configuration payload)
    : payload{std::move(payload)} {
  }

  Configuration payload;
};

/// `POST /pipelines`
/// Hands the deployment a pipeline, whole, along with the id it is known under,
/// and starts it: the request needs nothing sent earlier to make sense. The
/// definition has to compile and to be a whole pipeline, with a source and a
/// sink, or it is refused. The id is the platform’s, and a taken id is a 409
/// whatever it holds: the deployment neither overwrites one pipeline with
/// another nor quietly accepts the same one twice, so a retry under the same id
/// has to be read as possibly-already-created. Answers with nothing otherwise,
/// like `updateConfiguration`: what became of it shows up in `listPipelines`.
class CreatePipeline {
public:
  explicit CreatePipeline(IdentifiedPipeline payload)
    : payload{std::move(payload)} {
  }

  IdentifiedPipeline payload;
};

using CreatePipelineFailure = variant<InvalidPipeline, PipelineExists>;

/// `GET /pipelines`
/// Every pipeline the deployment knows, each under the id it runs under, with
/// where its execution stands, sorted by id. What `createPipeline` and
/// `updateConfiguration` have persisted and started, read back: the platform
/// can compare this against what it meant to send and see what became of it.
class ListPipelines {
public:
  ListPipelines() = default;
};

/// `POST /pipelines/:pipelineId/start`
/// Starts another run of a persisted pipeline. Starting a pipeline that is
/// already running is a no-op, so a retry is safe. The deployment compiles the
/// persisted definition again before it starts the run.
class StartPipeline {
public:
  explicit StartPipeline(PipelinePath path) : path{std::move(path)} {
  }

  PipelinePath path;
};

using StartPipelineFailure = variant<InvalidPipeline, PipelineNotFound>;

/// `POST /pipelines/:pipelineId/stop`
/// Gracefully stops a running pipeline and remembers the stopped state. A
/// pipeline that is not running keeps its outcome, so a stop after a failure
/// does not erase the reason. Safe to retry either way.
class StopPipeline {
public:
  explicit StopPipeline(PipelinePath path) : path{std::move(path)} {
  }

  PipelinePath path;
};

/// `DELETE /pipelines/:pipelineId`
/// Stops the pipeline if it is running, then removes its persisted definition
/// and outcome. The request is idempotent: deleting an id that is no longer
/// present succeeds.
class DeletePipeline {
public:
  explicit DeletePipeline(PipelinePath path) : path{std::move(path)} {
  }

  PipelinePath path;
};

/// `POST /adhoc-pipelines`
/// Runs a pipeline once, for somebody to look at its events. Unlike
/// `createPipeline`, nothing is persisted and nothing shows up in
/// `listPipelines`: the ad-hoc pipeline lives in the process that answers this,
/// and ends with it. The definition has to compile, begin with a source, and
/// produce events, which is the opposite of what a persisted pipeline has to
/// do; one that ends in a sink or in bytes is refused. Answers as soon as the
/// ad-hoc pipeline has started. The deployment then opens a request of its own
/// to the platform, `POST
/// /deployments/:deploymentId/adhoc-pipelines/:adhocPipelineId/events`, and
/// streams the events in its body for as long as the ad-hoc pipeline lasts. A
/// start of an ad-hoc pipeline that is already live is a no-op, so a retry is
/// safe.
class StartAdhocPipeline {
public:
  explicit StartAdhocPipeline(AdhocPipelineDefinition payload)
    : payload{std::move(payload)} {
  }

  AdhocPipelineDefinition payload;
};

using StartAdhocPipelineFailure = variant<InvalidPipeline, Busy>;

/// What the platform asks of a running deployment
class DeploymentApi {
public:
  virtual ~DeploymentApi() = default;

  virtual auto handle(ReadHealth) -> Task<Liveness> = 0;

  virtual auto handle(UpdateConfiguration request) -> Task<void> = 0;

  virtual auto handle(CreatePipeline request)
    -> Task<Result<void, CreatePipelineFailure>>
    = 0;

  virtual auto handle(ListPipelines) -> Task<std::vector<PipelineStatus>> = 0;

  virtual auto handle(StartPipeline request)
    -> Task<Result<void, StartPipelineFailure>>
    = 0;

  virtual auto handle(StopPipeline request)
    -> Task<Result<void, PipelineNotFound>>
    = 0;

  virtual auto handle(DeletePipeline request) -> Task<void> = 0;

  virtual auto handle(StartAdhocPipeline request)
    -> Task<Result<void, StartAdhocPipelineFailure>>
    = 0;
};

auto dispatch(DeploymentApi& handler, HttpRequest request)
  -> Task<HttpResponse>;

// ── Codecs ──────────────────────────────────────────────────────────────────

/// Decodes a JSON value into the type, or returns why it is not one. Only
/// code that embeds API types in JSON of its own needs these; callers and
/// servers of the API do not.
///
/// The element points into the caller's parsed document, which must
/// outlive the call. The result owns its data.
auto parse_deployment_id(simdjson::dom::element value)
  -> Result<DeploymentId, ParseError>;
auto parse_deployment_path(simdjson::dom::element value)
  -> Result<DeploymentPath, ParseError>;
auto parse_workspace_id(simdjson::dom::element value)
  -> Result<WorkspaceId, ParseError>;
auto parse_deployment_info(simdjson::dom::element value)
  -> Result<DeploymentInfo, ParseError>;
auto parse_telemetry_count(simdjson::dom::element value)
  -> Result<TelemetryCount, ParseError>;
auto parse_tenzir_telemetry_telemetry_count(simdjson::dom::element value)
  -> Result<TenzirTelemetryTelemetryCount, ParseError>;
auto parse_pipeline_id(simdjson::dom::element value)
  -> Result<PipelineId, ParseError>;
auto parse_telemetry_count2(simdjson::dom::element value)
  -> Result<TelemetryCount2, ParseError>;
auto parse_connector_flow(simdjson::dom::element value)
  -> Result<ConnectorFlow, ParseError>;
auto parse_pipeline_sample(simdjson::dom::element value)
  -> Result<PipelineSample, ParseError>;
auto parse_flow(simdjson::dom::element value) -> Result<Flow, ParseError>;
auto parse_telemetry_seconds(simdjson::dom::element value)
  -> Result<TelemetrySeconds, ParseError>;
auto parse_telemetry_count3(simdjson::dom::element value)
  -> Result<TelemetryCount3, ParseError>;
auto parse_telemetry_small_count(simdjson::dom::element value)
  -> Result<TelemetrySmallCount, ParseError>;
auto parse_operator_sample(simdjson::dom::element value)
  -> Result<OperatorSample, ParseError>;
auto parse_tenzir_telemetry_telemetry_small_count(simdjson::dom::element value)
  -> Result<TenzirTelemetryTelemetrySmallCount, ParseError>;
auto parse_severity(simdjson::dom::element value)
  -> Result<Severity, ParseError>;
auto parse_push_telemetry_payload_diagnostics_notes(simdjson::dom::element value)
  -> Result<PushTelemetryPayloadDiagnosticsNotes, ParseError>;
auto parse_push_telemetry_payload_diagnostics_annotations(
  simdjson::dom::element value)
  -> Result<PushTelemetryPayloadDiagnosticsAnnotations, ParseError>;
auto parse_diagnostic_sample(simdjson::dom::element value)
  -> Result<DiagnosticSample, ParseError>;
auto parse_run_state(simdjson::dom::element value)
  -> Result<RunState, ParseError>;
auto parse_run_transition(simdjson::dom::element value)
  -> Result<RunTransition, ParseError>;
auto parse_tenzir_telemetry_telemetry_seconds(simdjson::dom::element value)
  -> Result<TenzirTelemetryTelemetrySeconds, ParseError>;
auto parse_deployment_sample(simdjson::dom::element value)
  -> Result<DeploymentSample, ParseError>;
auto parse_batch(simdjson::dom::element value) -> Result<Batch, ParseError>;
auto parse_liveness(simdjson::dom::element value)
  -> Result<Liveness, ParseError>;
auto parse_pipeline(simdjson::dom::element value)
  -> Result<Pipeline, ParseError>;
auto parse_configuration(simdjson::dom::element value)
  -> Result<Configuration, ParseError>;
auto parse_identified_pipeline(simdjson::dom::element value)
  -> Result<IdentifiedPipeline, ParseError>;
auto parse_pipeline_state(simdjson::dom::element value)
  -> Result<PipelineState, ParseError>;
auto parse_pipeline_status(simdjson::dom::element value)
  -> Result<PipelineStatus, ParseError>;
auto parse_pipeline_path(simdjson::dom::element value)
  -> Result<PipelinePath, ParseError>;
auto parse_adhoc_pipeline_id(simdjson::dom::element value)
  -> Result<AdhocPipelineId, ParseError>;
auto parse_adhoc_pipeline_definition(simdjson::dom::element value)
  -> Result<AdhocPipelineDefinition, ParseError>;

/// Appends the type as JSON, in the bytes the API puts on the wire, so a
/// copy stored elsewhere decodes with the same function.
auto write_json(DeploymentId const& value, std::string& out) -> void;
auto write_json(DeploymentPath const& value, std::string& out) -> void;
auto write_json(WorkspaceId const& value, std::string& out) -> void;
auto write_json(DeploymentInfo const& value, std::string& out) -> void;
auto write_json(TelemetryCount const& value, std::string& out) -> void;
auto write_json(TenzirTelemetryTelemetryCount const& value, std::string& out)
  -> void;
auto write_json(PipelineId const& value, std::string& out) -> void;
auto write_json(TelemetryCount2 const& value, std::string& out) -> void;
auto write_json(ConnectorFlow const& value, std::string& out) -> void;
auto write_json(PipelineSample const& value, std::string& out) -> void;
auto write_json(Flow const& value, std::string& out) -> void;
auto write_json(TelemetrySeconds const& value, std::string& out) -> void;
auto write_json(TelemetryCount3 const& value, std::string& out) -> void;
auto write_json(TelemetrySmallCount const& value, std::string& out) -> void;
auto write_json(OperatorSample const& value, std::string& out) -> void;
auto write_json(TenzirTelemetryTelemetrySmallCount const& value,
                std::string& out) -> void;
auto write_json(Severity const& value, std::string& out) -> void;
auto write_json(PushTelemetryPayloadDiagnosticsNotes const& value,
                std::string& out) -> void;
auto write_json(PushTelemetryPayloadDiagnosticsAnnotations const& value,
                std::string& out) -> void;
auto write_json(DiagnosticSample const& value, std::string& out) -> void;
auto write_json(RunState const& value, std::string& out) -> void;
auto write_json(RunTransition const& value, std::string& out) -> void;
auto write_json(TenzirTelemetryTelemetrySeconds const& value, std::string& out)
  -> void;
auto write_json(DeploymentSample const& value, std::string& out) -> void;
auto write_json(Batch const& value, std::string& out) -> void;
auto write_json(Liveness const& value, std::string& out) -> void;
auto write_json(Pipeline const& value, std::string& out) -> void;
auto write_json(Configuration const& value, std::string& out) -> void;
auto write_json(IdentifiedPipeline const& value, std::string& out) -> void;
auto write_json(PipelineState const& value, std::string& out) -> void;
auto write_json(PipelineStatus const& value, std::string& out) -> void;
auto write_json(PipelinePath const& value, std::string& out) -> void;
auto write_json(AdhocPipelineId const& value, std::string& out) -> void;
auto write_json(AdhocPipelineDefinition const& value, std::string& out) -> void;

} // namespace tenzir
