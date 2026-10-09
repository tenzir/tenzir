//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Generated from the Effect API definitions in `platform/services/shared/api`.
// Do not edit. Run `bun tools/codegen/main.ts` in the platform checkout.

#include <tenzir/controller/api_support.hpp>
#include <tenzir/controller/generated/api.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/try.hpp>

#include <fmt/format.h>

#include <iterator>
#include <regex>
#include <simdjson.h>
#include <utility>

namespace tenzir {

namespace {

/// Lifts anything the call itself can fail with into `CallError`, so that a
/// `CO_TRY` has one conversion to make rather than two.
struct AsCallError {
  template <class Failed>
    requires std::constructible_from<CallError, Failed>
  auto operator()(Failed failed) const -> CallError {
    return CallError{std::move(failed)};
  }
};

inline constexpr auto as_call_error = AsCallError{};

/// A body that did not match the schema chosen for it. Distinct from a status
/// nobody declared: there, no schema was chosen at all.
[[maybe_unused]] auto as_parse_error(ParseError error) -> CallError {
  return CallError{std::move(error)};
}

/// A body that could not be read under a status that does declare one.
[[maybe_unused]] auto
as_response_error(std::uint16_t status, std::string const& body) {
  return [status, body](ParseError) -> CallError {
    return CallError{
      ResponseError{ResponseError::Reason::decode, status, body}};
  };
}

} // namespace

// ── Codecs ──────────────────────────────────────────────────────────────────

auto parse_internal_server_error(simdjson::dom::element value)
  -> Result<InternalServerError, ParseError>;
auto parse_not_found(simdjson::dom::element value)
  -> Result<NotFound, ParseError>;
auto parse_unauthorized(simdjson::dom::element value)
  -> Result<Unauthorized, ParseError>;
auto parse_unauthenticated(simdjson::dom::element value)
  -> Result<Unauthenticated, ParseError>;
auto parse_credential_conflict(simdjson::dom::element value)
  -> Result<CredentialConflict, ParseError>;
auto parse_validation(simdjson::dom::element value)
  -> Result<Validation, ParseError>;
auto parse_too_large(simdjson::dom::element value)
  -> Result<TooLarge, ParseError>;
auto parse_service_unavailable(simdjson::dom::element value)
  -> Result<ServiceUnavailable, ParseError>;
auto parse_invalid_pipeline(simdjson::dom::element value)
  -> Result<InvalidPipeline, ParseError>;
auto parse_pipeline_exists(simdjson::dom::element value)
  -> Result<PipelineExists, ParseError>;
auto parse_pipeline_not_found(simdjson::dom::element value)
  -> Result<PipelineNotFound, ParseError>;
auto parse_busy(simdjson::dom::element value) -> Result<Busy, ParseError>;
auto write_json(InternalServerError const& value, std::string& out) -> void;
auto write_json(NotFound const& value, std::string& out) -> void;
auto write_json(Unauthorized const& value, std::string& out) -> void;
auto write_json(Unauthenticated const& value, std::string& out) -> void;
auto write_json(CredentialConflict const& value, std::string& out) -> void;
auto write_json(Validation const& value, std::string& out) -> void;
auto write_json(TooLarge const& value, std::string& out) -> void;
auto write_json(ServiceUnavailable const& value, std::string& out) -> void;
auto write_json(InvalidPipeline const& value, std::string& out) -> void;
auto write_json(PipelineExists const& value, std::string& out) -> void;
auto write_json(PipelineNotFound const& value, std::string& out) -> void;
auto write_json(Busy const& value, std::string& out) -> void;

// ── Failures ────────────────────────────────────────────────────────────────

auto parse_internal_server_error(simdjson::dom::element value)
  -> Result<InternalServerError, ParseError> {
  TRY(auto object, api::object_of(value, "InternalServerError"));

  TRY(auto tag_raw, api::field(object, "_tag", "InternalServerError"));
  TRY(auto tag, api::string_of(tag_raw, "InternalServerError"));
  if (tag != "InternalServerError") {
    return Err{ParseError{"InternalServerError",
                          "expected `_tag` 'InternalServerError'"}};
  }

  return InternalServerError{};
}

auto write_json(InternalServerError const&, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"InternalServerError\"";
  out += '}';
}

auto parse_not_found(simdjson::dom::element value)
  -> Result<NotFound, ParseError> {
  TRY(auto object, api::object_of(value, "NotFound"));

  TRY(auto tag_raw, api::field(object, "_tag", "NotFound"));
  TRY(auto tag, api::string_of(tag_raw, "NotFound"));
  if (tag != "NotFound") {
    return Err{ParseError{"NotFound", "expected `_tag` 'NotFound'"}};
  }

  return NotFound{};
}

auto write_json(NotFound const&, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"NotFound\"";
  out += '}';
}

auto parse_unauthorized(simdjson::dom::element value)
  -> Result<Unauthorized, ParseError> {
  TRY(auto object, api::object_of(value, "Unauthorized"));

  TRY(auto tag_raw, api::field(object, "_tag", "Unauthorized"));
  TRY(auto tag, api::string_of(tag_raw, "Unauthorized"));
  if (tag != "Unauthorized") {
    return Err{ParseError{"Unauthorized", "expected `_tag` 'Unauthorized'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "Unauthorized"));
  TRY(auto message, api::string_of(message_raw, "Unauthorized.message"));

  return Unauthorized{std::move(message)};
}

auto write_json(Unauthorized const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"Unauthorized\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto parse_unauthenticated(simdjson::dom::element value)
  -> Result<Unauthenticated, ParseError> {
  TRY(auto object, api::object_of(value, "Unauthenticated"));

  TRY(auto tag_raw, api::field(object, "_tag", "Unauthenticated"));
  TRY(auto tag, api::string_of(tag_raw, "Unauthenticated"));
  if (tag != "Unauthenticated") {
    return Err{
      ParseError{"Unauthenticated", "expected `_tag` 'Unauthenticated'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "Unauthenticated"));
  TRY(auto message, api::string_of(message_raw, "Unauthenticated.message"));

  return Unauthenticated{std::move(message)};
}

auto write_json(Unauthenticated const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"Unauthenticated\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto parse_credential_conflict(simdjson::dom::element value)
  -> Result<CredentialConflict, ParseError> {
  TRY(auto object, api::object_of(value, "CredentialConflict"));

  TRY(auto tag_raw, api::field(object, "_tag", "CredentialConflict"));
  TRY(auto tag, api::string_of(tag_raw, "CredentialConflict"));
  if (tag != "CredentialConflict") {
    return Err{
      ParseError{"CredentialConflict", "expected `_tag` 'CredentialConflict'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "CredentialConflict"));
  TRY(auto message, api::string_of(message_raw, "CredentialConflict.message"));

  return CredentialConflict{std::move(message)};
}

auto write_json(CredentialConflict const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"CredentialConflict\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto parse_validation(simdjson::dom::element value)
  -> Result<Validation, ParseError> {
  TRY(auto object, api::object_of(value, "Validation"));

  TRY(auto tag_raw, api::field(object, "_tag", "Validation"));
  TRY(auto tag, api::string_of(tag_raw, "Validation"));
  if (tag != "Validation") {
    return Err{ParseError{"Validation", "expected `_tag` 'Validation'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "Validation"));
  TRY(auto message, api::string_of(message_raw, "Validation.message"));

  return Validation{std::move(message)};
}

auto write_json(Validation const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"Validation\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto parse_too_large(simdjson::dom::element value)
  -> Result<TooLarge, ParseError> {
  TRY(auto object, api::object_of(value, "TooLarge"));

  TRY(auto tag_raw, api::field(object, "_tag", "TooLarge"));
  TRY(auto tag, api::string_of(tag_raw, "TooLarge"));
  if (tag != "TooLarge") {
    return Err{ParseError{"TooLarge", "expected `_tag` 'TooLarge'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "TooLarge"));
  TRY(auto message, api::string_of(message_raw, "TooLarge.message"));

  return TooLarge{std::move(message)};
}

auto write_json(TooLarge const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"TooLarge\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto parse_service_unavailable(simdjson::dom::element value)
  -> Result<ServiceUnavailable, ParseError> {
  TRY(auto object, api::object_of(value, "ServiceUnavailable"));

  TRY(auto tag_raw, api::field(object, "_tag", "ServiceUnavailable"));
  TRY(auto tag, api::string_of(tag_raw, "ServiceUnavailable"));
  if (tag != "ServiceUnavailable") {
    return Err{
      ParseError{"ServiceUnavailable", "expected `_tag` 'ServiceUnavailable'"}};
  }

  return ServiceUnavailable{};
}

auto write_json(ServiceUnavailable const&, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"ServiceUnavailable\"";
  out += '}';
}

auto parse_invalid_pipeline(simdjson::dom::element value)
  -> Result<InvalidPipeline, ParseError> {
  TRY(auto object, api::object_of(value, "InvalidPipeline"));

  TRY(auto tag_raw, api::field(object, "_tag", "InvalidPipeline"));
  TRY(auto tag, api::string_of(tag_raw, "InvalidPipeline"));
  if (tag != "InvalidPipeline") {
    return Err{
      ParseError{"InvalidPipeline", "expected `_tag` 'InvalidPipeline'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "InvalidPipeline"));
  TRY(auto message, api::string_of(message_raw, "InvalidPipeline.message"));

  return InvalidPipeline{std::move(message)};
}

auto write_json(InvalidPipeline const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"InvalidPipeline\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto parse_pipeline_exists(simdjson::dom::element value)
  -> Result<PipelineExists, ParseError> {
  TRY(auto object, api::object_of(value, "PipelineExists"));

  TRY(auto tag_raw, api::field(object, "_tag", "PipelineExists"));
  TRY(auto tag, api::string_of(tag_raw, "PipelineExists"));
  if (tag != "PipelineExists") {
    return Err{
      ParseError{"PipelineExists", "expected `_tag` 'PipelineExists'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "PipelineExists"));
  TRY(auto message, api::string_of(message_raw, "PipelineExists.message"));

  return PipelineExists{std::move(message)};
}

auto write_json(PipelineExists const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"PipelineExists\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto parse_pipeline_not_found(simdjson::dom::element value)
  -> Result<PipelineNotFound, ParseError> {
  TRY(auto object, api::object_of(value, "PipelineNotFound"));

  TRY(auto tag_raw, api::field(object, "_tag", "PipelineNotFound"));
  TRY(auto tag, api::string_of(tag_raw, "PipelineNotFound"));
  if (tag != "PipelineNotFound") {
    return Err{
      ParseError{"PipelineNotFound", "expected `_tag` 'PipelineNotFound'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "PipelineNotFound"));
  TRY(auto message, api::string_of(message_raw, "PipelineNotFound.message"));

  return PipelineNotFound{std::move(message)};
}

auto write_json(PipelineNotFound const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"PipelineNotFound\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto parse_busy(simdjson::dom::element value) -> Result<Busy, ParseError> {
  TRY(auto object, api::object_of(value, "Busy"));

  TRY(auto tag_raw, api::field(object, "_tag", "Busy"));
  TRY(auto tag, api::string_of(tag_raw, "Busy"));
  if (tag != "Busy") {
    return Err{ParseError{"Busy", "expected `_tag` 'Busy'"}};
  }

  TRY(auto message_raw, api::field(object, "message", "Busy"));
  TRY(auto message, api::string_of(message_raw, "Busy.message"));

  return Busy{std::move(message)};
}

auto write_json(Busy const& value, std::string& out) -> void {
  out += '{';
  out += "\"_tag\":\"Busy\"";
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

// ── Decoding ────────────────────────────────────────────────────────────────

auto DeploymentId::make(std::string value) -> Result<DeploymentId, ParseError> {
  static auto const pattern = std::regex{R"(^[A-Za-z0-9][A-Za-z0-9_-]*$)"};
  if (not std::regex_match(value, pattern)) {
    return Err{ParseError{
      "DeploymentId", fmt::format("`{}` does not match the pattern", value)}};
  }
  return DeploymentId{std::move(value)};
}

auto parse_deployment_id(simdjson::dom::element value)
  -> Result<DeploymentId, ParseError> {
  TRY(auto raw, api::string_of(value, "DeploymentId"));
  return DeploymentId::make(std::move(raw));
}

auto parse_deployment_path(simdjson::dom::element value)
  -> Result<DeploymentPath, ParseError> {
  TRY(auto object, api::object_of(value, "DeploymentPath"));

  TRY(auto deployment_id_raw,
      api::field(object, "deploymentId", "DeploymentPath"));
  TRY(auto deployment_id, parse_deployment_id(deployment_id_raw));

  return DeploymentPath{std::move(deployment_id)};
}

auto WorkspaceId::make(std::string value) -> Result<WorkspaceId, ParseError> {
  static auto const pattern = std::regex{
    R"(^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$)"};
  if (not std::regex_match(value, pattern)) {
    return Err{ParseError{
      "WorkspaceId", fmt::format("`{}` does not match the pattern", value)}};
  }
  return WorkspaceId{std::move(value)};
}

auto parse_workspace_id(simdjson::dom::element value)
  -> Result<WorkspaceId, ParseError> {
  TRY(auto raw, api::string_of(value, "WorkspaceId"));
  return WorkspaceId::make(std::move(raw));
}

auto parse_deployment_info(simdjson::dom::element value)
  -> Result<DeploymentInfo, ParseError> {
  TRY(auto object, api::object_of(value, "DeploymentInfo"));

  TRY(auto id_raw, api::field(object, "id", "DeploymentInfo"));
  TRY(auto id, parse_deployment_id(id_raw));

  auto key_digest = Option<std::string>{None{}};
  if (auto key_digest_raw = api::optional_field(object, "keyDigest")) {
    TRY(auto key_digest_present,
        api::string_of(*key_digest_raw, "DeploymentInfo.keyDigest"));
    key_digest = std::move(key_digest_present);
  }

  TRY(auto workspace_id_raw,
      api::field(object, "workspaceId", "DeploymentInfo"));
  TRY(auto workspace_id, parse_workspace_id(workspace_id_raw));

  TRY(auto name_raw, api::field(object, "name", "DeploymentInfo"));
  TRY(auto name, api::string_of(name_raw, "DeploymentInfo.name"));

  auto version = Option<std::string>{None{}};
  if (auto version_raw = api::optional_field(object, "version")) {
    TRY(auto version_present,
        api::string_of(*version_raw, "DeploymentInfo.version"));
    version = std::move(version_present);
  }

  auto last_seen = Option<time>{None{}};
  if (auto last_seen_raw = api::optional_field(object, "lastSeen")) {
    TRY(auto last_seen_present,
        api::time_of(*last_seen_raw, "DeploymentInfo.lastSeen"));
    last_seen = std::move(last_seen_present);
  }

  return DeploymentInfo{std::move(id),           std::move(key_digest),
                        std::move(workspace_id), std::move(name),
                        std::move(version),      std::move(last_seen)};
}

auto TelemetryCount::make(std::int64_t value)
  -> Result<TelemetryCount, ParseError> {
  if (value < -9'007'199'254'740'991 or value > 9'007'199'254'740'991) {
    return Err{ParseError{"TelemetryCount",
                          "integer exceeds the wire safe-integer range"}};
  }
  if (value < 0) {
    return Err{ParseError{"TelemetryCount", "integer is below the minimum"}};
  }
  return TelemetryCount{std::move(value)};
}

auto parse_telemetry_count(simdjson::dom::element value)
  -> Result<TelemetryCount, ParseError> {
  TRY(auto raw, api::int64_of(value, "TelemetryCount"));
  return TelemetryCount::make(std::move(raw));
}

auto TenzirTelemetryTelemetryCount::make(std::int64_t value)
  -> Result<TenzirTelemetryTelemetryCount, ParseError> {
  if (value < -9'007'199'254'740'991 or value > 9'007'199'254'740'991) {
    return Err{ParseError{"TenzirTelemetryTelemetryCount",
                          "integer exceeds the wire safe-integer range"}};
  }
  if (value < 0) {
    return Err{ParseError{"TenzirTelemetryTelemetryCount",
                          "integer is below the minimum"}};
  }
  return TenzirTelemetryTelemetryCount{std::move(value)};
}

auto parse_tenzir_telemetry_telemetry_count(simdjson::dom::element value)
  -> Result<TenzirTelemetryTelemetryCount, ParseError> {
  TRY(auto raw, api::int64_of(value, "TenzirTelemetryTelemetryCount"));
  return TenzirTelemetryTelemetryCount::make(std::move(raw));
}

auto PipelineId::make(std::string value) -> Result<PipelineId, ParseError> {
  static auto const pattern = std::regex{R"(^[A-Za-z0-9][A-Za-z0-9_-]*$)"};
  if (not std::regex_match(value, pattern)) {
    return Err{ParseError{
      "PipelineId", fmt::format("`{}` does not match the pattern", value)}};
  }
  return PipelineId{std::move(value)};
}

auto parse_pipeline_id(simdjson::dom::element value)
  -> Result<PipelineId, ParseError> {
  TRY(auto raw, api::string_of(value, "PipelineId"));
  return PipelineId::make(std::move(raw));
}

auto TelemetryCount2::make(std::int64_t value)
  -> Result<TelemetryCount2, ParseError> {
  if (value < -9'007'199'254'740'991 or value > 9'007'199'254'740'991) {
    return Err{ParseError{"TelemetryCount2",
                          "integer exceeds the wire safe-integer range"}};
  }
  if (value < 0) {
    return Err{ParseError{"TelemetryCount2", "integer is below the minimum"}};
  }
  return TelemetryCount2{std::move(value)};
}

auto parse_telemetry_count2(simdjson::dom::element value)
  -> Result<TelemetryCount2, ParseError> {
  TRY(auto raw, api::int64_of(value, "TelemetryCount2"));
  return TelemetryCount2::make(std::move(raw));
}

auto parse_connector_flow(simdjson::dom::element value)
  -> Result<ConnectorFlow, ParseError> {
  TRY(auto object, api::object_of(value, "ConnectorFlow"));

  TRY(auto connector_raw, api::field(object, "connector", "ConnectorFlow"));
  TRY(auto connector, api::string_of(connector_raw, "ConnectorFlow.connector"));

  TRY(auto events_raw, api::field(object, "events", "ConnectorFlow"));
  TRY(auto events, parse_telemetry_count2(events_raw));

  TRY(auto bytes_raw, api::field(object, "bytes", "ConnectorFlow"));
  TRY(auto bytes, parse_telemetry_count2(bytes_raw));

  return ConnectorFlow{std::move(connector), std::move(events),
                       std::move(bytes)};
}

auto parse_pipeline_sample(simdjson::dom::element value)
  -> Result<PipelineSample, ParseError> {
  TRY(auto object, api::object_of(value, "PipelineSample"));

  TRY(auto pipeline_id_raw, api::field(object, "pipelineId", "PipelineSample"));
  TRY(auto pipeline_id, parse_pipeline_id(pipeline_id_raw));

  TRY(auto run_raw, api::field(object, "run", "PipelineSample"));
  TRY(auto run, parse_telemetry_count2(run_raw));

  TRY(auto window_raw, api::field(object, "window", "PipelineSample"));
  TRY(auto window, api::time_of(window_raw, "PipelineSample.window"));

  TRY(auto sources_raw, api::field(object, "sources", "PipelineSample"));
  auto sources = std::vector<ConnectorFlow>{};
  auto sources_array = simdjson::dom::array{};
  if (sources_raw.get_array().get(sources_array) != simdjson::SUCCESS) {
    return Err{ParseError{"PipelineSample.sources", "expected an array"}};
  }
  for (auto sources_item : sources_array) {
    TRY(auto sources_decoded, parse_connector_flow(sources_item));
    sources.push_back(std::move(sources_decoded));
  }

  TRY(auto sinks_raw, api::field(object, "sinks", "PipelineSample"));
  auto sinks = std::vector<ConnectorFlow>{};
  auto sinks_array = simdjson::dom::array{};
  if (sinks_raw.get_array().get(sinks_array) != simdjson::SUCCESS) {
    return Err{ParseError{"PipelineSample.sinks", "expected an array"}};
  }
  for (auto sinks_item : sinks_array) {
    TRY(auto sinks_decoded, parse_connector_flow(sinks_item));
    sinks.push_back(std::move(sinks_decoded));
  }

  return PipelineSample{std::move(pipeline_id), std::move(run), window,
                        std::move(sources), std::move(sinks)};
}

auto parse_flow(simdjson::dom::element value) -> Result<Flow, ParseError> {
  TRY(auto object, api::object_of(value, "Flow"));

  TRY(auto events_raw, api::field(object, "events", "Flow"));
  TRY(auto events, parse_telemetry_count2(events_raw));

  TRY(auto bytes_raw, api::field(object, "bytes", "Flow"));
  TRY(auto bytes, parse_telemetry_count2(bytes_raw));

  return Flow{std::move(events), std::move(bytes)};
}

auto TelemetrySeconds::make(double value)
  -> Result<TelemetrySeconds, ParseError> {
  return TelemetrySeconds{std::move(value)};
}

auto parse_telemetry_seconds(simdjson::dom::element value)
  -> Result<TelemetrySeconds, ParseError> {
  TRY(auto raw, api::double_of(value, "TelemetrySeconds"));
  return TelemetrySeconds::make(std::move(raw));
}

auto TelemetryCount3::make(std::int64_t value)
  -> Result<TelemetryCount3, ParseError> {
  if (value < -9'007'199'254'740'991 or value > 9'007'199'254'740'991) {
    return Err{ParseError{"TelemetryCount3",
                          "integer exceeds the wire safe-integer range"}};
  }
  if (value < 0) {
    return Err{ParseError{"TelemetryCount3", "integer is below the minimum"}};
  }
  return TelemetryCount3{std::move(value)};
}

auto parse_telemetry_count3(simdjson::dom::element value)
  -> Result<TelemetryCount3, ParseError> {
  TRY(auto raw, api::int64_of(value, "TelemetryCount3"));
  return TelemetryCount3::make(std::move(raw));
}

auto TelemetrySmallCount::make(std::int64_t value)
  -> Result<TelemetrySmallCount, ParseError> {
  if (value < -9'007'199'254'740'991 or value > 9'007'199'254'740'991) {
    return Err{ParseError{"TelemetrySmallCount",
                          "integer exceeds the wire safe-integer range"}};
  }
  if (value < 0) {
    return Err{
      ParseError{"TelemetrySmallCount", "integer is below the minimum"}};
  }
  if (value > 4294967295) {
    return Err{
      ParseError{"TelemetrySmallCount", "integer exceeds the maximum"}};
  }
  return TelemetrySmallCount{std::move(value)};
}

auto parse_telemetry_small_count(simdjson::dom::element value)
  -> Result<TelemetrySmallCount, ParseError> {
  TRY(auto raw, api::int64_of(value, "TelemetrySmallCount"));
  return TelemetrySmallCount::make(std::move(raw));
}

auto parse_operator_sample(simdjson::dom::element value)
  -> Result<OperatorSample, ParseError> {
  TRY(auto object, api::object_of(value, "OperatorSample"));

  TRY(auto pipeline_id_raw, api::field(object, "pipelineId", "OperatorSample"));
  TRY(auto pipeline_id, parse_pipeline_id(pipeline_id_raw));

  TRY(auto run_raw, api::field(object, "run", "OperatorSample"));
  TRY(auto run, parse_telemetry_count2(run_raw));

  TRY(auto operator_id_raw, api::field(object, "operatorId", "OperatorSample"));
  TRY(auto operator_id,
      api::string_of(operator_id_raw, "OperatorSample.operatorId"));

  TRY(auto name_raw, api::field(object, "name", "OperatorSample"));
  TRY(auto name, api::string_of(name_raw, "OperatorSample.name"));

  TRY(auto window_raw, api::field(object, "window", "OperatorSample"));
  TRY(auto window, api::time_of(window_raw, "OperatorSample.window"));

  TRY(auto input_raw, api::field(object, "input", "OperatorSample"));
  TRY(auto input, parse_flow(input_raw));

  TRY(auto output_raw, api::field(object, "output", "OperatorSample"));
  TRY(auto output, parse_flow(output_raw));

  TRY(auto cpu_seconds_raw, api::field(object, "cpuSeconds", "OperatorSample"));
  TRY(auto cpu_seconds, parse_telemetry_seconds(cpu_seconds_raw));

  TRY(auto input_bytes_raw, api::field(object, "inputBytes", "OperatorSample"));
  TRY(auto input_bytes, parse_telemetry_count3(input_bytes_raw));

  TRY(auto input_capacity_raw,
      api::field(object, "inputCapacity", "OperatorSample"));
  TRY(auto input_capacity, parse_telemetry_count2(input_capacity_raw));

  TRY(auto tasks_raw, api::field(object, "tasks", "OperatorSample"));
  TRY(auto tasks, parse_telemetry_small_count(tasks_raw));

  return OperatorSample{std::move(pipeline_id),
                        std::move(run),
                        std::move(operator_id),
                        std::move(name),
                        window,
                        std::move(input),
                        std::move(output),
                        std::move(cpu_seconds),
                        std::move(input_bytes),
                        std::move(input_capacity),
                        std::move(tasks)};
}

auto TenzirTelemetryTelemetrySmallCount::make(std::int64_t value)
  -> Result<TenzirTelemetryTelemetrySmallCount, ParseError> {
  if (value < -9'007'199'254'740'991 or value > 9'007'199'254'740'991) {
    return Err{ParseError{"TenzirTelemetryTelemetrySmallCount",
                          "integer exceeds the wire safe-integer range"}};
  }
  if (value < 0) {
    return Err{ParseError{"TenzirTelemetryTelemetrySmallCount",
                          "integer is below the minimum"}};
  }
  if (value > 4294967295) {
    return Err{ParseError{"TenzirTelemetryTelemetrySmallCount",
                          "integer exceeds the maximum"}};
  }
  return TenzirTelemetryTelemetrySmallCount{std::move(value)};
}

auto parse_tenzir_telemetry_telemetry_small_count(simdjson::dom::element value)
  -> Result<TenzirTelemetryTelemetrySmallCount, ParseError> {
  TRY(auto raw, api::int64_of(value, "TenzirTelemetryTelemetrySmallCount"));
  return TenzirTelemetryTelemetrySmallCount::make(std::move(raw));
}

auto to_string(Severity value) -> std::string_view {
  switch (value) {
    case Severity::note:
      return "note";
    case Severity::warning:
      return "warning";
    case Severity::error:
      return "error";
  }
  TENZIR_UNREACHABLE();
}

auto parse_severity(simdjson::dom::element value)
  -> Result<Severity, ParseError> {
  TRY(auto raw, api::string_of(value, "Severity"));
  if (raw == "note") {
    return Severity::note;
  }
  if (raw == "warning") {
    return Severity::warning;
  }
  if (raw == "error") {
    return Severity::error;
  }
  return Err{ParseError{
    "Severity", fmt::format("`{}` is not one of note, warning, error", raw)}};
}

auto parse_push_telemetry_payload_diagnostics_notes(simdjson::dom::element value)
  -> Result<PushTelemetryPayloadDiagnosticsNotes, ParseError> {
  TRY(auto object,
      api::object_of(value, "PushTelemetryPayloadDiagnosticsNotes"));

  TRY(auto kind_raw,
      api::field(object, "kind", "PushTelemetryPayloadDiagnosticsNotes"));
  TRY(auto kind,
      api::string_of(kind_raw, "PushTelemetryPayloadDiagnosticsNotes.kind"));

  TRY(auto message_raw,
      api::field(object, "message", "PushTelemetryPayloadDiagnosticsNotes"));
  TRY(auto message, api::string_of(message_raw, "PushTelemetryPayloadDiagnostic"
                                                "sNotes.message"));

  return PushTelemetryPayloadDiagnosticsNotes{std::move(kind),
                                              std::move(message)};
}

auto parse_push_telemetry_payload_diagnostics_annotations(
  simdjson::dom::element value)
  -> Result<PushTelemetryPayloadDiagnosticsAnnotations, ParseError> {
  TRY(auto object,
      api::object_of(value, "PushTelemetryPayloadDiagnosticsAnnotations"));

  TRY(auto primary_raw,
      api::field(object, "primary",
                 "PushTelemetryPayloadDiagnosticsAnnotations"));
  TRY(auto primary, api::bool_of(primary_raw, "PushTelemetryPayloadDiagnosticsA"
                                              "nnotations.primary"));

  TRY(auto text_raw,
      api::field(object, "text", "PushTelemetryPayloadDiagnosticsAnnotations"));
  TRY(auto text, api::string_of(text_raw, "PushTelemetryPayloadDiagnosticsAnnot"
                                          "ations.text"));

  TRY(auto begin_raw, api::field(object, "begin",
                                 "PushTelemetryPayloadDiagnosticsAnnotations"));
  TRY(auto begin, parse_telemetry_count2(begin_raw));

  TRY(auto end_raw,
      api::field(object, "end", "PushTelemetryPayloadDiagnosticsAnnotations"));
  TRY(auto end, parse_telemetry_count2(end_raw));

  return PushTelemetryPayloadDiagnosticsAnnotations{
    primary, std::move(text), std::move(begin), std::move(end)};
}

auto parse_diagnostic_sample(simdjson::dom::element value)
  -> Result<DiagnosticSample, ParseError> {
  TRY(auto object, api::object_of(value, "DiagnosticSample"));

  TRY(auto pipeline_id_raw,
      api::field(object, "pipelineId", "DiagnosticSample"));
  TRY(auto pipeline_id, parse_pipeline_id(pipeline_id_raw));

  TRY(auto run_raw, api::field(object, "run", "DiagnosticSample"));
  TRY(auto run, parse_telemetry_count2(run_raw));

  TRY(auto first_seen_raw, api::field(object, "firstSeen", "DiagnosticSample"));
  TRY(auto first_seen,
      api::time_of(first_seen_raw, "DiagnosticSample.firstSeen"));

  TRY(auto last_seen_raw, api::field(object, "lastSeen", "DiagnosticSample"));
  TRY(auto last_seen, api::time_of(last_seen_raw, "DiagnosticSample.lastSeen"));

  TRY(auto occurrences_raw,
      api::field(object, "occurrences", "DiagnosticSample"));
  TRY(auto occurrences,
      parse_tenzir_telemetry_telemetry_small_count(occurrences_raw));

  TRY(auto fingerprint_raw,
      api::field(object, "fingerprint", "DiagnosticSample"));
  TRY(auto fingerprint,
      api::string_of(fingerprint_raw, "DiagnosticSample.fingerprint"));

  TRY(auto severity_raw, api::field(object, "severity", "DiagnosticSample"));
  TRY(auto severity, parse_severity(severity_raw));

  TRY(auto message_raw, api::field(object, "message", "DiagnosticSample"));
  TRY(auto message, api::string_of(message_raw, "DiagnosticSample.message"));

  TRY(auto rendered_raw, api::field(object, "rendered", "DiagnosticSample"));
  TRY(auto rendered, api::string_of(rendered_raw, "DiagnosticSample.rendered"));

  TRY(auto notes_raw, api::field(object, "notes", "DiagnosticSample"));
  auto notes = std::vector<PushTelemetryPayloadDiagnosticsNotes>{};
  auto notes_array = simdjson::dom::array{};
  if (notes_raw.get_array().get(notes_array) != simdjson::SUCCESS) {
    return Err{ParseError{"DiagnosticSample.notes", "expected an array"}};
  }
  for (auto notes_item : notes_array) {
    TRY(auto notes_decoded,
        parse_push_telemetry_payload_diagnostics_notes(notes_item));
    notes.push_back(std::move(notes_decoded));
  }

  TRY(auto annotations_raw,
      api::field(object, "annotations", "DiagnosticSample"));
  auto annotations = std::vector<PushTelemetryPayloadDiagnosticsAnnotations>{};
  auto annotations_array = simdjson::dom::array{};
  if (annotations_raw.get_array().get(annotations_array) != simdjson::SUCCESS) {
    return Err{ParseError{"DiagnosticSample.annotations", "expected an array"}};
  }
  for (auto annotations_item : annotations_array) {
    TRY(auto annotations_decoded,
        parse_push_telemetry_payload_diagnostics_annotations(annotations_item));
    annotations.push_back(std::move(annotations_decoded));
  }

  return DiagnosticSample{std::move(pipeline_id),
                          std::move(run),
                          first_seen,
                          last_seen,
                          std::move(occurrences),
                          std::move(fingerprint),
                          std::move(severity),
                          std::move(message),
                          std::move(rendered),
                          std::move(notes),
                          std::move(annotations)};
}

auto to_string(RunState value) -> std::string_view {
  switch (value) {
    case RunState::running:
      return "running";
    case RunState::finished:
      return "finished";
    case RunState::failed:
      return "failed";
    case RunState::stopped:
      return "stopped";
  }
  TENZIR_UNREACHABLE();
}

auto parse_run_state(simdjson::dom::element value)
  -> Result<RunState, ParseError> {
  TRY(auto raw, api::string_of(value, "RunState"));
  if (raw == "running") {
    return RunState::running;
  }
  if (raw == "finished") {
    return RunState::finished;
  }
  if (raw == "failed") {
    return RunState::failed;
  }
  if (raw == "stopped") {
    return RunState::stopped;
  }
  return Err{ParseError{"RunState", fmt::format("`{}` is not one of running, "
                                                "finished, failed, stopped",
                                                raw)}};
}

auto parse_run_transition(simdjson::dom::element value)
  -> Result<RunTransition, ParseError> {
  TRY(auto object, api::object_of(value, "RunTransition"));

  TRY(auto pipeline_id_raw, api::field(object, "pipelineId", "RunTransition"));
  TRY(auto pipeline_id, parse_pipeline_id(pipeline_id_raw));

  TRY(auto run_raw, api::field(object, "run", "RunTransition"));
  TRY(auto run, parse_telemetry_count2(run_raw));

  TRY(auto at_raw, api::field(object, "at", "RunTransition"));
  TRY(auto at, api::time_of(at_raw, "RunTransition.at"));

  TRY(auto state_raw, api::field(object, "state", "RunTransition"));
  TRY(auto state, parse_run_state(state_raw));

  TRY(auto error_raw, api::field(object, "error", "RunTransition"));
  auto error = Option<std::string>{None{}};
  if (not api::is_null(error_raw)) {
    TRY(auto error_value, api::string_of(error_raw, "RunTransition.error"));
    error = std::move(error_value);
  }

  return RunTransition{std::move(pipeline_id), std::move(run), at,
                       std::move(state), std::move(error)};
}

auto TenzirTelemetryTelemetrySeconds::make(double value)
  -> Result<TenzirTelemetryTelemetrySeconds, ParseError> {
  return TenzirTelemetryTelemetrySeconds{std::move(value)};
}

auto parse_tenzir_telemetry_telemetry_seconds(simdjson::dom::element value)
  -> Result<TenzirTelemetryTelemetrySeconds, ParseError> {
  TRY(auto raw, api::double_of(value, "TenzirTelemetryTelemetrySeconds"));
  return TenzirTelemetryTelemetrySeconds::make(std::move(raw));
}

auto parse_deployment_sample(simdjson::dom::element value)
  -> Result<DeploymentSample, ParseError> {
  TRY(auto object, api::object_of(value, "DeploymentSample"));

  TRY(auto window_raw, api::field(object, "window", "DeploymentSample"));
  TRY(auto window, api::time_of(window_raw, "DeploymentSample.window"));

  TRY(auto cpu_seconds_raw,
      api::field(object, "cpuSeconds", "DeploymentSample"));
  TRY(auto cpu_seconds,
      parse_tenzir_telemetry_telemetry_seconds(cpu_seconds_raw));

  TRY(auto memory_bytes_raw,
      api::field(object, "memoryBytes", "DeploymentSample"));
  TRY(auto memory_bytes, parse_telemetry_count2(memory_bytes_raw));

  TRY(auto uptime_ms_raw, api::field(object, "uptimeMs", "DeploymentSample"));
  TRY(auto uptime_ms, parse_telemetry_count2(uptime_ms_raw));

  return DeploymentSample{window, std::move(cpu_seconds),
                          std::move(memory_bytes), std::move(uptime_ms)};
}

auto parse_batch(simdjson::dom::element value) -> Result<Batch, ParseError> {
  TRY(auto object, api::object_of(value, "Batch"));

  TRY(auto boot_raw, api::field(object, "boot", "Batch"));
  TRY(auto boot, api::string_of(boot_raw, "Batch.boot"));

  TRY(auto seq_raw, api::field(object, "seq", "Batch"));
  TRY(auto seq, parse_telemetry_count(seq_raw));

  TRY(auto dropped_raw, api::field(object, "dropped", "Batch"));
  TRY(auto dropped, parse_tenzir_telemetry_telemetry_count(dropped_raw));

  TRY(auto pipelines_raw, api::field(object, "pipelines", "Batch"));
  auto pipelines = std::vector<PipelineSample>{};
  auto pipelines_array = simdjson::dom::array{};
  if (pipelines_raw.get_array().get(pipelines_array) != simdjson::SUCCESS) {
    return Err{ParseError{"Batch.pipelines", "expected an array"}};
  }
  for (auto pipelines_item : pipelines_array) {
    TRY(auto pipelines_decoded, parse_pipeline_sample(pipelines_item));
    pipelines.push_back(std::move(pipelines_decoded));
  }

  TRY(auto operators_raw, api::field(object, "operators", "Batch"));
  auto operators = std::vector<OperatorSample>{};
  auto operators_array = simdjson::dom::array{};
  if (operators_raw.get_array().get(operators_array) != simdjson::SUCCESS) {
    return Err{ParseError{"Batch.operators", "expected an array"}};
  }
  for (auto operators_item : operators_array) {
    TRY(auto operators_decoded, parse_operator_sample(operators_item));
    operators.push_back(std::move(operators_decoded));
  }

  TRY(auto diagnostics_raw, api::field(object, "diagnostics", "Batch"));
  auto diagnostics = std::vector<DiagnosticSample>{};
  auto diagnostics_array = simdjson::dom::array{};
  if (diagnostics_raw.get_array().get(diagnostics_array) != simdjson::SUCCESS) {
    return Err{ParseError{"Batch.diagnostics", "expected an array"}};
  }
  for (auto diagnostics_item : diagnostics_array) {
    TRY(auto diagnostics_decoded, parse_diagnostic_sample(diagnostics_item));
    diagnostics.push_back(std::move(diagnostics_decoded));
  }

  TRY(auto runs_raw, api::field(object, "runs", "Batch"));
  auto runs = std::vector<RunTransition>{};
  auto runs_array = simdjson::dom::array{};
  if (runs_raw.get_array().get(runs_array) != simdjson::SUCCESS) {
    return Err{ParseError{"Batch.runs", "expected an array"}};
  }
  for (auto runs_item : runs_array) {
    TRY(auto runs_decoded, parse_run_transition(runs_item));
    runs.push_back(std::move(runs_decoded));
  }

  TRY(auto deployment_raw, api::field(object, "deployment", "Batch"));
  auto deployment = std::vector<DeploymentSample>{};
  auto deployment_array = simdjson::dom::array{};
  if (deployment_raw.get_array().get(deployment_array) != simdjson::SUCCESS) {
    return Err{ParseError{"Batch.deployment", "expected an array"}};
  }
  for (auto deployment_item : deployment_array) {
    TRY(auto deployment_decoded, parse_deployment_sample(deployment_item));
    deployment.push_back(std::move(deployment_decoded));
  }

  return Batch{std::move(boot),      std::move(seq),
               std::move(dropped),   std::move(pipelines),
               std::move(operators), std::move(diagnostics),
               std::move(runs),      std::move(deployment)};
}

auto parse_liveness(simdjson::dom::element value)
  -> Result<Liveness, ParseError> {
  TRY(auto object, api::object_of(value, "Liveness"));

  TRY(auto status_raw, api::field(object, "status", "Liveness"));
  TRY(auto status, api::string_of(status_raw, "Liveness.status"));

  TRY(auto uptime_ms_raw, api::field(object, "uptime_ms", "Liveness"));
  TRY(auto uptime_ms, api::double_of(uptime_ms_raw, "Liveness.uptime_ms"));

  return Liveness{std::move(status), uptime_ms};
}

auto parse_pipeline(simdjson::dom::element value)
  -> Result<Pipeline, ParseError> {
  TRY(auto object, api::object_of(value, "Pipeline"));

  TRY(auto name_raw, api::field(object, "name", "Pipeline"));
  TRY(auto name, api::string_of(name_raw, "Pipeline.name"));

  TRY(auto definition_raw, api::field(object, "definition", "Pipeline"));
  TRY(auto definition, api::string_of(definition_raw, "Pipeline.definition"));

  return Pipeline{std::move(name), std::move(definition)};
}

auto parse_configuration(simdjson::dom::element value)
  -> Result<Configuration, ParseError> {
  TRY(auto object, api::object_of(value, "Configuration"));

  TRY(auto pipelines_raw, api::field(object, "pipelines", "Configuration"));
  auto pipelines = std::unordered_map<PipelineId, Pipeline>{};
  TRY(auto pipelines_object,
      api::object_of(pipelines_raw, "Configuration.pipelines"));
  for (auto pipelines_entry : pipelines_object) {
    TRY(auto pipelines_key, PipelineId::make(std::string{pipelines_entry.key}));
    TRY(auto pipelines_decoded, parse_pipeline(pipelines_entry.value));
    pipelines.insert_or_assign(std::move(pipelines_key),
                               std::move(pipelines_decoded));
  }

  return Configuration{std::move(pipelines)};
}

auto parse_identified_pipeline(simdjson::dom::element value)
  -> Result<IdentifiedPipeline, ParseError> {
  TRY(auto object, api::object_of(value, "IdentifiedPipeline"));

  TRY(auto id_raw, api::field(object, "id", "IdentifiedPipeline"));
  TRY(auto id, parse_pipeline_id(id_raw));

  TRY(auto name_raw, api::field(object, "name", "IdentifiedPipeline"));
  TRY(auto name, api::string_of(name_raw, "IdentifiedPipeline.name"));

  TRY(auto definition_raw,
      api::field(object, "definition", "IdentifiedPipeline"));
  TRY(auto definition,
      api::string_of(definition_raw, "IdentifiedPipeline.definition"));

  return IdentifiedPipeline{std::move(id), std::move(name),
                            std::move(definition)};
}

auto to_string(PipelineState value) -> std::string_view {
  switch (value) {
    case PipelineState::running:
      return "running";
    case PipelineState::finished:
      return "finished";
    case PipelineState::failed:
      return "failed";
    case PipelineState::stopped:
      return "stopped";
  }
  TENZIR_UNREACHABLE();
}

auto parse_pipeline_state(simdjson::dom::element value)
  -> Result<PipelineState, ParseError> {
  TRY(auto raw, api::string_of(value, "PipelineState"));
  if (raw == "running") {
    return PipelineState::running;
  }
  if (raw == "finished") {
    return PipelineState::finished;
  }
  if (raw == "failed") {
    return PipelineState::failed;
  }
  if (raw == "stopped") {
    return PipelineState::stopped;
  }
  return Err{ParseError{
    "PipelineState",
    fmt::format("`{}` is not one of running, finished, failed, stopped", raw)}};
}

auto parse_pipeline_status(simdjson::dom::element value)
  -> Result<PipelineStatus, ParseError> {
  TRY(auto object, api::object_of(value, "PipelineStatus"));

  TRY(auto id_raw, api::field(object, "id", "PipelineStatus"));
  TRY(auto id, parse_pipeline_id(id_raw));

  TRY(auto name_raw, api::field(object, "name", "PipelineStatus"));
  TRY(auto name, api::string_of(name_raw, "PipelineStatus.name"));

  TRY(auto definition_raw, api::field(object, "definition", "PipelineStatus"));
  TRY(auto definition,
      api::string_of(definition_raw, "PipelineStatus.definition"));

  TRY(auto state_raw, api::field(object, "state", "PipelineStatus"));
  TRY(auto state, parse_pipeline_state(state_raw));

  TRY(auto run_raw, api::field(object, "run", "PipelineStatus"));
  TRY(auto run, api::int64_of(run_raw, "PipelineStatus.run"));

  TRY(auto error_raw, api::field(object, "error", "PipelineStatus"));
  auto error = Option<std::string>{None{}};
  if (not api::is_null(error_raw)) {
    TRY(auto error_value, api::string_of(error_raw, "PipelineStatus.error"));
    error = std::move(error_value);
  }

  return PipelineStatus{std::move(id),         std::move(name),
                        std::move(definition), std::move(state),
                        std::move(run),        std::move(error)};
}

auto parse_pipeline_path(simdjson::dom::element value)
  -> Result<PipelinePath, ParseError> {
  TRY(auto object, api::object_of(value, "PipelinePath"));

  TRY(auto pipeline_id_raw, api::field(object, "pipelineId", "PipelinePath"));
  TRY(auto pipeline_id, parse_pipeline_id(pipeline_id_raw));

  return PipelinePath{std::move(pipeline_id)};
}

auto AdhocPipelineId::make(std::string value)
  -> Result<AdhocPipelineId, ParseError> {
  static auto const pattern = std::regex{
    R"(^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$)"};
  if (not std::regex_match(value, pattern)) {
    return Err{
      ParseError{"AdhocPipelineId",
                 fmt::format("`{}` does not match the pattern", value)}};
  }
  return AdhocPipelineId{std::move(value)};
}

auto parse_adhoc_pipeline_id(simdjson::dom::element value)
  -> Result<AdhocPipelineId, ParseError> {
  TRY(auto raw, api::string_of(value, "AdhocPipelineId"));
  return AdhocPipelineId::make(std::move(raw));
}

auto parse_adhoc_pipeline_definition(simdjson::dom::element value)
  -> Result<AdhocPipelineDefinition, ParseError> {
  TRY(auto object, api::object_of(value, "AdhocPipelineDefinition"));

  TRY(auto adhoc_pipeline_id_raw,
      api::field(object, "adhocPipelineId", "AdhocPipelineDefinition"));
  TRY(auto adhoc_pipeline_id, parse_adhoc_pipeline_id(adhoc_pipeline_id_raw));

  TRY(auto definition_raw,
      api::field(object, "definition", "AdhocPipelineDefinition"));
  TRY(auto definition,
      api::string_of(definition_raw, "AdhocPipelineDefinition.definition"));

  return AdhocPipelineDefinition{std::move(adhoc_pipeline_id),
                                 std::move(definition)};
}

// ── Encoding ────────────────────────────────────────────────────────────────

auto write_json(DeploymentId const& value, std::string& out) -> void {
  api::write_string(value.to_string(), out);
}

auto write_json(DeploymentPath const& value, std::string& out) -> void {
  out += '{';
  out += "\"deploymentId\":";
  write_json(value.deployment_id, out);
  out += '}';
}

auto write_json(WorkspaceId const& value, std::string& out) -> void {
  api::write_string(value.to_string(), out);
}

auto write_json(DeploymentInfo const& value, std::string& out) -> void {
  out += '{';
  out += "\"id\":";
  write_json(value.id, out);
  out += ",\"keyDigest\":";
  if (value.key_digest) {
    api::write_string(*value.key_digest, out);
  } else {
    out += "null";
  }
  out += ",\"workspaceId\":";
  write_json(value.workspace_id, out);
  out += ",\"name\":";
  api::write_string(value.name, out);
  out += ",\"version\":";
  if (value.version) {
    api::write_string(*value.version, out);
  } else {
    out += "null";
  }
  out += ",\"lastSeen\":";
  if (value.last_seen) {
    api::write_time(*value.last_seen, out);
  } else {
    out += "null";
  }
  out += '}';
}

auto write_json(TelemetryCount const& value, std::string& out) -> void {
  fmt::format_to(std::back_inserter(out), "{}", value.value());
}

auto write_json(TenzirTelemetryTelemetryCount const& value, std::string& out)
  -> void {
  fmt::format_to(std::back_inserter(out), "{}", value.value());
}

auto write_json(PipelineId const& value, std::string& out) -> void {
  api::write_string(value.to_string(), out);
}

auto write_json(TelemetryCount2 const& value, std::string& out) -> void {
  fmt::format_to(std::back_inserter(out), "{}", value.value());
}

auto write_json(ConnectorFlow const& value, std::string& out) -> void {
  out += '{';
  out += "\"connector\":";
  api::write_string(value.connector, out);
  out += ",\"events\":";
  write_json(value.events, out);
  out += ",\"bytes\":";
  write_json(value.bytes, out);
  out += '}';
}

auto write_json(PipelineSample const& value, std::string& out) -> void {
  out += '{';
  out += "\"pipelineId\":";
  write_json(value.pipeline_id, out);
  out += ",\"run\":";
  write_json(value.run, out);
  out += ",\"window\":";
  api::write_time(value.window, out);
  out += ",\"sources\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.sources) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += ",\"sinks\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.sinks) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += '}';
}

auto write_json(Flow const& value, std::string& out) -> void {
  out += '{';
  out += "\"events\":";
  write_json(value.events, out);
  out += ",\"bytes\":";
  write_json(value.bytes, out);
  out += '}';
}

auto write_json(TelemetrySeconds const& value, std::string& out) -> void {
  fmt::format_to(std::back_inserter(out), "{}", value.value());
}

auto write_json(TelemetryCount3 const& value, std::string& out) -> void {
  fmt::format_to(std::back_inserter(out), "{}", value.value());
}

auto write_json(TelemetrySmallCount const& value, std::string& out) -> void {
  fmt::format_to(std::back_inserter(out), "{}", value.value());
}

auto write_json(OperatorSample const& value, std::string& out) -> void {
  out += '{';
  out += "\"pipelineId\":";
  write_json(value.pipeline_id, out);
  out += ",\"run\":";
  write_json(value.run, out);
  out += ",\"operatorId\":";
  api::write_string(value.operator_id, out);
  out += ",\"name\":";
  api::write_string(value.name, out);
  out += ",\"window\":";
  api::write_time(value.window, out);
  out += ",\"input\":";
  write_json(value.input, out);
  out += ",\"output\":";
  write_json(value.output, out);
  out += ",\"cpuSeconds\":";
  write_json(value.cpu_seconds, out);
  out += ",\"inputBytes\":";
  write_json(value.input_bytes, out);
  out += ",\"inputCapacity\":";
  write_json(value.input_capacity, out);
  out += ",\"tasks\":";
  write_json(value.tasks, out);
  out += '}';
}

auto write_json(TenzirTelemetryTelemetrySmallCount const& value,
                std::string& out) -> void {
  fmt::format_to(std::back_inserter(out), "{}", value.value());
}

auto write_json(Severity const& value, std::string& out) -> void {
  api::write_string(to_string(value), out);
}

auto write_json(PushTelemetryPayloadDiagnosticsNotes const& value,
                std::string& out) -> void {
  out += '{';
  out += "\"kind\":";
  api::write_string(value.kind, out);
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += '}';
}

auto write_json(PushTelemetryPayloadDiagnosticsAnnotations const& value,
                std::string& out) -> void {
  out += '{';
  out += "\"primary\":";
  out += value.primary ? "true" : "false";
  out += ",\"text\":";
  api::write_string(value.text, out);
  out += ",\"begin\":";
  write_json(value.begin, out);
  out += ",\"end\":";
  write_json(value.end, out);
  out += '}';
}

auto write_json(DiagnosticSample const& value, std::string& out) -> void {
  out += '{';
  out += "\"pipelineId\":";
  write_json(value.pipeline_id, out);
  out += ",\"run\":";
  write_json(value.run, out);
  out += ",\"firstSeen\":";
  api::write_time(value.first_seen, out);
  out += ",\"lastSeen\":";
  api::write_time(value.last_seen, out);
  out += ",\"occurrences\":";
  write_json(value.occurrences, out);
  out += ",\"fingerprint\":";
  api::write_string(value.fingerprint, out);
  out += ",\"severity\":";
  write_json(value.severity, out);
  out += ",\"message\":";
  api::write_string(value.message, out);
  out += ",\"rendered\":";
  api::write_string(value.rendered, out);
  out += ",\"notes\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.notes) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += ",\"annotations\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.annotations) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += '}';
}

auto write_json(RunState const& value, std::string& out) -> void {
  api::write_string(to_string(value), out);
}

auto write_json(RunTransition const& value, std::string& out) -> void {
  out += '{';
  out += "\"pipelineId\":";
  write_json(value.pipeline_id, out);
  out += ",\"run\":";
  write_json(value.run, out);
  out += ",\"at\":";
  api::write_time(value.at, out);
  out += ",\"state\":";
  write_json(value.state, out);
  out += ",\"error\":";
  if (value.error) {
    api::write_string(*value.error, out);
  } else {
    out += "null";
  }
  out += '}';
}

auto write_json(TenzirTelemetryTelemetrySeconds const& value, std::string& out)
  -> void {
  fmt::format_to(std::back_inserter(out), "{}", value.value());
}

auto write_json(DeploymentSample const& value, std::string& out) -> void {
  out += '{';
  out += "\"window\":";
  api::write_time(value.window, out);
  out += ",\"cpuSeconds\":";
  write_json(value.cpu_seconds, out);
  out += ",\"memoryBytes\":";
  write_json(value.memory_bytes, out);
  out += ",\"uptimeMs\":";
  write_json(value.uptime_ms, out);
  out += '}';
}

auto write_json(Batch const& value, std::string& out) -> void {
  out += '{';
  out += "\"boot\":";
  api::write_string(value.boot, out);
  out += ",\"seq\":";
  write_json(value.seq, out);
  out += ",\"dropped\":";
  write_json(value.dropped, out);
  out += ",\"pipelines\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.pipelines) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += ",\"operators\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.operators) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += ",\"diagnostics\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.diagnostics) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += ",\"runs\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.runs) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += ",\"deployment\":";
  {
    out += '[';
    auto separator0 = "";
    for (auto const& element0 : value.deployment) {
      out += separator0;
      separator0 = ",";
      write_json(element0, out);
    }
    out += ']';
  }
  out += '}';
}

auto write_json(Liveness const& value, std::string& out) -> void {
  out += '{';
  out += "\"status\":";
  api::write_string(value.status, out);
  out += ",\"uptime_ms\":";
  fmt::format_to(std::back_inserter(out), "{}", value.uptime_ms);
  out += '}';
}

auto write_json(Pipeline const& value, std::string& out) -> void {
  out += '{';
  out += "\"name\":";
  api::write_string(value.name, out);
  out += ",\"definition\":";
  api::write_string(value.definition, out);
  out += '}';
}

auto write_json(Configuration const& value, std::string& out) -> void {
  out += '{';
  out += "\"pipelines\":";
  {
    out += '{';
    auto separator0 = "";
    for (auto const& entry0 : value.pipelines) {
      out += separator0;
      separator0 = ",";
      api::write_string(entry0.first.to_string(), out);
      out += ':';
      write_json(entry0.second, out);
    }
    out += '}';
  }
  out += '}';
}

auto write_json(IdentifiedPipeline const& value, std::string& out) -> void {
  out += '{';
  out += "\"id\":";
  write_json(value.id, out);
  out += ",\"name\":";
  api::write_string(value.name, out);
  out += ",\"definition\":";
  api::write_string(value.definition, out);
  out += '}';
}

auto write_json(PipelineState const& value, std::string& out) -> void {
  api::write_string(to_string(value), out);
}

auto write_json(PipelineStatus const& value, std::string& out) -> void {
  out += '{';
  out += "\"id\":";
  write_json(value.id, out);
  out += ",\"name\":";
  api::write_string(value.name, out);
  out += ",\"definition\":";
  api::write_string(value.definition, out);
  out += ",\"state\":";
  write_json(value.state, out);
  out += ",\"run\":";
  fmt::format_to(std::back_inserter(out), "{}", value.run);
  out += ",\"error\":";
  if (value.error) {
    api::write_string(*value.error, out);
  } else {
    out += "null";
  }
  out += '}';
}

auto write_json(PipelinePath const& value, std::string& out) -> void {
  out += '{';
  out += "\"pipelineId\":";
  write_json(value.pipeline_id, out);
  out += '}';
}

auto write_json(AdhocPipelineId const& value, std::string& out) -> void {
  api::write_string(value.to_string(), out);
}

auto write_json(AdhocPipelineDefinition const& value, std::string& out)
  -> void {
  out += '{';
  out += "\"adhocPipelineId\":";
  write_json(value.adhoc_pipeline_id, out);
  out += ",\"definition\":";
  api::write_string(value.definition, out);
  out += '}';
}

// ── Deployments ─────────────────────────────────────────────────────────────

auto call(Transport& transport, ReadDeployment request)
  -> Task<Result<DeploymentInfo, variant<ReadDeploymentError, CallError>>> {
  auto path = fmt::format("/api/deployments/{}",
                          request.path.deployment_id.to_string());
  auto body = std::string{};
  CO_TRY(auto response, (co_await transport.request(HttpRequest{
                           "GET", std::move(path), std::move(body)}))
                          .map_err(as_call_error));
  if (response.status != 200) {
    if (response.status == 500) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "InternalServerError")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_internal_server_error(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 404) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "NotFound")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_not_found(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 403) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "Unauthorized")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_unauthorized(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 401) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "Unauthenticated")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_unauthenticated(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 400) {
      auto tag = api::tag_of(response.body);
      if (tag == "CredentialConflict") {
        CO_TRY(auto document,
               api::Document::parse(response.body, "CredentialConflict")
                 .map_err(as_response_error(response.status, response.body)));
        CO_TRY(auto error,
               parse_credential_conflict(document.root())
                 .map_err(as_response_error(response.status, response.body)));
        co_return Err{std::move(error)};
      }
      if (tag == "Validation") {
        CO_TRY(auto document,
               api::Document::parse(response.body, "Validation")
                 .map_err(as_response_error(response.status, response.body)));
        CO_TRY(auto error,
               parse_validation(document.root())
                 .map_err(as_response_error(response.status, response.body)));
        co_return Err{std::move(error)};
      }
      co_return Err{
        CallError{ResponseError{ResponseError::Reason::decode, response.status,
                                std::move(response.body)}}};
    }
    co_return Err{
      CallError{ResponseError{ResponseError::Reason::decode, response.status,
                              std::move(response.body)}}};
  }
  CO_TRY(auto document, api::Document::parse(response.body, "readDeployment")
                          .map_err(as_parse_error));
  CO_TRY(auto value,
         parse_deployment_info(document.root()).map_err(as_parse_error));
  co_return value;
}

// ── Telemetry ───────────────────────────────────────────────────────────────

auto call(Transport& transport, PushTelemetry request)
  -> Task<Result<void, variant<PushTelemetryError, CallError>>> {
  auto path = fmt::format("/api/deployments/{}/telemetry",
                          request.path.deployment_id.to_string());
  auto body = std::string{};
  write_json(request.payload, body);
  CO_TRY(auto response, (co_await transport.request(HttpRequest{
                           "POST", std::move(path), std::move(body)}))
                          .map_err(as_call_error));
  if (response.status != 204) {
    if (response.status == 413) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "TooLarge")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_too_large(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 500) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "InternalServerError")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_internal_server_error(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 404) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "NotFound")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_not_found(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 503) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "ServiceUnavailable")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_service_unavailable(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 403) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "Unauthorized")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_unauthorized(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 401) {
      CO_TRY(auto failed,
             api::Document::parse(response.body, "Unauthenticated")
               .map_err(as_response_error(response.status, response.body)));
      CO_TRY(auto error,
             parse_unauthenticated(failed.root())
               .map_err(as_response_error(response.status, response.body)));
      co_return Err{std::move(error)};
    }
    if (response.status == 400) {
      auto tag = api::tag_of(response.body);
      if (tag == "CredentialConflict") {
        CO_TRY(auto document,
               api::Document::parse(response.body, "CredentialConflict")
                 .map_err(as_response_error(response.status, response.body)));
        CO_TRY(auto error,
               parse_credential_conflict(document.root())
                 .map_err(as_response_error(response.status, response.body)));
        co_return Err{std::move(error)};
      }
      if (tag == "Validation") {
        CO_TRY(auto document,
               api::Document::parse(response.body, "Validation")
                 .map_err(as_response_error(response.status, response.body)));
        CO_TRY(auto error,
               parse_validation(document.root())
                 .map_err(as_response_error(response.status, response.body)));
        co_return Err{std::move(error)};
      }
      co_return Err{
        CallError{ResponseError{ResponseError::Reason::decode, response.status,
                                std::move(response.body)}}};
    }
    co_return Err{
      CallError{ResponseError{ResponseError::Reason::decode, response.status,
                              std::move(response.body)}}};
  }
  co_return {};
}

// ── Deployment ──────────────────────────────────────────────────────────────

auto dispatch(DeploymentApi& handler, HttpRequest request)
  -> Task<HttpResponse> {
  auto parameters = std::vector<std::string_view>{};
  if (api::match_path("/health", request.path, parameters)) {
    if (request.method == "GET") {
      auto decoded = ReadHealth{};
      co_return api::encode<200>(co_await handler.handle(std::move(decoded)));
    }
    co_return HttpResponse{405,
                           R"({"error":"method not allowed","allow":["GET"]})"};
  }
  if (api::match_path("/configuration", request.path, parameters)) {
    if (request.method == "PUT") {
      auto parsed = api::Document::parse(request.body, "updateConfiguration");
      if (parsed.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(parsed).unwrap_err());
      }
      auto document = std::move(parsed).unwrap();
      auto payload = parse_configuration(document.root());
      if (payload.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(payload).unwrap_err());
      }
      auto decoded = UpdateConfiguration{std::move(payload).unwrap()};
      co_await handler.handle(std::move(decoded));
      co_return api::encode<204>();
    }
    co_return HttpResponse{405,
                           R"({"error":"method not allowed","allow":["PUT"]})"};
  }
  if (api::match_path("/pipelines", request.path, parameters)) {
    if (request.method == "POST") {
      auto parsed = api::Document::parse(request.body, "createPipeline");
      if (parsed.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(parsed).unwrap_err());
      }
      auto document = std::move(parsed).unwrap();
      auto payload = parse_identified_pipeline(document.root());
      if (payload.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(payload).unwrap_err());
      }
      auto decoded = CreatePipeline{std::move(payload).unwrap()};
      co_return api::encode<204>(co_await handler.handle(std::move(decoded)));
    }
    if (request.method == "GET") {
      auto decoded = ListPipelines{};
      co_return api::encode<200>(co_await handler.handle(std::move(decoded)));
    }
    co_return HttpResponse{
      405, R"({"error":"method not allowed","allow":["POST","GET"]})"};
  }
  if (api::match_path("/pipelines/:pipelineId/start", request.path,
                      parameters)) {
    if (request.method == "POST") {
      auto pipeline_id = PipelineId::make(std::string{parameters[0]});
      if (pipeline_id.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(pipeline_id).unwrap_err());
      }
      auto path = PipelinePath{std::move(pipeline_id).unwrap()};
      auto decoded = StartPipeline{std::move(path)};
      co_return api::encode<204>(co_await handler.handle(std::move(decoded)));
    }
    co_return HttpResponse{
      405, R"({"error":"method not allowed","allow":["POST"]})"};
  }
  if (api::match_path("/pipelines/:pipelineId/stop", request.path,
                      parameters)) {
    if (request.method == "POST") {
      auto pipeline_id = PipelineId::make(std::string{parameters[0]});
      if (pipeline_id.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(pipeline_id).unwrap_err());
      }
      auto path = PipelinePath{std::move(pipeline_id).unwrap()};
      auto decoded = StopPipeline{std::move(path)};
      co_return api::encode<204>(co_await handler.handle(std::move(decoded)));
    }
    co_return HttpResponse{
      405, R"({"error":"method not allowed","allow":["POST"]})"};
  }
  if (api::match_path("/pipelines/:pipelineId", request.path, parameters)) {
    if (request.method == "DELETE") {
      auto pipeline_id = PipelineId::make(std::string{parameters[0]});
      if (pipeline_id.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(pipeline_id).unwrap_err());
      }
      auto path = PipelinePath{std::move(pipeline_id).unwrap()};
      auto decoded = DeletePipeline{std::move(path)};
      co_await handler.handle(std::move(decoded));
      co_return api::encode<204>();
    }
    co_return HttpResponse{
      405, R"({"error":"method not allowed","allow":["DELETE"]})"};
  }
  if (api::match_path("/adhoc-pipelines", request.path, parameters)) {
    if (request.method == "POST") {
      auto parsed = api::Document::parse(request.body, "startAdhocPipeline");
      if (parsed.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(parsed).unwrap_err());
      }
      auto document = std::move(parsed).unwrap();
      auto payload = parse_adhoc_pipeline_definition(document.root());
      if (payload.is_err()) {
        co_return api::as_request_failure(api::bad_request)(
          std::move(payload).unwrap_err());
      }
      auto decoded = StartAdhocPipeline{std::move(payload).unwrap()};
      co_return api::encode<204>(co_await handler.handle(std::move(decoded)));
    }
    co_return HttpResponse{
      405, R"({"error":"method not allowed","allow":["POST"]})"};
  }
  co_return HttpResponse{404, R"({"error":"no such route"})"};
}

} // namespace tenzir
