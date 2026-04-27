//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/data.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/uuid.hpp>
#include <tenzir/view.hpp>

#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "velociraptor.pb.h"

namespace tenzir::plugins::velociraptor {

/// The ID of an Organization.
constexpr auto default_org_id = "root";

/// The maximum number of rows per response.
constexpr auto default_max_rows = uint64_t{1'000};

/// The number of seconds to wait on responses.
constexpr auto default_max_wait = std::chrono::seconds{1};

/// A VQL request.
struct request {
  std::string name;
  std::string vql;

  friend auto inspect(auto& f, request& x) -> bool {
    return f.object(x).pretty_name("request").fields(f.field("name", x.name),
                                                     f.field("vql", x.vql));
  }
};

/// The arguments passed to the operator.
struct operator_args {
  uint64_t max_rows;
  std::chrono::seconds max_wait;
  std::string org_id;
  std::vector<request> requests;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("max_rows", x.max_rows), f.field("max_wait", x.max_wait),
              f.field("org_id", x.org_id), f.field("requests", x.requests));
  }
};

struct GrpcClientConfig {
  std::string ca_certificate;
  std::string client_private_key;
  std::string client_cert;
  std::string api_connection_string;
};

/// Christoph Lobmeyer (https://github.com/lo-chr) devised this query and
/// provided the use case to subscribe to a specific set of artifacts from
/// multiple clients.
constexpr auto subscribe_artifact_vql = R"__(
LET subscribe_artifact = "{}"

LET completions = SELECT *
                  FROM watch_monitoring(artifact="System.Flow.Completion")
                  WHERE Flow.artifacts_with_results =~ subscribe_artifact

SELECT *
FROM foreach(
  row=completions,
  query={{
     SELECT *
     FROM foreach(
       row=Flow.artifacts_with_results,
       query={{
         SELECT *
         FROM if(
          condition=(_value =~ subscribe_artifact),
          then={{
             SELECT
               {{
                 SELECT *
                 FROM source(
                   client_id=ClientId,
                   flow_id=Flow.session_id,
                   artifact=_value)
               }} AS HuntResult,
               _value AS Artifact,
               client_info(client_id=ClientId).os_info.hostname AS Hostname,
               timestamp(epoch=now()) AS timestamp,
               ClientId,
               Flow.session_id AS FlowId
             FROM source(
               client_id=ClientId,
               flow_id=Flow.session_id,
               artifact=_value)
             GROUP BY
               artifact
          }})
        }})
  }})
)__";

inline auto make_subscribe_query(std::string_view artifact) -> std::string {
  return fmt::format(subscribe_artifact_vql, artifact);
}

/// Parses a response as table slice.
inline auto parse(const proto::VQLResponse& response)
  -> caf::expected<std::vector<table_slice>> {
  auto builder = series_builder{};
  auto us = std::chrono::microseconds(response.timestamp());
  auto timestamp = time{std::chrono::duration_cast<duration>(us)};
  // Velociraptor sends a stream of responses that consists of "control"
  // and "data" messages. If the response payload is empty, then we have a
  // control message, otherwise we have a data message.
  if (not response.response().empty()) {
    TENZIR_DEBUG("got a data message");
    // There's an opportunity for improvement here, as we are not (yet)
    // making use of the additional types provided in the response. We
    // should synthesize a schema from that and provide that as hint to
    // the series builder.
    auto json = from_json(response.response());
    if (not json) {
      return caf::make_error(ec::parse_error,
                             "Velociraptor response not in JSON format");
    }
    const auto* objects = try_as<list>(&*json);
    if (objects == nullptr) {
      return caf::make_error(ec::parse_error,
                             "expected JSON array in Velociraptor response");
    }
    for (const auto& object : *objects) {
      const auto* rec = try_as<record>(&object);
      if (rec == nullptr) {
        return caf::make_error(ec::parse_error,
                               "expected objects in Velociraptor response");
      }
      auto row = builder.record();
      row.field("timestamp").data(timestamp);
      row.field("query_id").data(response.query_id());
      row.field("query").data(record{
        {"name", response.query().name()},
        {"vql", response.query().vql()},
      });
      row.field("part").data(response.part());
      auto resp = row.field("response").record();
      for (const auto& [field, value] : *rec) {
        resp.field(field).data(make_view(value));
      }
    }
    return builder.finish_as_table_slice("velociraptor.response");
  }
  if (not response.log().empty()) {
    TENZIR_DEBUG("got a control message");
    auto row = builder.record();
    row.field("timestamp").data(timestamp);
    row.field("log").data(response.log());
    return builder.finish_as_table_slice("velociraptor.log");
  }
  return caf::make_error(ec::unspecified, "empty Velociraptor response");
}

inline auto emit_parse_warning(proto::VQLResponse const& response,
                               diagnostic_handler& dh, caf::error const& error,
                               location const& operator_location
                               = location::unknown) -> void {
  diagnostic::warning("failed to parse Velociraptor gRPC response")
    .primary(operator_location)
    .note("{}", error)
    .note("response: '{}'", response.response())
    .note("query_id: '{}'", response.query_id())
    .note("part: '{}'", response.part())
    .note("query name: '{}'", response.query().name())
    .note("query VQL: '{}'", response.query().vql())
    .note("timestamp: '{}'", response.timestamp())
    .note("total_rows: '{}'", response.total_rows())
    .note("log: '{}'", response.log())
    .emit(dh);
}

inline auto make_collector_args(operator_args const& args)
  -> proto::VQLCollectorArgs {
  auto result = proto::VQLCollectorArgs{};
  for (auto const& request : args.requests) {
    auto* query = result.add_query();
    query->set_name(request.name);
    query->set_vql(request.vql);
  }
  result.set_max_row(args.max_rows);
  result.set_max_wait(args.max_wait.count());
  result.set_org_id(args.org_id);
  return result;
}

inline auto available_profiles(record const& config)
  -> std::vector<std::string> {
  auto const* profiles = get_if<record>(&config, "profiles");
  if (not profiles) {
    return {};
  }
  auto result = std::vector<std::string>{};
  result.reserve(profiles->size());
  for (auto const& [key, _] : *profiles) {
    result.push_back(key);
  }
  return result;
}

inline auto
make_client_config(record const& config, diagnostic_handler& dh,
                   location const& operator_location = location::unknown)
  -> failure_or<GrpcClientConfig> {
  auto const* ca_certificate = get_if<std::string>(&config, "ca_certificate");
  if (ca_certificate == nullptr) {
    diagnostic::error("no 'ca_certificate' found in config file")
      .primary(operator_location)
      .emit(dh);
    return failure::promise();
  }
  auto const* client_private_key
    = get_if<std::string>(&config, "client_private_key");
  if (client_private_key == nullptr) {
    diagnostic::error("no 'client_private_key' found in config file")
      .primary(operator_location)
      .emit(dh);
    return failure::promise();
  }
  auto const* client_cert = get_if<std::string>(&config, "client_cert");
  if (client_cert == nullptr) {
    diagnostic::error("no 'client_cert' found in config file")
      .primary(operator_location)
      .emit(dh);
    return failure::promise();
  }
  auto const* api_connection_string
    = get_if<std::string>(&config, "api_connection_string");
  if (api_connection_string == nullptr) {
    diagnostic::error("no 'api_connection_string' found in config file")
      .primary(operator_location)
      .emit(dh);
    return failure::promise();
  }
  return GrpcClientConfig{
    .ca_certificate = *ca_certificate,
    .client_private_key = *client_private_key,
    .client_cert = *client_cert,
    .api_connection_string = *api_connection_string,
  };
}

} // namespace tenzir::plugins::velociraptor
