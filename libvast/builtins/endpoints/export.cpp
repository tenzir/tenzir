//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/json.hpp"
#include "vast/system/make_pipelines.hpp"

#include <vast/command.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/numeric.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/actors.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/parse_query.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/table_slice.hpp>

#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::export_ {

static auto const* SPEC_V0 = R"_(
/export:
  get:
    summary: Export data
    description: Export data from VAST according to a query. The query must be a valid expression in the VAST Query Language. (see https://vast.io/docs/understand/query-language)
    parameters:
      - in: query
        name: expression
        schema:
          type: string
          default: A query matching every event.
        required: true
        description: The query expression to execute.
        example: ":ip in 10.42.0.0/16"
      - in: query
        name: limit
        schema:
          type: int64
          default: 50
        required: false
        description: Maximum number of returned events.
        example: 3
      - in: query
        name: pipeline
        schema:
          type: object
          properties:
            steps:
              type: array
              items:
                type: object
        required: false
        description: A JSON description of a pipeline to be applied to the exported data.
      - in: query
        name: flatten
        schema:
          type: boolean
          default: false
        required: false
        description: Flatten nested elements in the response data.
        example: false
      - in: query
        name: omit-nulls
        schema:
          type: boolean
          default: false
        required: false
        description: Omit null elements in the response data.
        example: false
      - in: query
        name: numeric-durations
        schema:
          type: boolean
          default: false
        required: false
        description: Render durations as numeric values.
        example: false
    responses:
      200:
        description: The result data.
        content:
          application/json:
            schema:
                type: object
                properties:
                  num_events:
                    type: int64
                  version:
                    type: string
                  events:
                    type: array
                    items:
                      type: object
                example:
                  version: v2.3.0-169-ge42a9652e5-dirty
                  num_events: 3
                  events:
                    - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"flow_id\": 929669869939483, \"pcap_cnt\": null, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"dest_ip\": \"147.32.84.255\", \"dest_port\": 138, \"proto\": \"UDP\", \"event_type\": \"netflow\", \"community_id\": null, \"netflow\": {\"pkts\": 2, \"bytes\": 486, \"start\": \"2011-08-12T12:53:47.928539\", \"end\": \"2011-08-12T12:53:47.928552\", \"age\": 0}, \"app_proto\": \"failed\"}"
                    - "{\"timestamp\": \"2011-08-12T13:00:36.378914\", \"flow_id\": 269421754201300, \"pcap_cnt\": 22569, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 1027, \"dest_ip\": \"74.125.232.202\", \"dest_port\": 80, \"proto\": \"TCP\", \"event_type\": \"http\", \"community_id\": null, \"http\": {\"hostname\": \"cr-tools.clients.google.com\", \"url\": \"/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202\", \"http_port\": null, \"http_user_agent\": \"Google Update/1.3.21.65;winhttp\", \"http_content_type\": null, \"http_method\": \"GET\", \"http_refer\": null, \"protocol\": \"HTTP/1.1\", \"status\": null, \"redirect\": null, \"length\": 0}, \"tx_id\": 0}"
                    - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"flow_id\": 929669869939483, \"pcap_cnt\": null, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"dest_ip\": \"147.32.84.255\", \"dest_port\": 138, \"proto\": \"UDP\", \"event_type\": \"netflow\", \"community_id\": null, \"netflow\": {\"pkts\": 2, \"bytes\": 486, \"start\": \"2011-08-12T12:53:47.928539\", \"end\": \"2011-08-12T12:53:47.928552\", \"age\": 0}, \"app_proto\": \"failed\"}"
      401:
        description: Not authenticated.
      422:
        description: Invalid query string or invalid limit.

  post:
    summary: Export data
    description: Export data from VAST according to a query. The query must be a valid expression in the VAST Query Language. (see https://vast.io/docs/understand/query-language)
    requestBody:
      description: Request parameters
      required: false
      content:
        application/json:
          schema:
            type: object
            required: ["expression"]
            properties:
              expression:
                type: string
                description: The query expression to execute.
                example: ":ip in 10.42.0.0/16"
                default: A query matching every event.
              limit:
                type: int64
                default: 50
                description: Maximum number of returned events
                example: 3
              pipeline:
                type: object
                properties:
                  steps:
                    type: array
                    items:
                      type: object
                description: A JSON object describing a pipeline to be applied on the exported data.
              omit-nulls:
                type: boolean
                description: Omit null elements in the response data.
                default: false
                example: false
              numeric-durations:
                type: boolean
                default: false
                description: Render durations as numeric values.
                example: false
              flatten:
                type: boolean
                default: true
                description: Flatten nested elements in the response data.
                example: false
    responses:
      200:
        description: The result data.
        content:
          application/json:
            schema:
                type: object
                properties:
                  num_events:
                    type: int64
                  version:
                    type: string
                  events:
                    type: array
                    items:
                      type: object
                example:
                  version: v2.3.0-169-ge42a9652e5-dirty
                  events:
                    - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"flow_id\": 929669869939483, \"pcap_cnt\": null, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"dest_ip\": \"147.32.84.255\", \"dest_port\": 138, \"proto\": \"UDP\", \"event_type\": \"netflow\", \"community_id\": null, \"netflow\": {\"pkts\": 2, \"bytes\": 486, \"start\": \"2011-08-12T12:53:47.928539\", \"end\": \"2011-08-12T12:53:47.928552\", \"age\": 0}, \"app_proto\": \"failed\"}"
                    - "{\"timestamp\": \"2011-08-12T13:00:36.378914\", \"flow_id\": 269421754201300, \"pcap_cnt\": 22569, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 1027, \"dest_ip\": \"74.125.232.202\", \"dest_port\": 80, \"proto\": \"TCP\", \"event_type\": \"http\", \"community_id\": null, \"http\": {\"hostname\": \"cr-tools.clients.google.com\", \"url\": \"/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202\", \"http_port\": null, \"http_user_agent\": \"Google Update/1.3.21.65;winhttp\", \"http_content_type\": null, \"http_method\": \"GET\", \"http_refer\": null, \"protocol\": \"HTTP/1.1\", \"status\": null, \"redirect\": null, \"length\": 0}, \"tx_id\": 0}"
                    - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"flow_id\": 929669869939483, \"pcap_cnt\": null, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"dest_ip\": \"147.32.84.255\", \"dest_port\": 138, \"proto\": \"UDP\", \"event_type\": \"netflow\", \"community_id\": null, \"netflow\": {\"pkts\": 2, \"bytes\": 486, \"start\": \"2011-08-12T12:53:47.928539\", \"end\": \"2011-08-12T12:53:47.928552\", \"age\": 0}, \"app_proto\": \"failed\"}"
                  num_events: 3
      401:
        description: Not authenticated.
      422:
        description: Invalid query string or invalid limit.

/export/with-schemas:
  post:
    summary: Export data with schema information
    description: >
      Export data from VAST according to a query.
      The query must be a valid expression in the VAST Query Language. (see https://vast.io/docs/understand/query-language)
      The data is returned grouped by schema.
    requestBody:
      description: Request parameters
      required: false
      content:
        application/json:
          schema:
            type: object
            required: ["expression"]
            properties:
              expression:
                type: string
                description: The query expression to execute.
                example: ":ip in 10.42.0.0/16"
                default: A query matching every event.
              limit:
                type: int64
                default: 50
                description: Maximum number of returned events
                example: 3
              pipeline:
                type: object
                required: ["steps"]
                properties:
                  steps:
                    type: array
                    items:
                      type: object
                description: A JSON object describing a pipeline to be applied on the exported data.
              omit-nulls:
                type: boolean
                description: Omit null elements in the response data.
                default: false
                example: false
              numeric-durations:
                type: boolean
                default: false
                description: Render durations as numeric values.
                example: false
              flatten:
                type: boolean
                default: true
                description: Flatten nested elements in the response data.
                example: false
    responses:
      200:
        description: The result data.
        content:
          application/json:
            schema:
                type: object
                properties:
                  num_events:
                    type: int64
                  version:
                    type: string
                  events:
                    type: array
                    items:
                      type: object
                      properties:
                        name:
                          type: string
                        schema:
                          type: array
                          items:
                            type: object
                            properties:
                              name:
                                type: string
                              type:
                                type: string
                        data:
                          type: array
                          items:
                            type: object

                example:
                  version: v2.3.0-169-ge42a9652e5-dirty
                  num_events: 3
                  events:
                    - name: "suricata.netflow"
                      schema:
                        - { "name": "timestamp", "type": "timestamp" }
                        - { "name": "pcap_cnt", "type": "count" }
                        - { "name": "src_ip", "type": "addr" }
                        - { "name": "src_port", "type": "count" }
                        - { "name": "pkts", "type": "count" }
                        - { "name": "bytes", "type": "count" }
                        - { "name": "action", "type": "enum {allowed: 0, blocked: 1}"}
                      data:
                        - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"pcap_cnt\": null,  \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"netflow.pkts\": 2, \"netflow.bytes\": 486, \"alert.action\": \"allowed\"}"
      401:
        description: Not authenticated.
      422:
        description: Invalid query string or invalid limit.
    )_";

/// The EXPORT_HELPER handles a single query request.
using export_helper_actor = system::typed_actor_fwd<
  // Receives an `atom::done` from the index after each batch of table slices.
  auto(atom::done)->caf::result<void>>
  // Receives table slices from the index.
  ::extend_with<system::receiver_actor<table_slice>>::unwrap;

/// The EXPORT_MULTIPLEXER receives requests against the rest api
/// and spawns export helper actors as needed.
using export_multiplexer_actor = system::typed_actor_fwd<>
  // Provide the REST HANDLER actor interface.
  ::extend_with<system::rest_handler_actor>::unwrap;

struct export_format_options {
  bool typed_results = false;
  bool flatten = defaults::rest::export_::flatten;
  bool numeric_durations = defaults::rest::export_::numeric_durations;
  bool omit_nulls = defaults::rest::export_::omit_nulls;
};

struct export_parameters {
  vast::expression expr = {};
  size_t limit = defaults::rest::export_::limit;
  export_format_options format_opts = {};
};

struct export_helper_state {
  export_helper_state() = default;

  system::index_actor index_ = {};
  export_parameters params_ = {};
  size_t events_ = 0;
  size_t limit_ = std::string::npos;
  std::optional<pipeline_executor> pipeline_ = {};
  std::optional<system::query_cursor> cursor_ = std::nullopt;
  std::vector<table_slice> results_ = {};
  http_request request_;
};

/// Format a set of table slices like this:
// clang-format off
//
//  {
//    "version": "v2.4-rc2",
//    "num_events": 3,
//    "events": [
//      {"_path": "snmp", "_write_ts": "2020-04-01T16:24:33.525023", "ts": "2020-04-01T16:19:33.529926", "uid": "C8Z7zO3pFoxOiC4yj9", "id.orig_h": "104.206.128.30", "id.orig_p": 63509, "id.resp_h": "141.9.71.231", "id.resp_p": 161, "duration": "0.0ns", "version": "1", "community": "public", "get_requests": 1, "get_bulk_requests": 0, "get_responses": 0, "set_requests": 0, "display_string": null, "up_since": null}
//      {"_path": "snmp", "_write_ts": "2020-04-01T16:24:33.525023", "ts": "2020-04-01T16:19:33.529926", "uid": "C8Z7zO3pFoxOiC4yj9", "id.orig_h": "104.206.128.30", "id.orig_p": 63509, "id.resp_h": "141.9.71.231", "id.resp_p": 161, "duration": "0.0ns", "version": "1", "community": "public", "get_requests": 1, "get_bulk_requests": 0, "get_responses": 0, "set_requests": 0, "display_string": null, "up_since": null}
//      {"_path": "snmp", "_write_ts": "2020-04-01T16:24:33.525023", "ts": "2020-04-01T16:19:33.529926", "uid": "C8Z7zO3pFoxOiC4yj9", "id.orig_h": "104.206.128.30", "id.orig_p": 63509, "id.resp_h": "141.9.71.231", "id.resp_p": 161, "duration": "0.0ns", "version": "1", "community": "public", "get_requests": 1, "get_bulk_requests": 0, "get_responses": 0, "set_requests": 0, "display_string": null, "up_since": null}
//      [...]
//    ]
//  }
//
// clang-format on
std::string format_result_flat(const std::vector<table_slice>& slices,
                               const caf::settings& formatting_options) {
  auto num_events = size_t{0ull};
  auto ostream = std::make_unique<std::stringstream>();
  auto writer
    = vast::format::json::writer{std::move(ostream), formatting_options};
  for (auto const& slice : slices) {
    num_events += slice.rows();
    if (auto error = writer.write(slice))
      VAST_WARN("json writer failed to write table slice: {}", error);
  }
  auto data = static_cast<std::stringstream&>(writer.out()).str();
  // Remove line breaks since writer output is NDJSON, except for the
  // last since JSON doesn't support trailing commas in arrays.
  std::replace(data.begin(), data.end(), '\n', ',');
  if (!data.empty())
    data.back() = ' ';
  return fmt::format("{{\"version\": \"{}\",\n \"num_events\": {},\n "
                     "\"events\": "
                     "[{}] }}",
                     vast::version::version, num_events, data);
}

/// Format a set of table slices like this:
// clang-format off
//
// {
//   "version": "v2.4-rc2",
//   "num_events": 3,
//   "events": [
//      {
//        "name": "zeek.conn",
//        "schema": [{"name": "_path", "type": "string"}, {"name": "uid", "type": "string"}, ...],
//        "data": [
//          {"_path": "snmp", "_write_ts": "2020-04-01T16:24:33.525023", "ts": "2020-04-01T16:19:33.529926", "uid": "C8Z7zO3pFoxOiC4yj9", "id.orig_h": "104.206.128.30", "id.orig_p": 63509, "id.resp_h": "141.9.71.231", "id.resp_p": 161, "duration": "0.0ns", "version": "1", "community": "public", "get_requests": 1, "get_bulk_requests": 0, "get_responses": 0, "set_requests": 0, "display_string": null, "up_since": null},
//          {"_path": "snmp", "_write_ts": "2020-04-01T16:24:33.525023", "ts": "2020-04-01T16:19:33.529926", "uid": "C8Z7zO3pFoxOiC4yj9", "id.orig_h": "104.206.128.30", "id.orig_p": 63509, "id.resp_h": "141.9.71.231", "id.resp_p": 161, "duration": "0.0ns", "version": "1", "community": "public", "get_requests": 1, "get_bulk_requests": 0, "get_responses": 0, "set_requests": 0, "display_string": null, "up_since": null},
//          {"_path": "snmp", "_write_ts": "2020-04-01T16:24:33.525023", "ts": "2020-04-01T16:19:33.529926", "uid": "C8Z7zO3pFoxOiC4yj9", "id.orig_h": "104.206.128.30", "id.orig_p": 63509, "id.resp_h": "141.9.71.231", "id.resp_p": 161, "duration": "0.0ns", "version": "1", "community": "public", "get_requests": 1, "get_bulk_requests": 0, "get_responses": 0, "set_requests": 0, "display_string": null, "up_since": null}
//        ],
//      },
//      {
//        "name": "zeek.dns",
//        [...]
//
// clang-format on
std::string format_result_typed(const std::vector<table_slice>& slices,
                                const caf::settings& formatting_options) {
  auto num_events = size_t{0ull};
  std::set<vast::type> all_types;
  for (auto const& slice : slices)
    all_types.insert(slice.schema());
  std::unordered_map<vast::type, vast::format::json::writer> events;
  std::unordered_map<vast::type, std::string> schemas;
  for (auto const& type : all_types) {
    auto data = type.to_definition();
    auto ostream = std::make_unique<std::stringstream>();
    auto writer
      = vast::format::json::writer{std::move(ostream), formatting_options};
    events.insert({vast::type{type}, std::move(writer)});
    auto& schema = schemas[type];
    schema = "[";
    auto record = caf::get<vast::record_type>(type);
    bool first = true;
    for (auto const& leaf : record.leaves()) {
      if (!first)
        schema += ", ";
      schema += fmt::format(R"_({{"name": "{}", "type": "{:-a}"}})_",
                            leaf.field.name, leaf.field.type);
      first = false;
    }
    schema += "]";
  }
  for (auto const& slice : slices) {
    auto& writer = events.at(slice.schema());
    num_events += slice.rows();
    if (auto error = writer.write(slice))
      VAST_WARN("json writer failed to write table slice: {}", error);
  }
  std::string events_stringified;
  for (auto& [type, writer] : events) {
    if (!events_stringified.empty())
      events_stringified += ",\n";
    auto data = static_cast<std::stringstream&>(writer.out()).str();
    // Remove line breaks since writer output is NDJSON, except for the
    // last since JSON doesn't support trailing commas in arrays.
    std::replace(data.begin(), data.end(), '\n', ',');
    if (!data.empty())
      data.back() = ' ';
    events_stringified += fmt::format("{{ \"name\": \"{}\",\n \"schema\": "
                                      "{},\n \"data\": [{}] }}",
                                      type.name(), schemas[type], data);
  }
  return fmt::format("{{\"version\": \"{}\",\n \"num_events\": {},\n "
                     "\"events\": [\n{}\n] }}",
                     vast::version::version, num_events, events_stringified);
}

std::string format_results(const std::vector<table_slice>& slices,
                           const export_format_options& opts) {
  auto json_writer_settings = caf::settings{};
  put(json_writer_settings, "vast.export.json.flatten", opts.flatten);
  put(json_writer_settings, "vast.export.json.numeric-durations",
      opts.numeric_durations);
  put(json_writer_settings, "vast.export.json.omit-nulls", opts.omit_nulls);
  if (opts.typed_results)
    return format_result_typed(slices, json_writer_settings);
  else
    return format_result_flat(slices, json_writer_settings);
}

constexpr static const auto ENDPOINT_EXPORT = 0ull;
constexpr static const auto ENDPOINT_EXPORT_TYPED = 1ull;

struct export_multiplexer_state {
  export_multiplexer_state() = default;

  system::index_actor index_ = {};
};

export_helper_actor::behavior_type
export_helper(export_helper_actor::stateful_pointer<export_helper_state> self,
              system::index_actor index, export_parameters&& params,
              std::optional<pipeline_executor>&& executor,
              http_request&& request) {
  self->state.index_ = std::move(index);
  self->state.request_ = std::move(request);
  self->state.params_ = std::move(params);
  self->state.pipeline_ = std::move(executor);
  auto query
    = vast::query_context::make_extract("api", self, self->state.params_.expr);
  self->request(self->state.index_, caf::infinite, atom::evaluate_v, query)
    .await(
      [self](system::query_cursor cursor) {
        self->state.cursor_ = cursor;
      },
      [self](const caf::error& e) {
        auto response = fmt::format("received error response from index {}", e);
        self->state.request_.response->abort(500, response);
        self->quit(e);
      });
  return {
    // Index-facing API
    [self](vast::table_slice& slice) {
      if (self->state.params_.limit <= self->state.events_)
        return;
      auto remaining = self->state.params_.limit - self->state.events_;
      self->state.events_ += std::min<size_t>(slice.rows(), remaining);
      if (slice.rows() < remaining)
        self->state.results_.emplace_back(std::move(slice));
      else
        self->state.results_.emplace_back(head(std::move(slice), remaining));
    },
    [self](atom::done) {
      bool remaining_partitions = self->state.cursor_->candidate_partitions
                                  > self->state.cursor_->scheduled_partitions;
      auto remaining_events = self->state.params_.limit > self->state.events_;
      if (remaining_partitions && remaining_events) {
        auto next_batch_size = uint32_t{1};
        self->state.cursor_->scheduled_partitions += next_batch_size;
        self->send(self->state.index_, atom::query_v, self->state.cursor_->id,
                   next_batch_size);
      } else {
        std::vector<table_slice> slices;
        if (self->state.pipeline_) {
          for (auto&& slice : std::exchange(self->state.results_, {}))
            if (auto error = self->state.pipeline_->add(std::move(slice))) {
              VAST_WARN("{} failed to add slice to pipeline: {}", *self, error);
              break; // Assume that `finish()` will also fail now.
            }
          auto transformed = self->state.pipeline_->finish();
          if (!transformed)
            return self->state.request_.response->abort(
              500,
              fmt::format("failed to apply pipeline: {}", transformed.error()));
          slices = std::move(*transformed);
        } else {
          slices = std::move(self->state.results_);
        }
        auto response_body
          = format_results(slices, self->state.params_.format_opts);
        self->state.request_.response->append(response_body);
        self->state.request_.response.reset();
      }
    }};
}

export_multiplexer_actor::behavior_type export_multiplexer(
  export_multiplexer_actor::stateful_pointer<export_multiplexer_state> self,
  const system::node_actor& node) {
  self
    ->request(node, caf::infinite, atom::get_v, atom::label_v,
              std::vector<std::string>{"index"})
    .await(
      [self](std::vector<caf::actor>& components) {
        VAST_ASSERT_CHEAP(components.size() == 1);
        self->state.index_
          = caf::actor_cast<system::index_actor>(components[0]);
      },
      [self](caf::error& err) {
        VAST_ERROR("failed to get index from node: {}", std::move(err));
        self->quit();
      });
  return {
    [self](atom::http_request, uint64_t endpoint_id, http_request rq) {
      VAST_VERBOSE("{} handles /export request", *self);
      auto query_string = std::optional<std::string>{};
      if (rq.params.contains("expression")) {
        auto& param = rq.params.at("expression");
        // Should be type-checked by the server.
        VAST_ASSERT(caf::holds_alternative<std::string>(param));
        query_string = caf::get<std::string>(param);
      } else {
        query_string = "#type != \"this_expression_matches_everything\"";
      }
      auto query_result = system::parse_query(*query_string);
      if (!query_result)
        return rq.response->abort(400, fmt::format("unparseable query: {}\n",
                                                   query_result.error()));
      auto expr = query_result->first;
      auto normalized_expr = normalize_and_validate(expr);
      if (!normalized_expr)
        return rq.response->abort(400, fmt::format("invalid query: {}\n",
                                                   normalized_expr.error()));
      auto params = export_parameters{
        .expr = std::move(*normalized_expr),
      };
      if (endpoint_id == ENDPOINT_EXPORT_TYPED)
        params.format_opts.typed_results = true;
      if (rq.params.contains("limit")) {
        auto& param = rq.params.at("limit");
        // Should be type-checked by the server.
        VAST_ASSERT(caf::holds_alternative<uint64_t>(param));
        params.limit = caf::get<uint64_t>(param);
      }
      if (rq.params.contains("flatten")) {
        auto& param = rq.params.at("flatten");
        VAST_ASSERT(caf::holds_alternative<bool>(param));
        params.format_opts.flatten = caf::get<bool>(param);
      }
      if (rq.params.contains("omit-nulls")) {
        auto& param = rq.params.at("omit-nulls");
        VAST_ASSERT(caf::holds_alternative<bool>(param));
        params.format_opts.omit_nulls = caf::get<bool>(param);
      }
      if (rq.params.contains("numeric-durations")) {
        auto& param = rq.params.at("numeric-durations");
        VAST_ASSERT(caf::holds_alternative<bool>(param));
        params.format_opts.numeric_durations = caf::get<bool>(param);
      }
      auto pipeline_executor = std::optional<vast::pipeline_executor>{};
      if (rq.params.contains("pipeline")) {
        auto data = from_json(caf::get<std::string>(rq.params.at("pipeline")));
        if (!data)
          return rq.response->abort(400, "couldn't parse pipeline "
                                         "definition\n");
        if (!caf::holds_alternative<record>(*data))
          return rq.response->abort(400, "expected json object for parameter "
                                         "'pipeline'\n");
        auto& record = caf::get<vast::record>(*data);
        if (!record.contains("steps"))
          return rq.response->abort(400, "missing 'steps'\n");
        auto settings = caf::config_value{};
        auto error = convert(record.at("steps"), settings);
        if (error)
          return rq.response->abort(400, "couldn't convert pipeline "
                                         "definition to caf::settings\n");
        if (!caf::holds_alternative<caf::config_value::list>(settings))
          return rq.response->abort(400, "expected a list of steps\n");
        auto& steps = caf::get<caf::config_value::list>(settings);
        // TODO: get name and types from json data
        auto pipeline
          = vast::pipeline{"rest-adhoc-pipeline", std::vector<std::string>{}};
        error = system::parse_pipeline_operators(pipeline, steps);
        if (error)
          return rq.response->abort(400, "couldn't convert pipeline "
                                         "definition\n");
        auto pipelines = std::vector<vast::pipeline>{};
        pipelines.emplace_back(std::move(pipeline));
        pipeline_executor = vast::pipeline_executor{std::move(pipelines)};
      }
      // TODO: Abort the request after some time limit has passed.
      auto exporter
        = self->spawn(export_helper, self->state.index_, std::move(params),
                      std::move(pipeline_executor), std::move(rq));
    },
  };
}

class plugin final : public virtual rest_endpoint_plugin {
  caf::error initialize([[maybe_unused]] data config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "api-export";
  };

  [[nodiscard]] std::string prefix() const override {
    return "";
  }

  [[nodiscard]] data openapi_specification(api_version version) const override {
    if (version != api_version::v0)
      return vast::record{};
    auto result = from_yaml(SPEC_V0);
    if (!result)
      VAST_ERROR("invalid yaml: {}", result.error());
    VAST_ASSERT(result);
    return *result;
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] const std::vector<rest_endpoint>&
  rest_endpoints() const override {
    static auto common_parameters = vast::record_type{
      {"expression", vast::string_type{}},
      {"limit", vast::uint64_type{}},
      {"pipeline", vast::string_type{}},
      {"flatten", vast::bool_type{}},
      {"omit-nulls", vast::bool_type{}},
      {"numeric-durations", vast::bool_type{}},
    };
    static auto endpoints = std::vector<vast::rest_endpoint>{
      {
        .endpoint_id = ENDPOINT_EXPORT,
        .method = http_method::get,
        .path = "/export",
        .params = common_parameters,
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id = ENDPOINT_EXPORT,
        .method = http_method::post,
        .path = "/export",
        .params = common_parameters,
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id = ENDPOINT_EXPORT_TYPED,
        .method = http_method::post,
        .path = "/export/with-schemas",
        .params = common_parameters,
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
    };
    return endpoints;
  }

  system::rest_handler_actor
  handler(caf::actor_system& system, system::node_actor node) const override {
    return system.spawn(export_multiplexer, node);
  }
};

} // namespace vast::plugins::rest_api::export_

VAST_REGISTER_PLUGIN(vast::plugins::rest_api::export_::plugin)
