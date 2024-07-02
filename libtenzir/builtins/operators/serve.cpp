//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// ARCHITECTURE
//
// The serve builtin contains three parts, namely the serve-manager component,
// the serve operator, and the /serve endpoint.
//
// SERVE OPERATOR
//
// The serve operator is an event sink that exposes the events it receives
// incrementally through a REST API.
//
// SERVE ENDPOINT
//
// The /serve endpoint allows for fetching events from a pipeline that ended in
// the serve operator incrementally.
//
// SERVE-MANAGER COMPONENT
//
// The serve-manager component is invisible to the user. It is responsible for
// bridging between the serve operator and the /serve endpoint, observing when
// the operator is done, throttling the operator when events are being requested
// too slowly, and managing request limits and timeouts.
//
// KNOWN ISSUES & LIMITATIONS
//
// The serve operator must currently run detached because it uses blocking
// communication for throttling. This would not be required if the operator API
// used an awaitable coroutine like an async generator. We should revisit this
// once the operator API supports awaiting non-blocking requests.
//
// The web is a lossy place—which is why the serve-manager caches its last
// result set and the last continuation token. To also be able to cache the last
// result set, we delay the removal of the managed serve operators in the
// serve-manager by 1 minute.
//
// Technically, the serve-manager should not be needed. However, the current
// architecture of the web plugin makes it so that the REST handler actor is not
// implicitly a component actor, and as such may run outside of the node or even
// multiple times. We should revisit this in the future.

#include <tenzir/actors.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/node.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/query_cursor.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/status.hpp>
#include <tenzir/table_slice.hpp>

#include <arrow/record_batch.h>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <sstream>

namespace tenzir::plugins::serve {

namespace {

constexpr auto SERVE_ENDPOINT_ID = 0;

constexpr auto SPEC_V0 = R"_(
/serve:
  post:
    summary: Return data from a pipeline
    description: "Returns events from an existing pipeline. The pipeline definition must include a serve operator. By default, the endpoint performs long polling (`timeout: 2s`) and returns events as soon as they are available (`min_events: 1`)."
    requestBody:
      description: Body for the serve endpoint
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [serve_id]
            properties:
              serve_id:
                type: string
                example: "query1"
                description: The id that was passed to the serve operator.
              continuation_token:
                type: string
                example: "340ce2j"
                description: The continuation token that was returned with the last response. For the initial request this is null.
              max_events:
                type: integer
                example: 1024
                default: 1024
                description: The maximum number of events returned.
              min_events:
                type: integer
                example: 1
                default: 1
                description: Wait for this number of events before returning.
              timeout:
                type: string
                example: "2000ms"
                default: "2000ms"
                description: The maximum amount of time spent on the request. Hitting the timeout is not an error. The timeout must not be greater than 5 seconds.
              use_simple_format:
                type: bool
                example: true
                default: false
                description: Use an experimental, more simple format for the contained schema, and render durations as numbers representing seconds as opposed to human-readable strings.
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              properties:
                next_continuation_token:
                  type: string
                  description: A token to access the next pipeline data batch, null if the pipeline is completed.
                  example: "340ce2j"
                schemas:
                  type: array
                  items:
                    type: object
                    properties:
                      schema_id:
                        type: string
                        description: The unique schema identifier.
                      definition:
                        type: object
                        description: The schema definition in JSON format.
                  description: The schemas that the served events are based on.
                  example:
                  - schema_id: "c631d301e4b18f4"
                    definition:
                      record:
                        - timestamp: "time"
                          schema: "string"
                          schema_id: "string"
                          events: "uint64"
                events:
                  type: array
                  items:
                    type: object
                    properties:
                      schema_id:
                        type: string
                        description: The unique schema identifier.
                      data:
                        type: object
                        description: The actual served data in JSON format.
                  description: The served events.
                  example:
                  - schema_id: c631d301e4b18f4
                    data:
                      timestamp: "2023-04-26T12:00:00Z"
                      schema: "zeek.conn"
                      schema_id: "ab2371bas235f1"
                      events: 50
                  - schema_id: c631d301e4b18f4
                    data:
                      timestamp: "2023-04-26T12:05:00Z"
                      schema: "suricata.dns"
                      schema_id: "cd4771bas235f1"
                      events: 50
      400:
        description: Invalid arguments.
        content:
          application/json:
            schema:
              type: object
              required: [error]
              properties:
                error:
                  type: string
                  example: "Invalid arguments"
                  description: The error message.
    )_";

// -- serve manager -----------------------------------------------------------

using serve_manager_actor = typed_actor_fwd<
  // Register a new serve operator.
  auto(atom::start, std::string serve_id, uint64_t buffer_size)
    ->caf::result<void>,
  // Deregister a serve operator, waiting until it completed.
  auto(atom::stop, std::string serve_id)->caf::result<void>,
  // Put additional slices into the buffer for the given access token.
  auto(atom::put, std::string serve_id, table_slice)->caf::result<void>,
  // Get slices from the buffer for the given access token, returning the next
  // access token and the desired number of events.
  auto(atom::get, std::string serve_id, std::string continuation_token,
       uint64_t min_events, duration timeout, uint64_t max_events)
    ->caf::result<std::tuple<std::string, std::vector<table_slice>>>>
  // Conform to the protocol of the COMPONENT PLUGIN actor interface.
  ::extend_with<component_plugin_actor>::unwrap;

struct request_limits {
  uint64_t max_events = defaults::api::serve::max_events;
  uint64_t min_events = defaults::api::serve::min_events;
  duration timeout = defaults::api::serve::timeout;
};

struct serve_request {
  std::string serve_id = {};
  std::string continuation_token = {};
  bool use_simple_format = {};
  request_limits limits = {};
};

/// A single serve operator as observed by the serve-manager.
struct managed_serve_operator {
  /// The actor address of the execution node of the serve operator; stored for
  /// tracking purposes.
  caf::actor_addr source = {};

  /// The serve ID and next expected continuation token of the operator.
  std::string serve_id = {};
  std::string continuation_token = {};

  /// The web is a naturally lossy place, so we cache the last response in case
  /// it didn't get delivered so the client can retry.
  bool done = {};
  std::string last_continuation_token = {};
  std::vector<table_slice> last_results = {};

  /// The buffered table slice, and the configured buffer size and the number of
  /// currently requested events (may exceed the buffer size).
  std::vector<table_slice> buffer = {};
  uint64_t buffer_size = 1 << 16;
  uint64_t requested = {};
  uint64_t min_events = {};

  /// The number of delivered results. Tracked only for the status output and
  /// not used otherwise.
  uint64_t delivered = {};

  /// Various handles for interfacing with the endpoint and the operator, and
  /// throttling the pipeline leading into the operator.
  caf::disposable delayed_attempt = {};
  caf::typed_response_promise<void> put_rp = {};
  caf::typed_response_promise<void> stop_rp = {};
  caf::typed_response_promise<std::tuple<std::string, std::vector<table_slice>>>
    get_rp = {};

  /// Attempt to deliver up to the number of requested results.
  /// @param force_underful Return underful result sets instead of failing when
  /// not enough results are buffered.
  /// @returns Whether the results were delivered.
  auto try_deliver_results(bool force_underful) -> bool {
    TENZIR_ASSERT(get_rp.pending());
    // If we throttled the serve operator, then we can continue its operation
    // again if we have less events buffered than desired.
    if (put_rp.pending() and rows(buffer) < std::max(buffer_size, requested)) {
      put_rp.deliver();
    }
    // Avoid delivering too early, i.e., when we don't yet have enough events.
    const auto return_underful = stop_rp.pending() or force_underful;
    if (not return_underful and rows(buffer) < min_events
        and rows(buffer) < requested) {
      return false;
    }
    // Cut the results buffer.
    auto results = std::vector<table_slice>{};
    std::tie(results, buffer) = split(buffer, requested);
    delivered += rows(results);
    // Clear the delayed attempt and the continuation token.
    delayed_attempt.dispose();
    requested = 0;
    TENZIR_DEBUG("clearing continuation token");
    last_continuation_token = std::exchange(continuation_token, {});
    last_results = results;
    if (stop_rp.pending() and buffer.empty()) {
      TENZIR_ASSERT(not put_rp.pending());
      TENZIR_DEBUG("serve for id {} is done", escape_operator_arg(serve_id));
      continuation_token.clear();
      get_rp.deliver(std::make_tuple(std::string{}, std::move(results)));
      stop_rp.deliver();
      return true;
    }
    // If we throttled the serve operator, then we can continue its operation
    // again if we have less events buffered than desired.
    if (put_rp.pending() and rows(buffer) < buffer_size) {
      put_rp.deliver();
    }
    continuation_token = fmt::to_string(uuid::random());
    TENZIR_DEBUG("serve for id {} is now available with continuation token {}",
                 escape_operator_arg(serve_id), continuation_token);
    get_rp.deliver(std::make_tuple(continuation_token, std::move(results)));
    return true;
  }
};

struct serve_manager_state {
  static constexpr auto name = "serve-manager";

  serve_manager_actor::pointer self = {};

  /// The serve operators currently observed by the serve-manager.
  std::vector<managed_serve_operator> ops = {};

  /// A list of previously known serve ids that were expired and their
  /// corresponding error messages. This exists only for returning better error
  /// messages to the user.
  std::unordered_map<std::string, caf::error> expired_ids = {};

  auto handle_down_msg(const caf::down_msg& msg) -> void {
    const auto found
      = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
          return op.source == msg.source;
        });
    if (found == ops.end()) {
      TENZIR_WARN("{} received unepexted DOWN from {}: {}", *self, msg.source,
                  msg.reason);
      return;
    }
    if (not found->continuation_token.empty()) {
      TENZIR_DEBUG("{} received premature DOWN for serve id {} with "
                   "continuation "
                   "token {}",
                   *self, found->serve_id, found->continuation_token);
    }
    // We delay the actual removal because we support fetching the
    // last set of events again by reusing the last continuation token.
    found->done = true;
    auto delete_serve = [this, source = msg.source, reason = msg.reason]() {
      const auto found
        = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
            return op.source == source;
          });
      if (found != ops.end()) {
        expired_ids.emplace(found->serve_id, reason);
        if (found->get_rp.pending()) {
          found->delayed_attempt.dispose();
          found->get_rp.deliver(reason);
        }
        ops.erase(found);
      }
    };
    if (msg.reason) {
      delete_serve();
    } else {
      detail::weak_run_delayed(self, defaults::api::serve::retention_time,
                               delete_serve);
    }
  }

  auto start(std::string serve_id, uint64_t buffer_size) -> caf::result<void> {
    const auto found
      = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
          return op.serve_id == serve_id;
        });
    if (found != ops.end()) {
      if (not found->done) {
        return caf::make_error(
          ec::invalid_argument,
          fmt::format("{} received duplicate serve id {}", *self,
                      escape_operator_arg(found->serve_id)));
      }
      ops.erase(found);
    }
    ops.push_back({
      .source = self->current_sender()->address(),
      .serve_id = serve_id,
      .continuation_token = "",
      .buffer_size = buffer_size,
    });
    self->monitor(ops.back().source);
    return {};
  }

  auto stop(std::string serve_id) -> caf::result<void> {
    const auto found
      = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
          return op.serve_id == serve_id;
        });
    if (found == ops.end()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("{} received request to despawn for "
                                         "unknown serve id {}",
                                         *self, escape_operator_arg(serve_id)));
    }
    if (found->stop_rp.pending()) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} received duplicate request to "
                                         "despawn for serve id {}",
                                         *self, escape_operator_arg(serve_id)));
    }
    found->stop_rp = self->make_response_promise<void>();
    return found->stop_rp;
  }

  auto put(std::string serve_id, table_slice slice) -> caf::result<void> {
    const auto found
      = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
          return op.serve_id == serve_id;
        });
    if (found == ops.end()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("{} received events for unknown serve "
                                         "id {}",
                                         *self, escape_operator_arg(serve_id)));
    }
    if (found->put_rp.pending()) {
      return caf::make_error(
        ec::logic_error, fmt::format("{} received events for serve id {}, but "
                                     "promise is still pending",
                                     *self, escape_operator_arg(serve_id)));
    }
    found->buffer.push_back(std::move(slice));
    if (found->get_rp.pending()) {
      const auto delivered = found->try_deliver_results(false);
      if (delivered) {
        TENZIR_DEBUG("{} delivered results eagerly for serve id {}", *self,
                     escape_operator_arg(serve_id));
      }
    }
    if (rows(found->buffer) < std::max(found->requested, found->buffer_size)) {
      return {};
    }
    found->put_rp = self->make_response_promise<void>();
    return found->put_rp;
  }

  auto get(serve_request request)
    -> caf::result<std::tuple<std::string, std::vector<table_slice>>> {
    const auto found
      = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
          return op.serve_id == request.serve_id;
        });
    if (found == ops.end()) {
      const auto expired_id = expired_ids.find(request.serve_id);
      if (expired_id != expired_ids.end()) {
        if (expired_id->second == ec::diagnostic) {
          return expired_id->second;
        }
        return caf::make_error(
          ec::logic_error,
          fmt::format("{} got request for events with expired serve id {}; the "
                      "pipeline serving this data is no longer available: {}",
                      *self, request.serve_id, expired_id->second));
      }
      return caf::make_error(ec::invalid_argument,
                             fmt::format("{} got request for events with "
                                         "unknown serve id {}",
                                         *self, request.serve_id));
    }
    if (not found->continuation_token.empty()
        and found->last_continuation_token == request.continuation_token) {
      return std::make_tuple(
        found->continuation_token,
        split(found->last_results, request.limits.max_events).first);
    }
    if (found->continuation_token != request.continuation_token) {
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("{} got request for events with unknown continuation token "
                    "{} for serve id {}",
                    *self, request.continuation_token, request.serve_id));
    }
    if (found->get_rp.pending()) {
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("{} got duplicate request for events "
                    "with continuation token {} for serve id {}",
                    *self, request.continuation_token, request.serve_id));
    }
    found->get_rp = self->make_response_promise<
      std::tuple<std::string, std::vector<table_slice>>>();
    found->requested = request.limits.max_events;
    found->min_events = request.limits.min_events;
    const auto delivered = found->try_deliver_results(false);
    if (delivered) {
      return found->get_rp;
    }
    found->delayed_attempt.dispose();
    found->delayed_attempt = detail::weak_run_delayed(
      self, request.limits.timeout,
      [this, serve_id = request.serve_id,
       continuation_token = request.continuation_token]() mutable {
        const auto found
          = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
              return op.serve_id == serve_id;
            });
        if (found == ops.end()) {
          TENZIR_DEBUG("unable to find serve request after timeout expired");
          return;
        }
        TENZIR_ASSERT(not found->done);
        TENZIR_ASSERT(found->continuation_token == continuation_token);
        TENZIR_ASSERT(found->get_rp.pending());
        const auto delivered = found->try_deliver_results(true);
        TENZIR_ASSERT(delivered);
      });
    return found->get_rp;
  }

  auto status(status_verbosity verbosity) const -> caf::result<record> {
    auto requests = list{};
    requests.reserve(ops.size());
    for (const auto& op : ops) {
      auto& entry = caf::get<record>(requests.emplace_back(record{}));
      entry.emplace("serve_id", op.serve_id);
      entry.emplace("continuation_token", op.continuation_token.empty()
                                            ? data{}
                                            : op.continuation_token);
      entry.emplace("buffer_size", op.buffer_size);
      entry.emplace("num_buffered", rows(op.buffer));
      entry.emplace("num_requested", op.requested);
      entry.emplace("num_delivered", op.delivered);
      entry.emplace("lingering", op.continuation_token.empty());
      entry.emplace("done", op.done);
      if (verbosity >= status_verbosity::detailed) {
        entry.emplace("put_pending", op.put_rp.pending());
        entry.emplace("get_pending", op.get_rp.pending());
        entry.emplace("stop_pending", op.stop_rp.pending());
      }
      if (verbosity >= status_verbosity::debug) {
        entry.emplace("source", fmt::to_string(op.source));
        entry.emplace("last_continuation_token",
                      op.last_continuation_token.empty()
                        ? data{}
                        : op.last_continuation_token);
        entry.emplace("last_num_results", rows(op.last_results));
      }
    }
    return record{
      {"requests", std::move(requests)},
    };
  }
};

auto serve_manager(
  serve_manager_actor::stateful_pointer<serve_manager_state> self)
  -> serve_manager_actor::behavior_type {
  self->state.self = self;
  self->set_down_handler([self](const caf::down_msg& msg) {
    self->state.handle_down_msg(msg);
  });
  return {
    [self](atom::start, std::string& serve_id,
           uint64_t buffer_size) -> caf::result<void> {
      return self->state.start(std::move(serve_id), buffer_size);
    },
    [self](atom::stop, std::string& serve_id) -> caf::result<void> {
      return self->state.stop(std::move(serve_id));
    },
    [self](atom::put, std::string& serve_id,
           table_slice& slice) -> caf::result<void> {
      return self->state.put(std::move(serve_id), std::move(slice));
    },
    [self](atom::get, std::string& serve_id, std::string& continuation_token,
           uint64_t min_events, duration timeout, uint64_t max_events)
      -> caf::result<std::tuple<std::string, std::vector<table_slice>>> {
      return self->state.get(
        {.serve_id = std::move(serve_id),
         .continuation_token = std::move(continuation_token),
         .limits = {
           .max_events = max_events,
           .min_events = min_events,
           .timeout = timeout,
         }});
    },
    [self](atom::status, status_verbosity verbosity,
           duration) -> caf::result<record> {
      return self->state.status(verbosity);
    },
  };
}

// -- serve handler -----------------------------------------------------------

using serve_handler_actor
  = typed_actor_fwd<>::extend_with<rest_handler_actor>::unwrap;

struct serve_handler_state {
  static constexpr auto name = "serve-handler";

  serve_handler_actor::pointer self = {};
  serve_manager_actor serve_manager = {};

  struct parse_error {
    std::string message;
    caf::error detail;
  };

  static auto try_parse_request(const tenzir::record& params)
    // TODO: Switch to std::expected<serve_request, parse_error> after C++23.
    -> std::variant<serve_request, parse_error> {
    auto result = serve_request{};
    auto serve_id = try_get<std::string>(params, "serve_id");
    if (not serve_id) {
      return parse_error{
        .message = "failed to read serve_id parameter",
        .detail = caf::make_error(ec::invalid_argument,
                                  fmt::format("{}; got parameters {}",
                                              serve_id.error(), params))};
    }
    if (not *serve_id) {
      return parse_error{
        .message = "serve_id must be specified",
        .detail = caf::make_error(ec::invalid_argument,
                                  fmt::format("got parameters {}", params))};
    }
    result.serve_id = std::move(**serve_id);
    auto continuation_token
      = try_get<std::string>(params, "continuation_token");
    if (not continuation_token) {
      return parse_error{.message = "failed to read continuation_token",
                         .detail = caf::make_error(
                           ec::invalid_argument,
                           fmt::format("{}; got parameters {}",
                                       continuation_token.error(), params))};
    }
    if (*continuation_token) {
      result.continuation_token = std::move(**continuation_token);
    }
    auto max_events = try_get<uint64_t>(params, "max_events");
    if (not max_events) {
      return parse_error{
        .message = "failed to read max_events",
        .detail = caf::make_error(ec::invalid_argument,
                                  fmt::format("parameter: {}; got params {}",
                                              max_events.error(), params))};
    }
    if (*max_events) {
      result.limits.max_events = **max_events;
    }
    auto min_events = try_get<uint64_t>(params, "min_events");
    if (not min_events) {
      return parse_error{
        .message = "failed to read min_events",
        .detail = caf::make_error(ec::invalid_argument,
                                  fmt::format("parameter: {}; got params {}",
                                              max_events.error(), params))};
    }
    if (*min_events) {
      result.limits.min_events = **min_events;
    }
    auto timeout = try_get<duration>(params, "timeout");
    if (not timeout) {
      auto detail_msg
        = fmt::format("{}; got params {}", timeout.error(), params);
      auto detail
        = caf::make_error(ec::invalid_argument, std::move(detail_msg));
      return parse_error{.message = "failed to read timeout parameter",
                         .detail = std::move(detail)};
    }
    if (*timeout) {
      if (**timeout > defaults::api::serve::max_timeout) {
        auto message = fmt::format("timeout exceeds limit of {}",
                                   defaults::api::serve::max_timeout);
        auto detail = caf::make_error(
          ec::invalid_argument, fmt::format("got timeout {}", data{**timeout}));
        return parse_error{.message = std::move(message),
                           .detail = std::move(detail)};
      }
      result.limits.timeout = **timeout;
    }

    auto use_simple_format = try_get<bool>(params, "use_simple_format");
    if (not use_simple_format) {
      return parse_error{
        .message = "failed to read use_simple_format",
        .detail = caf::make_error(ec::invalid_argument,
                                  fmt::format("parameter: {}; got params {}",
                                              max_events.error(), params))};
    }
    if (*use_simple_format) {
      result.use_simple_format = **use_simple_format;
    }
    return result;
  }

  static auto create_response(const std::string& next_continuation_token,
                              const std::vector<table_slice>& results,
                              bool use_simple_format) -> std::string {
    auto printer = json_printer{{
      .indentation = 0,
      .oneline = true,
      .numeric_durations = use_simple_format,
    }};
    auto result
      = next_continuation_token.empty()
          ? std::string{R"({"next_continuation_token":null,"events":[)"}
          : fmt::format(R"({{"next_continuation_token":"{}","events":[)",
                        next_continuation_token);
    auto out_iter = std::back_inserter(result);
    auto seen_schemas = std::unordered_set<type>{};
    bool first = true;
    for (const auto& slice : results) {
      if (slice.rows() == 0)
        continue;
      seen_schemas.insert(slice.schema());
      auto resolved_slice = resolve_enumerations(slice);
      auto type = caf::get<record_type>(resolved_slice.schema());
      auto array
        = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
      for (const auto& row : values(type, *array)) {
        if (first)
          out_iter = fmt::format_to(out_iter, "{{");
        else
          out_iter = fmt::format_to(out_iter, "}},{{");
        first = false;
        out_iter = fmt::format_to(out_iter, R"("schema_id":"{}","data":)",
                                  slice.schema().make_fingerprint());
        TENZIR_ASSERT(row);
        const auto ok = printer.print(out_iter, *row);
        TENZIR_ASSERT(ok);
      }
    }
    // Write schemas
    if (seen_schemas.empty()) {
      out_iter = fmt::format_to(out_iter, R"(],"schemas":[]}}{})", '\n');
      return result;
    }
    out_iter = fmt::format_to(out_iter, R"(}}],"schemas":[)");
    for (bool first = true; const auto& schema : seen_schemas) {
      if (first)
        out_iter = fmt::format_to(out_iter, "{{");
      else
        out_iter = fmt::format_to(out_iter, "}},{{");
      first = false;
      out_iter = fmt::format_to(out_iter, R"("schema_id":"{}","definition":)",
                                schema.make_fingerprint());
      const auto ok
        = printer.print(out_iter, use_simple_format ? schema.to_definition2()
                                                    : schema.to_definition());
      TENZIR_ASSERT(ok);
    }
    out_iter = fmt::format_to(out_iter, R"(}}]}}{})", '\n');
    return result;
  }

  auto http_request(uint64_t endpoint_id, tenzir::record params) const
    -> caf::result<rest_response> {
    if (endpoint_id != SERVE_ENDPOINT_ID) {
      return caf::make_error(ec::logic_error,
                             fmt::format("unepexted /serve endpoint id {}",
                                         endpoint_id));
    }
    TENZIR_DEBUG("{} handles /serve request for endpoint id {} with params {}",
                 *self, endpoint_id, params);
    auto maybe_request = try_parse_request(params);
    if (auto* error = std::get_if<parse_error>(&maybe_request)) {
      return rest_response::make_error(400, std::move(error->message),
                                       std::move(error->detail));
    }
    auto& request = std::get<serve_request>(maybe_request);
    auto rp = self->make_response_promise<rest_response>();
    self
      ->request(serve_manager, caf::infinite, atom::get_v, request.serve_id,
                request.continuation_token, request.limits.min_events,
                request.limits.timeout, request.limits.max_events)
      .then(
        [rp, use_simple_format = request.use_simple_format](
          const std::tuple<std::string, std::vector<table_slice>>&
            result) mutable {
          rp.deliver(rest_response::from_json_string(create_response(
            std::get<0>(result), std::get<1>(result), use_simple_format)));
        },
        [rp](caf::error& err) mutable {
          // TODO: Use a struct with distinct fields for user-facing
          // error message and detail here.
          // TODO: We don't have the source here to print snippets of the
          // diagnostic! Either `serve` needs to be aware of that (which seems
          // like a very bad idea), or the diagnostics need to be rendered
          // somewhere else.
          auto stream = std::stringstream{};
          auto printer = make_diagnostic_printer(
            std::nullopt, color_diagnostics::yes, stream);
          printer->emit(diagnostic::error(err).done());
          auto rsp = rest_response::make_error(400, stream.str(), {});
          rp.deliver(std::move(rsp));
        });
    return rp;
  }
};

auto serve_handler(
  serve_handler_actor::stateful_pointer<serve_handler_state> self,
  const node_actor& node) -> serve_handler_actor::behavior_type {
  self->state.self = self;
  self
    ->request(node, caf::infinite, atom::get_v, atom::label_v,
              std::vector<std::string>{"serve-manager"})
    .await(
      [self](std::vector<caf::actor>& actors) {
        TENZIR_ASSERT(actors.size() == 1);
        self->state.serve_manager
          = caf::actor_cast<serve_manager_actor>(std::move(actors[0]));
      },
      [self](const caf::error& err) { //
        self->quit(caf::make_error(
          ec::logic_error,
          fmt::format("failed to find serve-manager: {}", err)));
      });
  return {
    [self](atom::http_request, uint64_t endpoint_id,
           tenzir::record& params) -> caf::result<rest_response> {
      return self->state.http_request(endpoint_id, std::move(params));
    },
  };
}

// -- serve operator ----------------------------------------------------------

class serve_operator final : public crtp_operator<serve_operator> {
public:
  serve_operator() = default;

  serve_operator(std::string serve_id, uint64_t buffer_size)
    : serve_id_{std::move(serve_id)}, buffer_size_{buffer_size} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    // This has to be blocking as the the code up to the first yield is run
    // synchronously, and we should guarantee that the serve manager knows about
    // a started pipeline containing a serve operator once it the pipeline
    // executor indicated that the pipeline started.
    auto serve_manager = serve_manager_actor{};
    // Step 1: Get a handle to the SERVE MANAGER actor.
    // NOTE: It is important that we let this actor run until the end of the
    // operator. The SERVE MANAGER monitors the actor that sends it the start
    // atom, and assumes that the operator shut down when it receives the
    // corresponding down message.
    {
      auto blocking = caf::scoped_actor{ctrl.self().system()};
      blocking
        ->request(ctrl.node(), caf::infinite, atom::get_v, atom::label_v,
                  std::vector<std::string>{"serve-manager"})
        .receive(
          [&](std::vector<caf::actor>& actors) {
            TENZIR_ASSERT(actors.size() == 1);
            serve_manager
              = caf::actor_cast<serve_manager_actor>(std::move(actors[0]));
          },
          [&](const caf::error& err) { //
            diagnostic::error(err)
              .note("failed to get serve-manager")
              .emit(ctrl.diagnostics());
          });
    }
    // Step 2: Register this operator at SERVE MANAGER actor using the serve_id.
    ctrl.self()
      .request(serve_manager, caf::infinite, atom::start_v, serve_id_,
               buffer_size_)
      .await(
        [&]() {
          TENZIR_DEBUG("serve for id {} is now available",
                       escape_operator_arg(serve_id_));
        },
        [&](const caf::error& err) { //
          diagnostic::error(err)
            .note("failed to register at serve-manager")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    // Step 3: Forward events to the SERVE MANAGER.
    for (auto&& slice : input) {
      // Send slice to SERVE MANAGER.
      ctrl.self()
        .request(serve_manager, caf::infinite, atom::put_v, serve_id_,
                 std::move(slice))
        .await(
          []() {
            // nop
          },
          [&](const caf::error& err) {
            diagnostic::error(err)
              .note("failed to buffer events at serve-manager")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    // Step 4: Wait until all events were fetched.
    ctrl.self()
      .request(serve_manager, caf::infinite, atom::stop_v, serve_id_)
      .await(
        []() {
          // nop
        },
        [&](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to deregister at serve-manager")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto name() const -> std::string override {
    return "serve";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, serve_operator& x) {
    return f.object(x)
      .pretty_name("tenzir.plugins.serve.serve-operator")
      .fields(f.field("serve-id", x.serve_id_),
              f.field("buffer-size", x.buffer_size_));
  }

private:
  std::string serve_id_ = {};
  uint64_t buffer_size_ = {};
};

// -- serve plugin ------------------------------------------------------------

class plugin final : public virtual component_plugin,
                     public virtual rest_endpoint_plugin,
                     public virtual operator_plugin<serve_operator>,
                     public virtual aspect_plugin {
public:
  auto component_name() const -> std::string override {
    return "serve-manager";
  }

  auto aspect_name() const -> std::string override {
    return "serves";
  }

  auto show(operator_control_plane& ctrl) const
    -> generator<table_slice> override {
    auto serve_manager = serve_manager_actor{};
    auto blocking = caf::scoped_actor{ctrl.self().system()};
    blocking
      ->request(ctrl.node(), caf::infinite, atom::get_v, atom::label_v,
                std::vector<std::string>{"serve-manager"})
      .receive(
        [&](std::vector<caf::actor>& actors) {
          TENZIR_ASSERT(actors.size() == 1);
          serve_manager
            = caf::actor_cast<serve_manager_actor>(std::move(actors[0]));
        },
        [&](const caf::error& err) { //
          diagnostic::error(err)
            .note("failed to get at serve-manager")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    auto serves = list{};
    blocking
      ->request(serve_manager, caf::infinite, atom::status_v,
                status_verbosity::debug, duration{std::chrono::seconds{10}})
      .receive(
        [&](record& response) {
          TENZIR_ASSERT(response.size() == 1);
          TENZIR_ASSERT(response.contains("requests"));
          TENZIR_ASSERT(caf::holds_alternative<list>(response["requests"]));
          serves = std::move(caf::get<list>(response["requests"]));
        },
        [&](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to get status")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    auto builder = series_builder{};
    for (const auto& serve : serves) {
      builder.data(serve);
    }
    for (auto&& result : builder.finish_as_table_slice("tenzir.serve")) {
      co_yield std::move(result);
    }
  }

  auto make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor override {
    return node->spawn<caf::linked>(serve_manager);
  }

  auto openapi_endpoints(api_version version) const -> record override {
    if (version != api_version::v0)
      return tenzir::record{};
    auto result = from_yaml(SPEC_V0);
    TENZIR_ASSERT(result);
    TENZIR_ASSERT(caf::holds_alternative<record>(*result));
    return caf::get<record>(*result);
  }

  auto rest_endpoints() const -> const std::vector<rest_endpoint>& override {
    static auto endpoints = std::vector<tenzir::rest_endpoint>{
      {
        .endpoint_id = SERVE_ENDPOINT_ID,
        .method = http_method::post,
        .path = "/serve",
        .params = record_type{
          {"serve_id", string_type{}},
          {"continuation_token", string_type{}},
          {"max_events", uint64_type{}},
          {"min_events", uint64_type{}},
          {"timeout", duration_type{}},
          {"use_simple_format", bool_type{}},
        },
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
    };
    return endpoints;
  }

  auto handler(caf::actor_system& system, node_actor node) const
    -> rest_handler_actor override {
    return system.spawn(serve_handler, node);
  }

  auto signature() const -> operator_signature override {
    return {.sink = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto buffer_size = located<uint64_t>{1 << 16, location::unknown};
    auto id = located<std::string>{};
    auto parser = argument_parser{"serve", "https://docs.tenzir.com/"
                                           "operators/serve"};
    parser.add("--buffer-size", buffer_size, "<size>");
    parser.add(id, "<id>");
    parser.parse(p);
    if (id.inner.empty()) {
      diagnostic::error("serve id must not be empty")
        .primary(id.source)
        .throw_();
    }
    if (buffer_size.inner == 0) {
      diagnostic::error("buffer size must not be zero")
        .primary(buffer_size.source)
        .throw_();
    }
    return std::make_unique<serve_operator>(std::move(id.inner),
                                            buffer_size.inner);
  }
};

} // namespace

} // namespace tenzir::plugins::serve

TENZIR_REGISTER_PLUGIN(tenzir::plugins::serve::plugin)
