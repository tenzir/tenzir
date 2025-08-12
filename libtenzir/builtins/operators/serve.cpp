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
// the serve operator, and the /serve and /serve-multi endpoints.
//
// SERVE OPERATOR
//
// The serve operator is an event sink that exposes the events it receives
// incrementally through a REST API.
//
// SERVE ENDPOINTS
//
// The /serve endpoint allows for fetching events from a pipeline that ended in
// the serve operator incrementally. The /serve-multi endpoint allows fetching
// from multiple pipelines at the same time, producing a keyed result.
//
// SERVE-MANAGER COMPONENT
//
// The serve-manager component is invisible to the user. It is responsible for
// bridging between the serve operator and the endpoints, observing when
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

#include "tenzir/detail/fanout_counter.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/node.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/query_cursor.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/status.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/record_batch.h>
#include <caf/actor_addr.hpp>
#include <caf/actor_registry.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <sstream>

namespace tenzir::plugins::serve {

TENZIR_ENUM(serve_state, running, completed, failed);
TENZIR_ENUM(schema, legacy, exact, never);

namespace {

constexpr auto serve_endpoint_id = 0;
constexpr auto serve_multi_endpoint_id = 1;

constexpr auto serve_spec = R"_(
/serve:
  post:
    summary: Return data from a pipeline
    description: "Returns events from an existing pipeline. The pipeline definition must include a serve operator. By default, the endpoint performs long polling (`timeout: 5s`) and returns events as soon as they are available (`min_events: 1`)."
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
                example: "200ms"
                default: "5s"
                description: The maximum amount of time spent on the request. Hitting the timeout is not an error. The timeout must not be greater than 10 seconds.
              schema:
                type: string
                example: "exact"
                default: "legacy"
                description: The output format in which schemas are represented. Must be one of "legacy", "exact", or "never". Use "exact" to switch to a type representation matching Tenzir's type system exactly, and "never" to omit schema definitions from the output entirely.
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
                state:
                  type: string
                  description: The state of the corresponding pipeline at the time of the request. One of `running`, `completed`, or `failed`.
                  example: "running"
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
                  - schema_id: c631d301e4b18f4
                    definition:
                    - name: tenzir.summarize
                      kind: record
                      type: tenzir.summarize
                      attributes: {}
                      path: []
                      fields:
                      - name: severity
                        kind: string
                        type: string
                        attributes: {}
                        path:
                        - 0
                        fields: []
                      - name: pipeline_id
                        kind: string
                        type: string
                        attributes: {}
                        path:
                        - 1
                        fields: []
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
constexpr auto serve_multi_spec = R"_(
/serve-multi:
  post:
    summary: Return data from multiple pipelines
    description: "Returns events from existing pipelines. The pipeline definitions must include a serve operator. By default, the endpoint performs long polling (`timeout: 5s`) and returns events as soon as they are available (`min_events: 1`)."
    requestBody:
      description: Body for the serve-multi endpoint
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [requests]
            properties:
              requests:
                type: array
                items:
                  type: object
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
                description: The maximum number of events returned. This is split evenly for all serve_ids. If necessary, it is rounded up.
              min_events:
                type: integer
                example: 1
                default: 1
                description: Wait for this number of events before returning. This is split evenly for all serve_ids. If necessary, it is rounded up.
              timeout:
                type: string
                example: "200ms"
                default: "5s"
                description: The maximum amount of time spent on the request. Hitting the timeout is not an error. The timeout must not be greater than 10 seconds.
              schema:
                type: string
                example: "exact"
                default: "legacy"
                description: The output format in which schemas are represented. Must be one of "legacy", "exact", or "never". Use "exact" to switch to a type representation matching Tenzir's type system exactly, and "never" to omit schema definitions from the output entirely.
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              additionalProperties:
                type: object
                description: The response is keyed by the serve-id
                properties:
                  next_continuation_token:
                    type: string
                    description: A token to access the next pipeline data batch, null if the pipeline is completed.
                    example: "340ce2j"
                  state:
                    type: string
                    description: The state of the corresponding pipeline at the time of the request. One of `running`, `completed`, or `failed`.
                    example: "running"
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
                    - schema_id: c631d301e4b18f4
                      definition:
                      - name: tenzir.summarize
                        kind: record
                        type: tenzir.summarize
                        attributes: {}
                        path: []
                        fields:
                        - name: severity
                          kind: string
                          type: string
                          attributes: {}
                          path:
                          - 0
                          fields: []
                        - name: pipeline_id
                          kind: string
                          type: string
                          attributes: {}
                          path:
                          - 1
                          fields: []
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

using serve_response = std::tuple<std::string, std::vector<table_slice>>;

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
    ->caf::result<serve_response>>
  // Conform to the protocol of the COMPONENT PLUGIN actor interface.
  ::extend_with<component_plugin_actor>::unwrap;

struct request_meta {
  uint64_t max_events = defaults::api::serve::max_events;
  uint64_t min_events = defaults::api::serve::min_events;
  duration timeout = defaults::api::serve::timeout;
  enum schema schema = schema::legacy;
};

struct request_base {
  std::string serve_id = {};
  std::string continuation_token = {};
};

struct single_serve_request : request_base, request_meta {};

struct multi_serve_request : request_meta {
  std::vector<request_base> requests;
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
  uint64_t buffer_size = defaults::api::serve::max_events;
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
  std::vector<caf::typed_response_promise<serve_response>> get_rps = {};

  /// Attempt to deliver up to the number of requested results.
  /// @param force_underful Return underful result sets instead of failing when
  /// not enough results are buffered.
  /// @returns Whether the results were delivered.
  auto try_deliver_results(bool force_underful) -> bool {
    TENZIR_ASSERT(not get_rps.empty());
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
      for (auto&& get_rp : std::exchange(get_rps, {})) {
        TENZIR_ASSERT(get_rp.pending());
        get_rp.deliver(std::make_tuple(std::string{}, results));
      }
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
    for (auto&& get_rp : std::exchange(get_rps, {})) {
      TENZIR_ASSERT(get_rp.pending());
      get_rp.deliver(std::make_tuple(continuation_token, results));
    }
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

  auto handle_down_msg(const caf::actor_addr& source, const caf::error& err)
    -> void {
    const auto found = std::ranges::find_if(ops, [&](const auto& op) {
      return op.source == source;
    });
    TENZIR_ASSERT(found != ops.end());
    if (not found->continuation_token.empty()) {
      TENZIR_DEBUG("{} received premature DOWN for serve id {} with "
                   "continuation "
                   "token {}",
                   *self, found->serve_id, found->continuation_token);
    }
    // We delay the actual removal because we support fetching the
    // last set of events again by reusing the last continuation token.
    found->done = true;
    auto delete_serve = [this, source, err]() {
      const auto found = std::ranges::find_if(ops, [&](const auto& op) {
        return op.source == source;
      });
      if (found != ops.end()) {
        expired_ids.emplace(found->serve_id, err);
        if (not found->get_rps.empty()) {
          found->delayed_attempt.dispose();
          for (auto&& get_rp : std::exchange(found->get_rps, {})) {
            get_rp.deliver(err);
          }
        }
        ops.erase(found);
      }
    };
    if (err) {
      delete_serve();
      return;
    }
    detail::weak_run_delayed(self, defaults::api::serve::retention_time,
                             delete_serve);
  }

  auto start(std::string serve_id, uint64_t buffer_size) -> caf::result<void> {
    const auto found = std::ranges::find_if(ops, [&](const auto& op) {
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
    const auto sender = caf::actor_cast<caf::actor>(self->current_sender());
    TENZIR_ASSERT(sender);
    const auto addr = sender->address();
    ops.push_back({
      .source = addr,
      .serve_id = serve_id,
      .continuation_token = "",
      .buffer_size = buffer_size,
    });
    self->monitor(sender, [this, addr](const caf::error& err) {
      handle_down_msg(addr, err);
    });
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
    if (not found->get_rps.empty()) {
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

  auto get(single_serve_request request) -> caf::result<serve_response> {
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
        split(found->last_results, request.max_events).first);
    }
    if (found->continuation_token != request.continuation_token) {
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("{} got request for events with unknown continuation token "
                    "{} for serve id {}",
                    *self, request.continuation_token, request.serve_id));
    }
    if (found->done) {
      return std::make_tuple(std::string{}, std::vector<table_slice>{});
    }
    auto rp = self->make_response_promise<serve_response>();
    found->get_rps.push_back(rp);
    found->requested = request.max_events;
    found->min_events = request.min_events;
    const auto delivered = found->try_deliver_results(false);
    if (delivered) {
      return rp;
    }
    found->delayed_attempt.dispose();
    found->delayed_attempt = detail::weak_run_delayed(
      self, request.timeout,
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
        // In case the client re-sent the request in the meantime we are done.
        if (found->done or found->continuation_token != continuation_token) {
          return;
        }
        TENZIR_ASSERT(not found->get_rps.empty());
        const auto delivered = found->try_deliver_results(true);
        TENZIR_ASSERT(delivered);
      });
    return rp;
  }

  auto status(status_verbosity verbosity) const -> caf::result<record> {
    auto requests = list{};
    requests.reserve(ops.size());
    for (const auto& op : ops) {
      auto& entry = as<record>(requests.emplace_back(record{}));
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
        entry.emplace("get_pending", not op.get_rps.empty());
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
  self->state().self = self;
  return {
    [self](atom::start, std::string& serve_id,
           uint64_t buffer_size) -> caf::result<void> {
      return self->state().start(std::move(serve_id), buffer_size);
    },
    [self](atom::stop, std::string& serve_id) -> caf::result<void> {
      return self->state().stop(std::move(serve_id));
    },
    [self](atom::put, std::string& serve_id,
           table_slice& slice) -> caf::result<void> {
      return self->state().put(std::move(serve_id), std::move(slice));
    },
    [self](atom::get, std::string& serve_id, std::string& continuation_token,
           uint64_t min_events, duration timeout,
           uint64_t max_events) -> caf::result<serve_response> {
      return self->state().get({
        {
          .serve_id = std::move(serve_id),
          .continuation_token = std::move(continuation_token),
        },
        {
          .max_events = max_events,
          .min_events = min_events,
          .timeout = timeout,
        },
      });
    },
    [self](atom::status, status_verbosity verbosity,
           duration) -> caf::result<record> {
      return self->state().status(verbosity);
    },
  };
}

// -- serve handler -----------------------------------------------------------

using serve_handler_actor
  = typed_actor_fwd<>::extend_with<rest_handler_actor>::unwrap;

auto round_up_to_multiple(size_t numToRound, size_t multiple) -> size_t {
  const size_t remainder = numToRound % multiple;
  if (remainder == 0) {
    return numToRound;
  }
  return numToRound + multiple - remainder;
}

struct serve_handler_state {
  static constexpr auto name = "serve-handler";

  serve_handler_actor::pointer self = {};
  serve_manager_actor serve_manager = {};

  struct parse_error {
    std::string message;
    caf::error detail;
  };

  // Extracts `serve_id` and `continuation_token` by moving out of `params`
  static auto try_extract_request_base(tenzir::record& params)
    -> std::variant<request_base, parse_error> {
    auto result = request_base{};
    const auto serve_id = try_get_only<std::string>(params, "serve_id");
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
    return result;
  }

  static auto try_extract_request_meta(const tenzir::record& params)
    -> std::variant<request_meta, parse_error> {
    auto result = request_meta{};
    auto max_events = try_get<uint64_t>(params, "max_events");
    if (not max_events) {
      return parse_error{
        .message = "failed to read max_events",
        .detail = caf::make_error(ec::invalid_argument,
                                  fmt::format("parameter: {}; got params {}",
                                              max_events.error(), params))};
    }
    if (*max_events) {
      result.max_events = **max_events;
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
      result.min_events = **min_events;
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
      result.timeout = **timeout;
    }
    auto schema = try_get<std::string>(params, "schema");
    if (not schema) {
      auto detail_msg
        = fmt::format("{}; got params {}", schema.error(), params);
      auto detail
        = caf::make_error(ec::invalid_argument, std::move(detail_msg));
      return parse_error{.message = "failed to read schema parameter",
                         .detail = std::move(detail)};
    }
    if (*schema) {
      auto opt = from_string<enum schema>(**schema);
      if (not opt) {
        return parse_error{
          .message = "invalid schema parameter",
          .detail = caf::make_error(ec::invalid_argument,
                                    fmt::format("got `{}`", **schema))};
      }
      result.schema = *opt;
    }
    return result;
  }

  /// Validates a request to /serve and turns it into a structured form
  static auto try_parse_single_request(tenzir::record params)
    -> std::variant<single_serve_request, parse_error> {
    auto base = try_extract_request_base(params);
    if (auto* err = try_as<parse_error>(base)) {
      return std::move(*err);
    }
    auto meta = try_extract_request_meta(params);
    if (auto* err = try_as<parse_error>(meta)) {
      return std::move(*err);
    }
    return single_serve_request{
      std::move(as<request_base>(base)),
      std::move(as<request_meta>(meta)),
    };
  }

  /// Validates a request to /serve-multi and turns it into a structured form
  static auto try_parse_multi_request(tenzir::record params)
    -> std::variant<multi_serve_request, parse_error> {
    auto meta = try_extract_request_meta(params);
    if (auto* err = try_as<parse_error>(meta)) {
      return std::move(*err);
    }
    auto requests = std::vector<request_base>{};
    auto it = params.find("requests");
    if (it == params.end()) {
      return parse_error{
        .message = "missing field `requests`",
        .detail = caf::make_error(ec::invalid_argument),
      };
    }
    auto* const l = try_as<tenzir::list>(it->second);
    if (not l) {
      return parse_error{
        .message = "expected `requests` to be a list",
        .detail = caf::make_error(ec::invalid_argument),
      };
    }
    if (l->empty()) {
      return parse_error{
        .message = "expected `requests` to have at least one element",
        .detail = caf::make_error(ec::invalid_argument),
      };
    }
    requests.reserve(l->size());
    for (auto& e : *l) {
      auto* const r = try_as<tenzir::record>(e);
      if (not r) {
        return parse_error{
          .message = "expected `requests` to be a list of records",
          .detail = caf::make_error(ec::invalid_argument),
        };
      }
      auto parsed = try_extract_request_base(*r);
      if (auto* err = try_as<parse_error>(parsed)) {
        return std::move(*err);
      }
      auto& new_request = as<request_base>(parsed);
      const auto is_duplicate = std::ranges::contains(
        requests, new_request.serve_id, &request_base::serve_id);
      if (is_duplicate) {
        return parse_error{
          .message
          = fmt::format("duplicate `serve_id`: `{}`", new_request.serve_id),
          .detail = caf::make_error(ec::invalid_argument),
        };
      }
      requests.push_back(std::move(new_request));
    }
    return multi_serve_request{
      std::move(as<request_meta>(meta)),
      std::move(requests),
    };
  }

  /// Creates a response string for a single serve result
  static auto create_response(const std::string& next_continuation_token,
                              const std::vector<table_slice>& results,
                              serve_state state, enum schema schema)
    -> std::string {
    auto printer = json_printer{{
      .indentation = 0,
      .oneline = true,
      .numeric_durations = true,
    }};
    auto result
      = next_continuation_token.empty()
          ? fmt::format(
              R"({{"next_continuation_token":null,"state":"{}","events":[)",
              state)
          : fmt::format(
              R"({{"next_continuation_token":"{}","state":"{}","events":[)",
              next_continuation_token, state);
    auto out_iter = std::back_inserter(result);
    auto seen_types = std::unordered_set<type>{};
    bool first = true;
    for (const auto& slice : results) {
      if (slice.rows() == 0) {
        continue;
      }
      seen_types.insert(slice.schema());
      auto resolved_slice = resolve_enumerations(slice);
      auto type = as<record_type>(resolved_slice.schema());
      auto array = check(to_record_batch(resolved_slice)->ToStructArray());
      for (const auto& row : values(type, *array)) {
        if (first) {
          out_iter = fmt::format_to(out_iter, "{{");
        } else {
          out_iter = fmt::format_to(out_iter, "}},{{");
        }
        first = false;
        out_iter = fmt::format_to(out_iter, R"("schema_id":"{}","data":)",
                                  slice.schema().make_fingerprint());
        TENZIR_ASSERT(row);
        const auto ok = printer.print(out_iter, *row);
        TENZIR_ASSERT(ok);
      }
    }
    if (schema == schema::never) {
      if (not seen_types.empty()) {
        *out_iter++ = '}';
      }
      *out_iter++ = ']';
      *out_iter++ = '}';
      return result;
    }
    // Write schemas
    if (seen_types.empty()) {
      out_iter = fmt::format_to(out_iter, R"(],"schemas":[]}}{})", '\n');
      return result;
    }
    out_iter = fmt::format_to(out_iter, R"(}}],"schemas":[)");
    for (bool first = true; const auto& type : seen_types) {
      if (first) {
        out_iter = fmt::format_to(out_iter, "{{");
      } else {
        out_iter = fmt::format_to(out_iter, "}},{{");
      }
      first = false;
      out_iter = fmt::format_to(out_iter, R"("schema_id":"{}","definition":)",
                                type.make_fingerprint());
      const auto ok = printer.print(out_iter, schema == schema::legacy
                                                ? type.to_legacy_definition()
                                                : type.to_definition());
      TENZIR_ASSERT(ok);
    }
    out_iter = fmt::format_to(out_iter, R"(}}]}})");
    return result;
  }

  /// Handles a request to /serve by
  /// * "parsing" `params`
  /// * Making a request to the serve-manager for events according to `params`
  /// * Delivering a response based on the server-managers answer
  auto handle_single_request(tenzir::record params) const
    -> caf::result<rest_response> {
    auto maybe_request = try_parse_single_request(params);
    if (auto* err = try_as<parse_error>(maybe_request)) {
      return rest_response::make_error(400, std::move(err->message),
                                       std::move(err->detail));
    }
    auto& request = as<single_serve_request>(maybe_request);
    auto rp = self->make_response_promise<rest_response>();
    self
      ->mail(atom::get_v, std::move(request.serve_id),
             std::move(request.continuation_token), request.min_events,
             request.timeout, request.max_events)
      .request(serve_manager, caf::infinite)
      .then(
        [rp, schema = request.schema](const serve_response& result) mutable {
          const auto& [continuation_token, results] = result;
          rp.deliver(rest_response::from_json_string(
            create_response(continuation_token, results,
                            continuation_token.empty() ? serve_state::completed
                                                       : serve_state::running,
                            schema)));
        },
        [rp, schema = request.schema](const caf::error& err) mutable {
          if (err == caf::exit_reason::user_shutdown
              or err.context().match_elements<diagnostic>()) {
            // The pipeline has either shut down naturally or we got an
            // error that's a diagnostic. In either case, do not report the
            // error as an internal error from the /serve endpoint, but
            // rather report that we're done. The user must get the
            // diagnostic from the `diagnostics` operator.
            rp.deliver(rest_response::from_json_string(create_response(
              {}, {},
              err == caf::exit_reason::user_shutdown ? serve_state::completed
                                                     : serve_state::failed,
              schema)));
            return;
          }
          rp.deliver(rest_response::make_error(400, fmt::to_string(err), {}));
        });
    return rp;
  }

  struct serve_response_with_state {
    serve_response response;
    serve_state state;
  };

  /// Handles a request to /serve-multi by
  /// * "parsing" `params`
  /// * Performing a fanout over all `serve_id` in params.requests, making a
  ///   request to the serve-manager for each
  /// * Collecting all answers from the serve-manager
  /// * Creating a response and delivering it.
  auto handle_multi_request(tenzir::record params) const
    -> caf::result<rest_response> {
    auto maybe_requests = try_parse_multi_request(params);
    if (auto* err = try_as<parse_error>(maybe_requests)) {
      return rest_response::make_error(400, std::move(err->message),
                                       std::move(err->detail));
    }
    auto& request = as<multi_serve_request>(maybe_requests);
    auto rp = self->make_response_promise<rest_response>();
    const auto min_events_per_request
      = round_up_to_multiple(request.min_events, request.requests.size())
        / request.requests.size();
    const auto max_events_per_request
      = round_up_to_multiple(request.max_events, request.requests.size())
        / request.requests.size();
    auto result_map = std::make_shared<
      std::unordered_map<std::string, serve_response_with_state>>();
    auto fan = detail::make_fanout_counter(
      request.requests.size(),
      [rp, result_map, schema = request.schema]() mutable {
        auto json_text = std::string{'{'};
        auto first = true;
        for (auto& [id, result] : *result_map) {
          const auto& [response, state] = result;
          const auto& [next_token, data] = response;
          if (not first) {
            json_text += ',';
          }
          first = false;
          json_text += "\"" + id + "\":";
          json_text += create_response(next_token, data, state, schema);
        }
        json_text += '}';
        rp.deliver(rest_response::from_json_string(json_text));
      },
      [rp](caf::error e) mutable {
        rp.deliver(rest_response::make_error(400, fmt::to_string(e), {}));
      });
    for (auto& r : request.requests) {
      self
        ->mail(atom::get_v, r.serve_id, r.continuation_token,
               min_events_per_request, request.timeout, max_events_per_request)
        .request(serve_manager, caf::infinite)
        .then(
          [fan, id = r.serve_id, result_map](serve_response& result) mutable {
            const auto state = std::get<0>(result).empty()
                                 ? serve_state::completed
                                 : serve_state::running;
            const auto [_, success] = result_map->try_emplace(
              std::move(id), std::move(result), state);
            TENZIR_ASSERT(success);
            fan->receive_success();
          },
          [fan, id = r.serve_id, result_map](caf::error& err) mutable {
            if (err == caf::exit_reason::user_shutdown
                or err.context().match_elements<diagnostic>()) {
              // The pipeline has either shut down naturally or we got an
              // error that's a diagnostic. In either case, do not report
              // the error as an internal error from the /serve endpoint,
              // but rather report that we're done. The user must get the
              // diagnostic from the `diagnostics` operator.
              const auto state = err == caf::exit_reason::user_shutdown
                                   ? serve_state::completed
                                   : serve_state::failed;
              const auto [_, success] = result_map->try_emplace(
                std::move(id), serve_response{}, state);
              TENZIR_ASSERT(success);
              fan->receive_success();
              return;
            }
            fan->receive_error(std::move(err));
          });
    }
    return rp;
  }

  auto http_request(uint64_t endpoint_id, tenzir::record params) const
    -> caf::result<rest_response> {
    switch (endpoint_id) {
      case serve_endpoint_id:
        return handle_single_request(std::move(params));
      case serve_multi_endpoint_id:
        return handle_multi_request(std::move(params));
    }
    TENZIR_UNREACHABLE();
  }
};

auto serve_handler(
  serve_handler_actor::stateful_pointer<serve_handler_state> self,
  const node_actor& node) -> serve_handler_actor::behavior_type {
  self->state().self = self;
  self
    ->mail(atom::get_v, atom::label_v,
           std::vector<std::string>{"serve-manager"})
    .request(node, caf::infinite)
    .await([self](std::vector<caf::actor>& actors) {
      TENZIR_ASSERT(actors.size() == 1);
      self->state().serve_manager
        = caf::actor_cast<serve_manager_actor>(actors.front());
    });
  return {
    [self](atom::http_request, uint64_t endpoint_id,
           tenzir::record& params) -> caf::result<rest_response> {
      return self->state().http_request(endpoint_id, std::move(params));
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
    auto serve_manager
      = ctrl.self().system().registry().get<serve_manager_actor>(
        "tenzir.serve-manager");
    // Register this operator at SERVE MANAGER actor using the serve_id.
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::start_v, serve_id_, buffer_size_)
      .request(serve_manager, caf::infinite)
      .then(
        [&]() {
          ctrl.set_waiting(false);
          TENZIR_DEBUG("serve for id {} is now available",
                       escape_operator_arg(serve_id_));
        },
        [&](const caf::error& err) { //
          diagnostic::error(err)
            .note("failed to register at serve-manager")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    // Forward events to the SERVE MANAGER.
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Send slice to SERVE MANAGER.
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::put_v, serve_id_, std::move(slice))
        .request(serve_manager, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](const caf::error& err) {
            diagnostic::error(err)
              .note("failed to buffer events at serve-manager")
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    //  Wait until all events were fetched.
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::stop_v, serve_id_)
      .request(serve_manager, caf::infinite)
      .then(
        [&]() {
          ctrl.set_waiting(false);
        },
        [&](const caf::error& err) {
          diagnostic::error(err)
            .note("failed to deregister at serve-manager")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
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
                     public virtual operator_plugin2<serve_operator>,
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
      ->mail(atom::get_v, atom::label_v,
             std::vector<std::string>{"serve-manager"})
      .request(ctrl.node(), caf::infinite)
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
      ->mail(atom::status_v, status_verbosity::debug,
             duration{std::chrono::seconds{10}})
      .request(serve_manager, caf::infinite)
      .receive(
        [&](record& response) {
          TENZIR_ASSERT(response.size() == 1);
          TENZIR_ASSERT(response.contains("requests"));
          TENZIR_ASSERT(is<list>(response["requests"]));
          serves = std::move(as<list>(response["requests"]));
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
    if (version != api_version::v0) {
      return tenzir::record{};
    }
    auto maybe_serve = from_yaml(serve_spec);
    TENZIR_ASSERT(maybe_serve, fmt::to_string(maybe_serve.error()).c_str());
    TENZIR_ASSERT(is<record>(*maybe_serve));
    auto maybe_serve_multi = from_yaml(serve_multi_spec);
    TENZIR_ASSERT(maybe_serve_multi,
                  fmt::to_string(maybe_serve_multi.error()).c_str());
    TENZIR_ASSERT(is<record>(*maybe_serve_multi));
    auto res = record{};
    for (auto& [k, v] : as<record>(*maybe_serve)) {
      const auto [_, success] = res.try_emplace(std::move(k), std::move(v));
      TENZIR_ASSERT(success);
    }
    for (auto& [k, v] : as<record>(*maybe_serve_multi)) {
      const auto [_, success] = res.try_emplace(std::move(k), std::move(v));
      TENZIR_ASSERT(success);
    }
    return res;
  }

  auto rest_endpoints() const -> const std::vector<rest_endpoint>& override {
    const static auto endpoints = std::vector<tenzir::rest_endpoint>{
      {
        .endpoint_id = serve_endpoint_id,
        .method = http_method::post,
        .path = "/serve",
        .params = record_type{
          {"serve_id", type{string_type{}, {{"required"}}}},
          {"continuation_token", string_type{}},
          {"max_events", uint64_type{}},
          {"min_events", uint64_type{}},
          {"timeout", duration_type{}},
          {"schema", string_type{}},
        },
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id = serve_multi_endpoint_id,
        .method = http_method::post,
        .path = "/serve-multi",
        .params = record_type{
          {"requests", type{list_type{
            record_type{
              {"serve_id", type{string_type{}, {{"required"}}}},
              {"continuation_token", string_type{}},
            },
          },{{"required"}}}},
          {"max_events", uint64_type{}},
          {"min_events", uint64_type{}},
          {"timeout", duration_type{}},
          {"schema", string_type{}},
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

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto id = located<std::string>{};
    auto buffer_size = std::optional<located<uint64_t>>{};
    argument_parser2::operator_("serve")
      .positional("id", id)
      .named("buffer_size", buffer_size)
      .parse(inv, ctx)
      .ignore();
    if (id.inner.empty()) {
      diagnostic::error("serve id must not be empty")
        .primary(id.source)
        .emit(ctx);
    }
    if (buffer_size and buffer_size->inner == 0) {
      diagnostic::error("buffer size must not be zero")
        .primary(buffer_size->source)
        .emit(ctx);
    }
    return std::make_unique<serve_operator>(
      std::move(id.inner),
      buffer_size ? buffer_size->inner : defaults::api::serve::max_events);
  }
};

} // namespace

} // namespace tenzir::plugins::serve

TENZIR_REGISTER_PLUGIN(tenzir::plugins::serve::plugin)
