//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
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

#include <vast/actors.hpp>
#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/numeric.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/detail/weak_run_delayed.hpp>
#include <vast/format/json.hpp>
#include <vast/node.hpp>
#include <vast/node_control.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/query_cursor.hpp>
#include <vast/status.hpp>
#include <vast/table_slice.hpp>

#include <arrow/record_batch.h>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::serve {

namespace {

constexpr auto SERVE_ENDPOINT_ID = 0;

constexpr auto SPEC_V0 = R"_(
/serve:
  post:
    summary: Return data from a pipeline
    description: Returns events from an existing pipeline. The pipeline definition must include a serve operator.
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
                example: 50
                description: The maximum number of events returned. If unset, the number is unlimited.
              timeout:
                type: string
                example: "100ms"
                default: "100ms"
                description: The maximum amount of time spent on the request. Hitting the timeout is not an error. The timeout must not be greater than 5 seconds.
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
                data:
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
       uint64_t limit, duration timeout)
    ->caf::result<std::tuple<std::string, std::vector<table_slice>>>>
  // Conform to the protocol of the COMPONENT PLUGIN actor interface.
  ::extend_with<component_plugin_actor>::unwrap;

struct serve_request {
  std::string serve_id = {};
  std::string continuation_token = {};
  uint64_t limit = std::numeric_limits<uint64_t>::max();
  duration timeout = std::chrono::milliseconds{100};
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
    VAST_ASSERT(get_rp.pending());
    // If we throttled the serve operator, then we can continue its operation
    // again if we have less events buffered than desired.
    if (put_rp.pending() and rows(buffer) < std::max(buffer_size, requested)) {
      put_rp.deliver();
    }
    // Avoid delivering too early, i.e., when we don't yet have enough events.
    const auto return_underful = stop_rp.pending() or force_underful;
    if (not return_underful and rows(buffer) < requested) {
      return false;
    }
    // Cut the results buffer.
    auto results = std::vector<table_slice>{};
    std::tie(results, buffer) = split(buffer, requested);
    delivered += rows(results);
    // Clear the delayed attempt and the continuation token.
    delayed_attempt.dispose();
    requested = 0;
    last_continuation_token = std::exchange(continuation_token, {});
    last_results = results;
    // If the pipeline is at its end then we must not assign a new token, but
    // rather end here.
    if (stop_rp.pending() and buffer.empty()) {
      VAST_ASSERT(not put_rp.pending());
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
    VAST_VERBOSE("serve for id {} is now available with continuation token {}",
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

  auto handle_down_msg(const caf::down_msg& msg) -> void {
    const auto found
      = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
          return op.source == msg.source;
        });
    if (found == ops.end()) {
      VAST_WARN("{} received unepexted DOWN from {}: {}", *self, msg.source,
                msg.reason);
      return;
    }
    if (not found->continuation_token.empty()) {
      VAST_DEBUG("{} received premature DOWN for serve id {} with continuation "
                 "token {}",
                 *self, found->serve_id, found->continuation_token);
    }
    // We delay the actual removal by 1 minute because we support fetching the
    // last set of events again by reusing the last continuation token.
    found->done = true;
    detail::weak_run_delayed(
      self, std::chrono::minutes{1}, [this, source = msg.source]() {
        const auto found
          = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
              return op.source == source;
            });
        if (found != ops.end()) {
          ops.erase(found);
        }
      });
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
    const auto buffered = rows(found->buffer);
    if (buffered == 0) {
      return {};
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
        VAST_DEBUG("{} delivered results eagerly for serve id {}", *self,
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
      return caf::make_error(ec::invalid_argument,
                             fmt::format("{} got request for events with "
                                         "unknown for serve id {}",
                                         *self, request.serve_id));
    }
    if ((found->done or not found->continuation_token.empty())
        and found->last_continuation_token == request.continuation_token) {
      return std::make_tuple(found->continuation_token,
                             split(found->last_results, request.limit).first);
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
    found->requested = request.limit;
    const auto delivered = found->try_deliver_results(false);
    if (delivered) {
      return found->get_rp;
    }
    found->delayed_attempt = detail::weak_run_delayed(
      self, request.timeout,
      [this, continuation_token = request.continuation_token]() mutable {
        const auto found
          = std::find_if(ops.begin(), ops.end(), [&](const auto& op) {
              return op.continuation_token == continuation_token;
            });
        if (found == ops.end()) {
          VAST_DEBUG("unable to find serve request after timeout expired");
          return;
        }
        const auto delivered = found->try_deliver_results(true);
        VAST_ASSERT(delivered);
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
           uint64_t limit, duration timeout)
      -> caf::result<std::tuple<std::string, std::vector<table_slice>>> {
      return self->state.get({
        .serve_id = std::move(serve_id),
        .continuation_token = std::move(continuation_token),
        .limit = limit,
        .timeout = timeout,
      });
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

  static auto try_parse_request(const vast::record& params)
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
      result.limit = **max_events;
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
      if (**timeout > std::chrono::seconds{5}) {
        auto detail = caf::make_error(
          ec::invalid_argument, fmt::format("got timeout {}", data{**timeout}));
        return parse_error{.message = "timeout exceeds limit of 5 seconds",
                           .detail = std::move(detail)};
      }
      result.timeout = **timeout;
    }
    return result;
  }

  static auto create_response(const std::string& next_continuation_token,
                              const std::vector<table_slice>& results)
    -> std::string {
    auto printer = json_printer{{
      .indentation = 0,
      .oneline = true,
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
        VAST_ASSERT_CHEAP(row);
        const auto ok = printer.print(out_iter, *row);
        VAST_ASSERT_CHEAP(ok);
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
        = printer.print(out_iter, schema.to_definition(/*expand*/ false));
      VAST_ASSERT_CHEAP(ok);
    }
    out_iter = fmt::format_to(out_iter, "}}]}}{}", '\n');
    return result;
  }

  auto http_request(uint64_t endpoint_id, vast::record params) const
    -> caf::result<rest_response> {
    if (endpoint_id != SERVE_ENDPOINT_ID) {
      return caf::make_error(ec::logic_error,
                             fmt::format("unepexted /serve endpoint id {}",
                                         endpoint_id));
    }
    VAST_DEBUG("{} handles /serve request for endpoint id {} with params {}",
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
                request.continuation_token, request.limit, request.timeout)
      .then(
        [rp](const std::tuple<std::string, std::vector<table_slice>>&
               result) mutable {
          rp.deliver(rest_response{
            create_response(std::get<0>(result), std::get<1>(result))});
        },
        [rp](caf::error& err) mutable {
          // TODO: Use a struct with distinct fields for user-facing
          // error message and detail here.
          auto rsp = rest_response::make_error(400, fmt::to_string(err), {});
          rp.deliver(std::move(rsp));
        });
    return rp;
  }
};

auto serve_handler(
  serve_handler_actor::stateful_pointer<serve_handler_state> self,
  const node_actor& node) -> serve_handler_actor::behavior_type {
  self->state.self = self;
  self->request(node, caf::infinite, atom::get_v, atom::type_v, "serve-manager")
    .await(
      [self](std::vector<caf::actor>& actors) {
        VAST_ASSERT(actors.size() == 1);
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
           vast::record& params) -> caf::result<rest_response> {
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
    // Step 1: Get a handle to the SERVE MANAGER actor.
    auto serve_manager = serve_manager_actor{};
    ctrl.self()
      .request(ctrl.node(), caf::infinite, atom::get_v, atom::type_v,
               "serve-manager")
      .await(
        [&](std::vector<caf::actor>& actors) {
          VAST_ASSERT(actors.size() == 1);
          serve_manager
            = caf::actor_cast<serve_manager_actor>(std::move(actors[0]));
        },
        [&](const caf::error& err) { //
          ctrl.abort(caf::make_error(
            ec::logic_error,
            fmt::format("failed to find serve-manager: {}", err)));
        });
    co_yield {};
    // Step 2: Register this operator at SERVE MANAGER actor using the serve_id.
    ctrl.self()
      .request(serve_manager, caf::infinite, atom::start_v, serve_id_,
               buffer_size_)
      .await(
        [&]() {
          VAST_VERBOSE("serve for id {} is now available",
                       escape_operator_arg(serve_id_));
        },
        [&](const caf::error& err) { //
          ctrl.abort(caf::make_error(
            ec::logic_error,
            fmt::format("failed to register at serve-manager: {}", err)));
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
            ctrl.abort(caf::make_error(ec::logic_error,
                                       fmt::format("failed to buffer events at "
                                                   "serve-manager: {}",
                                                   err)));
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
          ctrl.abort(caf::make_error(
            ec::logic_error,
            fmt::format("failed to deregister at serve-manager: {}", err)));
        });
    co_yield {};
  }

  auto to_string() const -> std::string override {
    return fmt::format("serve --buffer-size {} {}", buffer_size_,
                       escape_operator_arg(serve_id_));
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

  friend auto inspect(auto& f, serve_operator& x) -> bool {
    return f.apply(x.serve_id_) && f.apply(x.buffer_size_);
  }

private:
  std::string serve_id_ = {};
  uint64_t buffer_size_ = {};
};

// -- serve plugin ------------------------------------------------------------

class plugin final : public virtual component_plugin,
                     public virtual rest_endpoint_plugin,
                     public virtual operator_plugin<serve_operator> {
public:
  auto component_name() const -> std::string override {
    return "serve-manager";
  }

  auto make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor override {
    return node->spawn(serve_manager);
  }

  auto openapi_specification(api_version version) const -> data override {
    if (version != api_version::v0)
      return vast::record{};
    auto result = from_yaml(SPEC_V0);
    VAST_ASSERT(result);
    return *result;
  }

  auto rest_endpoints() const -> const std::vector<rest_endpoint>& override {
    static auto endpoints = std::vector<vast::rest_endpoint>{
      {
        .endpoint_id = SERVE_ENDPOINT_ID,
        .method = http_method::post,
        .path = "/serve",
        .params = record_type{
          {"serve_id", string_type{}},
          {"continuation_token", string_type{}},
          {"max_events", uint64_type{}},
          {"timeout", duration_type{}},
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

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::operator_arg, parsers::count;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = -(required_ws_or_comment >> "--buffer-size"
                     >> required_ws_or_comment >> count)
                   >> required_ws_or_comment >> operator_arg
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    auto buffer_size = std::optional<uint64_t>{};
    auto serve_id = std::string{};
    if (not p(f, l, buffer_size, serve_id)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse {} operator: '{}'", name(),
                                    pipeline)),
      };
    }
    if (serve_id.empty()) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse {} operator: serve-id "
                                    "must not be empty",
                                    pipeline)),
      };
    }
    if (buffer_size && *buffer_size == 0) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse {} operator: buffer-size "
                                    "must not be zero",
                                    pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<serve_operator>(std::move(serve_id),
                                       buffer_size.value_or(1 << 16)),
    };
  }
};

} // namespace

} // namespace vast::plugins::serve

VAST_REGISTER_PLUGIN(vast::plugins::serve::plugin)
