//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/command.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/numeric.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/detail/weak_run_delayed.hpp>
#include <vast/format/json.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/actors.hpp>
#include <vast/system/builtin_rest_endpoints.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/table_slice.hpp>

#include <arrow/record_batch.h>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::query {

static auto const* SPEC_V0 = R"_(
/query/new:
  post:
    summary: Create new query
    description: Create a new export query in VAST
    parameters:
      - in: query
        name: query
        schema:
          type: string
        example: "where :ip in 10.42.0.0/16 | head 100"
        required: true
        description: |
          The query used in the form of a pipeline.
      - in: query
        name: ttl
        schema:
          type: string
        example: "5 minutes"
        required: false
        description: |
          The time after which a query is cancelled. Use the /query/:id/next
          endpoint to refresh the TTL. To refresh the TTL without requesting
          further events, request zero events.
      - in: query
        name: expand
        schema:
          type: boolean
        example: false
        required: false
        description: |
          Whether to use the expanded output schema.
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
        description: Success.
        content:
          application/json:
            schema:
              type: object
              example:
                id: c91019bf-21fe-4999-8323-4d28aeb111ab
              properties:
                id:
                  type: string
      401:
        description: Not authenticated.
      422:
        description: Invalid expression or invalid lifetime.

/query/{id}/next:
  get:
    summary: Get additional query results
    description: Return `n` additional results from the specified query.
    parameters:
      - in: path
        name: id
        schema:
          type: string
        required: true
        example: e84308a2-1ba4-4559-9e0f-597dfea4fd3e
        description: The query ID.
      - in: query
        name: n
        schema:
          type: integer
        required: false
        example: 10
        description: Maximum number of returned events
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              properties:
                events:
                  type: array
                  items:
                    type: object
                  description: |
                    The returned events, including a schema-ref that uniquely
                    identifies the schema for each row.
                schemas:
                  type: array
                  items:
                    type: object
                  description: |
                    The schemas referenced in the events section of the same
                    reply, using the same format as the `vast show schemas`
                    command.
              example:
                events:
                  - schema-ref: "foobarbaz"
                    data: {"ts": "2009-11-18T22:11:04.011822", "uid": "iKxhjl8i1n3", "id": {"orig_h": "192.168.1.103"}}
                  - schema-ref: "foobarbaz"
                    data: {"ts": "2009-11-18T22:11:04.011822", "uid": "iKxhjl8i1n3", "id": {"orig_h": "192.168.1.103"}}
                schemas:
                  - schema-ref: "foobarbaz"
                    definition: <type-definition>
      401:
        description: Not authenticated.
      422:
        description: Invalid arguments.
    )_";

/// An actor to help with handling a single query.
using query_manager_actor = system::typed_actor_fwd<
  // Initiate a query.
  auto(atom::provision, system::query_cursor)->caf::result<void>,
  // Get the next results for a query.
  auto(atom::next, http_request, uint64_t)->caf::result<atom::done>,
  // Finish a query.
  auto(atom::done)->caf::result<void>>
  // Conform to the protocol of a RECEIVER ACTOR of table slices.
  ::extend_with<system::receiver_actor<table_slice>>::unwrap;

/// An actor to receive REST endpoint requests and spawn exporters
/// as needed.
using request_multiplexer_actor = system::typed_actor_fwd<>
  // Conform to the protocol of the REST HANDLER actor.
  ::extend_with<system::rest_handler_actor>::unwrap;

namespace {

constexpr auto BATCH_SIZE = uint32_t{1};

struct query_format_options {
  bool flatten{defaults::rest::query::flatten};
  bool numeric_durations{defaults::rest::query::numeric_durations};
  bool omit_nulls{defaults::rest::query::omit_nulls};
};

} // namespace

struct query_next_state {
  size_t limit = 0u;
  http_request request;
  caf::typed_response_promise<atom::done> promise = {};
};

struct query_manager_state {
  query_manager_state() = default;

  static constexpr auto name = "query-manager";

  query_manager_actor::pointer self;
  system::index_actor index = {};
  query_format_options format_opts = {};
  bool expand = {};
  duration ttl = {};
  caf::disposable ttl_disposable = {};
  std::deque<table_slice> source_buffer;
  std::deque<table_slice> sink_buffer;
  size_t shippable_events_count = 0;
  std::optional<system::query_cursor> cursor = std::nullopt;
  size_t processed_partitions = 0u;
  bool active_index_query = false;
  std::deque<query_next_state> nexts;
  generator<caf::expected<void>> executor;
  generator<caf::expected<void>>::iterator executor_it = {};

  void refresh_ttl() {
    // Zero TTL = no TTL at all. This is a requirement for the unit tests, which
    // use a deterministic clock that does not play well with the TTL.
    if (ttl == duration::zero())
      return;
    // Cancel the old TTL timeout one.
    if (ttl_disposable.valid()) {
      if (!ttl_disposable.disposed())
        ttl_disposable.dispose();
      else
        VAST_WARN("{} refreshes TTL that was already disposed", *self);
    }
    // Create a new one.
    ttl_disposable = detail::weak_run_delayed(self, ttl, [this] {
      VAST_VERBOSE("{} quits after TTL of {} expired", *self, data{ttl});
      self->quit();
    });
  }

  auto create_response(const query_next_state& next) -> std::string {
    auto printer
      = json_printer{{.oneline = true,
                      .flattened = format_opts.flatten,
                      .numeric_durations = format_opts.numeric_durations,
                      .omit_nulls = format_opts.omit_nulls}};
    auto result = std::string{"{\"events\":["};
    auto out_iter = std::back_inserter(result);
    auto seen_schemas = std::unordered_set<type>{};
    auto written = size_t{0};
    // write slices
    auto it = sink_buffer.begin();
    for (bool first = true; it != sink_buffer.end(); ++it) {
      auto slice = std::move(*it);
      if (slice.rows() == 0)
        continue;
      if (const auto remaining = next.limit - written;
          slice.rows() > remaining) {
        auto [head, tail] = split(slice, remaining);
        *it = std::move(tail);
        slice = std::move(head);
      }
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
        out_iter = fmt::format_to(out_iter, "\"schema-ref\":\"{}\",\"data\":",
                                  slice.schema().make_fingerprint());
        VAST_ASSERT_CHEAP(row);
        const auto ok = printer.print(out_iter, *row);
        VAST_ASSERT_CHEAP(ok);
      }
      written += slice.rows();
      if (written >= next.limit) {
        break;
      }
    }
    //  Remove events that will be shipped in the response.
    sink_buffer.erase(sink_buffer.begin(), it);
    // Write schemas
    if (written == 0) {
      out_iter = fmt::format_to(out_iter, "],\"schemas\":[]}}\n");
      return result;
    }
    out_iter = fmt::format_to(out_iter, "}}],\"schemas\":[");
    for (bool first = true; const auto& schema : seen_schemas) {
      if (first)
        out_iter = fmt::format_to(out_iter, "{{");
      else
        out_iter = fmt::format_to(out_iter, "}},{{");
      first = false;
      out_iter
        = fmt::format_to(out_iter, "\"schema-ref\":\"{}\",\"definition\":",
                         schema.make_fingerprint());
      const auto ok = printer.print(out_iter, schema.to_definition(expand));
      VAST_ASSERT_CHEAP(ok);
    }
    out_iter = fmt::format_to(out_iter, "}}]}}\n");
    shippable_events_count -= written;
    return result;
  }

  void run_executor(const query_next_state& next) {
    VAST_DEBUG("query: entering executor");
    while (true) {
      if (executor_exhausted()) {
        // The executor can always become exhausted, for example due to `head`.
        VAST_DEBUG("query: leaving due to exhausted executor");
        return;
      }
      if (enough_shippable_events(next)) {
        // We could continue even when we have enough shippable events, but
        // choose not to in case the user will not request more.
        VAST_DEBUG("query: leaving due enough events");
        return;
      }
      if (source_buffer.empty() && active_index_query) {
        // If the source buffer is empty, we want to continue until the source
        // requests more data from the index. If the index is exhausted, the
        // source will never make a request and becomes exhausted instead.
        VAST_DEBUG("query: leaving due to empty source && active index query");
        return;
      }
      auto result = std::move(*executor_it);
      VAST_DEBUG("query: advancing executor");
      ++executor_it;
      if (!result) {
        // This will abort the execution. We currently do not inform the query
        // consumer of this error, but might want to change this in the future.
        VAST_WARN("error while applying pipeline: {}", result.error());
      }
    }
  }

  void run() {
    // We have to wait until the cursor is provisioned.
    if (!cursor) {
      return;
    }
    while (!nexts.empty()) {
      auto& next = nexts.front();
      run_executor(next);
      if (!should_ship_results(next)) {
        return;
      }
      VAST_DEBUG("query: shipping results ({} available)",
                 shippable_events_count);
      VAST_ASSERT(next.request.response);
      VAST_ASSERT(next.promise.pending());
      next.request.response->append(create_response(next));
      next.promise.deliver(atom::done_v);
      nexts.pop_front();
      // Possibly continue, because we might be able to fullfil another request.
    }
  }

  auto index_exhausted() const -> bool {
    return cursor && cursor->candidate_partitions == processed_partitions;
  }

  auto enough_shippable_events(const query_next_state& next) const -> bool {
    if (nexts.empty()) {
      return false;
    }
    return shippable_events_count >= next.limit;
  }

  auto executor_exhausted() const -> bool {
    return executor_it == executor.end();
  }

  auto should_ship_results(const query_next_state& next) const -> bool {
    return enough_shippable_events(next) || executor_exhausted();
  }
};

using manager_ptr = query_manager_actor::stateful_pointer<query_manager_state>;

class query_source final : public crtp_operator<query_source> {
public:
  explicit query_source(manager_ptr manager) : manager_(manager) {
  }

  auto operator()() const -> generator<table_slice> {
    auto& state = manager_->state;
    while (true) {
      if (state.source_buffer.empty()) {
        if (state.index_exhausted()) {
          break;
        }
        if (!state.active_index_query && state.cursor) {
          VAST_DEBUG("query_source: sending query to index");
          manager_->send(state.index, atom::query_v, state.cursor->id,
                         BATCH_SIZE);
          state.active_index_query = true;
        }
        VAST_DEBUG("query_source: stalling");
        co_yield {};
      } else {
        VAST_DEBUG("query_source: popping element from queue");
        auto slice = std::move(state.source_buffer.front());
        state.source_buffer.pop_front();
        co_yield std::move(slice);
      }
    }
    VAST_DEBUG("query_source: done");
  }

  auto to_string() const -> std::string override {
    return "query_source";
  }

private:
  manager_ptr manager_;
};

class query_sink final : public crtp_operator<query_sink> {
public:
  explicit query_sink(manager_ptr manager) : manager_(manager) {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<std::monostate> {
    auto& state = manager_->state;
    for (auto&& slice : input) {
      if (slice.rows() > 0) {
        VAST_DEBUG("query_sink: putting result in sink buffer");
        state.sink_buffer.push_back(std::move(slice));
      }
      co_yield {};
    }
  }

  auto to_string() const -> std::string override {
    return "query_sink";
  }

private:
  manager_ptr manager_;
};

struct request_multiplexer_state {
  request_multiplexer_state() = default;

  static constexpr auto name = "request-multiplexer";

  system::index_actor index_ = {};
  std::unordered_map<std::string, query_manager_actor> live_queries_ = {};
};

query_manager_actor::behavior_type
query_manager(query_manager_actor::stateful_pointer<query_manager_state> self,
              system::index_actor index, pipeline open_pipeline, bool expand,
              duration ttl, query_format_options format_opts) {
  VAST_VERBOSE("{} starts with a TTL of {}", *self, data{ttl});
  self->state.self = self;
  self->state.index = std::move(index);
  self->state.expand = expand;
  self->state.ttl = ttl;
  self->state.format_opts = format_opts;
  auto ops = std::move(open_pipeline).unwrap();
  ops.insert(ops.begin(), std::make_unique<query_source>(self));
  ops.push_back(std::make_unique<query_sink>(self));
  auto result = pipeline{std::move(ops)};
  VAST_DEBUG("final query pipeline: {}", result.to_string());
  // We already checked that the original pipeline was `events -> events`.
  // Thus, we know that we now have a valid `void -> void` pipeline.
  VAST_ASSERT(result.is_closed());
  self->state.executor = make_local_executor(std::move(result));
  self->state.executor_it = self->state.executor.begin();
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    while (!self->state.nexts.empty()) {
      auto& next = self->state.nexts.front();
      if (next.promise.pending()) {
        next.promise.deliver(msg.reason);
      }
      self->state.nexts.pop_front();
    }
    self->quit();
  });
  return {
    [self](atom::provision, system::query_cursor cursor) {
      VAST_DEBUG("query: provision");
      self->state.refresh_ttl();
      self->state.cursor = cursor;
      VAST_DEBUG("query: provision done");
    },
    [self](atom::next, http_request& rq,
           uint64_t max_events_to_output) -> caf::result<atom::done> {
      VAST_DEBUG("query: received next request: {}", max_events_to_output);
      self->state.refresh_ttl();
      auto promise = self->make_response_promise<atom::done>();
      self->state.nexts.push_back(query_next_state{
        .limit = max_events_to_output,
        .request = std::move(rq),
        .promise = promise,
      });
      self->state.run();
      return promise;
    },
    // Index-facing API
    [self](vast::table_slice& slice) {
      VAST_DEBUG("query: received slice from index");
      self->state.source_buffer.push_back(std::move(slice));
      self->state.run();
    },
    [self](atom::done) {
      VAST_DEBUG("query: received done from index");
      // There's technically a race condition with atom::provision here,
      // since the index sends the first `done` asynchronously. But since
      // we always set `taste == 0`, we will not miss any data due to this.
      ++self->state.processed_partitions;
      self->state.active_index_query = false;
      self->state.run();
    },
  };
}

auto request_multiplexer(
  request_multiplexer_actor::stateful_pointer<request_multiplexer_state> self,
  const system::node_actor& node) -> request_multiplexer_actor::behavior_type {
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
  self->set_down_handler([self](const caf::down_msg& msg) {
    VAST_VERBOSE("{} received DOWN from {}: {}", *self, msg.source, msg.reason);
    auto it = std::find_if(self->state.live_queries_.begin(),
                           self->state.live_queries_.end(),
                           [addr = msg.source](const auto& query) {
                             return query.second->address() == addr;
                           });
    if (it == self->state.live_queries_.end()) {
      VAST_WARN("{} ignores received DOWN from an unknown actor {}: {}", *self,
                msg.source, msg.reason);
      return;
    }
    self->state.live_queries_.erase(it);
  });
  return {
    [self](atom::http_request, uint64_t endpoint_id_raw, http_request rq) {
      VAST_VERBOSE("{} handles /query request for endpoint id {} with params "
                   "{}",
                   *self, endpoint_id_raw, rq.params);
      auto endpoint_id = static_cast<system::query_endpoints>(endpoint_id_raw);
      switch (endpoint_id) {
        case system::query_endpoints::new_: {
          auto query_string = std::optional<std::string>{};
          auto expand = false;
          if (rq.params.contains("expand"))
            expand = caf::get<bool>(rq.params["expand"]);
          auto ttl = duration{std::chrono::minutes{5}};
          if (rq.params.contains("ttl"))
            ttl = caf::get<duration>(rq.params["ttl"]);
          auto format_opts = query_format_options{};
          if (rq.params.contains("flatten")) {
            auto& param = rq.params.at("flatten");
            VAST_ASSERT(caf::holds_alternative<bool>(param));
            format_opts.flatten = caf::get<bool>(param);
          }
          if (rq.params.contains("omit-nulls")) {
            auto& param = rq.params.at("omit-nulls");
            VAST_ASSERT(caf::holds_alternative<bool>(param));
            format_opts.omit_nulls = caf::get<bool>(param);
          }
          if (rq.params.contains("numeric-durations")) {
            auto& param = rq.params.at("numeric-durations");
            VAST_ASSERT(caf::holds_alternative<bool>(param));
            format_opts.numeric_durations = caf::get<bool>(param);
          }
          if (rq.params.contains("query")) {
            auto& param = rq.params.at("query");
            // Should be type-checked by the server.
            VAST_ASSERT(caf::holds_alternative<std::string>(param));
            query_string = caf::get<std::string>(param);
          } else {
            return rq.response->abort(422, "missing parameter 'query'\n",
                                      caf::error{});
          }
          auto parse_result = pipeline::parse(*query_string);
          if (!parse_result)
            return rq.response->abort(400, "invalid query\n",
                                      parse_result.error());
          auto pipeline = std::move(*parse_result);
          auto output = pipeline.infer_type<table_slice>();
          if (!output) {
            return rq.response->abort(400, "pipeline instantiation failed\n",
                                      output.error());
          }
          if (!output->is<table_slice>()) {
            return rq.response->abort(
              400, "query must return events as output\n",
              caf::make_error(ec::type_clash,
                              fmt::format("the given pipeline returns {}",
                                          operator_type_name(*output))));
          }
          // TODO: Consider replacing this with an empty conjunction.
          auto& trivially_true = trivially_true_expression();
          auto pushdown = pipeline.predicate_pushdown_pipeline(trivially_true);
          if (!pushdown) {
            pushdown.emplace(trivially_true, std::move(pipeline));
          }
          VAST_DEBUG("query: push down result: {}", *pushdown);
          auto [expr, new_pipeline] = std::move(*pushdown);
          VAST_ASSERT(expr != expression{});
          auto normalized_expr = normalize_and_validate(expr);
          if (!normalized_expr)
            return rq.response->abort(400, "invalid query\n",
                                      normalized_expr.error());
          auto handler
            = self->spawn<caf::monitored>(query_manager, self->state.index_,
                                          std::move(new_pipeline), expand, ttl,
                                          std::move(format_opts));
          auto query = vast::query_context::make_extract(
            "http-request", handler, std::move(*normalized_expr));
          query.taste = 0;
          self
            ->request(self->state.index_, caf::infinite, atom::evaluate_v,
                      query)
            .then(
              [self, handler,
               response = rq.response](system::query_cursor cursor) {
                auto id_string = fmt::format("{:l}", cursor.id);
                self->state.live_queries_[id_string] = handler;
                self->send(handler, atom::provision_v, cursor);
                response->append(
                  fmt::format("{{\"id\": \"{}\"}}\n", id_string));
              },
              [response = rq.response](const caf::error& e) {
                response->abort(500, "index evaluation failed\n", e);
              });
          break;
        }
        case system::query_endpoints::next: {
          if (!rq.params.contains("id"))
            return rq.response->abort(400, "missing id\n", caf::error{});
          if (!rq.params.contains("n"))
            return rq.response->abort(400, "missing parameter 'n'\n",
                                      caf::error{});
          auto id = caf::get<std::string>(rq.params["id"]);
          auto n = caf::get<uint64_t>(rq.params["n"]);
          auto it = self->state.live_queries_.find(id);
          if (it == self->state.live_queries_.end())
            return rq.response->abort(422, "unknown id\n", caf::error{});
          auto& handler = it->second;
          self->request(handler, caf::infinite, atom::next_v, std::move(rq), n)
            .then([](atom::done) { /* nop */ },
                  [response = rq.response](const caf::error& e) {
                    response->abort(500, "internal server error\n", e);
                  });
          break;
        }
        default:
          // If we get here there's a bug in the server.
          VAST_ASSERT_CHEAP(false, "unknown endpoint id");
      }
    },
  };
}

class plugin final : public virtual rest_endpoint_plugin {
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "api-query";
  };

  [[nodiscard]] std::string prefix() const override {
    return "";
  }

  [[nodiscard]] data openapi_specification(api_version version) const override {
    if (version != api_version::v0)
      return vast::record{};
    auto result = from_yaml(SPEC_V0);
    VAST_ASSERT(result);
    return *result;
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] const std::vector<rest_endpoint>&
  rest_endpoints() const override {
    static auto endpoints = std::vector<vast::rest_endpoint>{
      {
        .endpoint_id = static_cast<uint64_t>(system::query_endpoints::new_),
        .method = http_method::post,
        .path = "/query/new",
        .params = vast::record_type{
          {"query", vast::string_type{}},
          {"flatten", vast::bool_type{}},
          {"omit-nulls", vast::bool_type{}},
          {"numeric-durations", vast::bool_type{}},
          {"expand", vast::bool_type{}},
          {"ttl", vast::duration_type{}},
        },
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id = static_cast<uint64_t>(system::query_endpoints::next),
        .method = http_method::get,
        .path = "/query/:id/next",
        .params = vast::record_type{
          {"id", vast::string_type{}},
          {"n", vast::uint64_type{}},
        },
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
    };
    return endpoints;
  }

  auto handler(caf::actor_system& system, system::node_actor node) const
    -> system::rest_handler_actor override {
    return system.spawn(request_multiplexer, node);
  }
};

} // namespace vast::plugins::rest_api::query

VAST_REGISTER_PLUGIN(vast::plugins::rest_api::query::plugin)
