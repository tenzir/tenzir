//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/from_kafka.hpp"

#include <tenzir/async/mutex.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/controller/generated/api.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/platform_connection.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <fmt/format.h>
#include <librdkafka/rdkafka.h>

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins::kafka {

namespace {

struct IngestArgs {
  located<std::string> name;
  Option<located<std::string>> group;
  Option<located<ir::pipeline>> parser;
  let_id request;
  location operator_location = location::unknown;
};

auto describe(variant<AttachIngestEndpointError, CallError> const& error)
  -> std::string {
  return match(
    error,
    [](AttachIngestEndpointError const& declared) {
      return match(
        declared,
        [](NotFound const&) -> std::string {
          return "the endpoint does not exist, or this deployment may not "
                 "read it";
        },
        [](ServiceUnavailable const&) -> std::string {
          return "the platform cannot reach Kafka right now";
        },
        [](InternalServerError const&) -> std::string {
          return "the platform failed";
        },
        [](auto const& refused) -> std::string {
          return fmt::format("the platform refused the request: {}",
                             refused.message);
        });
    },
    [](CallError const& failed) {
      return match(
        failed,
        [](RequestError const& request) {
          return fmt::format("the platform is unreachable: {}",
                             request.message);
        },
        [](ResponseError const& response) {
          return fmt::format("the platform answered HTTP {} unexpectedly",
                             response.status);
        },
        [](ParseError const& parse) {
          return fmt::format("the platform's answer is invalid: {}",
                             parse.reason);
        });
    });
}

/// The Kafka credentials of this deployment, per endpoint, shared by every
/// `ingest` in the process.
///
/// The platform keeps one Kafka user per endpoint and deployment and gives it
/// a new password on every attach. So all readers of an endpoint use the
/// latest password, and every attach hands it to the consumers that already
/// run.
class Attachments {
public:
  static auto instance() -> Attachments& {
    static auto attachments = Attachments{};
    return attachments;
  }

  /// Serializes the attaches to an endpoint, so that the password stored last
  /// is also the one the platform issued last. The caller must hold a reader.
  auto lock(std::string const& name) -> Task<RawMutexGuard> {
    auto* attaching = static_cast<RawMutex*>(nullptr);
    {
      auto lock = std::scoped_lock{mutex_};
      auto endpoint = endpoints_.find(name);
      TENZIR_ASSERT(endpoint != endpoints_.end());
      attaching = std::addressof(endpoint->second.attaching);
    }
    co_return co_await attaching->lock();
  }

  /// Remembers the credentials of a fresh attachment and passes them on.
  auto store(std::string const& name,
             IngestEndpointAttachment const& attachment) -> void {
    auto lock = std::scoped_lock{mutex_};
    auto& endpoint = endpoints_[name];
    endpoint.username = attachment.username;
    if (endpoint.password != attachment.password) {
      endpoint.password = attachment.password;
      for (auto* consumer : endpoint.consumers) {
        update(*consumer, endpoint);
      }
    }
  }

  auto acquire(std::string const& name) -> void {
    auto lock = std::scoped_lock{mutex_};
    ++endpoints_[name].readers;
  }

  /// Forgets the endpoint with its last reader.
  auto release(std::string const& name) -> void {
    auto lock = std::scoped_lock{mutex_};
    auto endpoint = endpoints_.find(name);
    TENZIR_ASSERT(endpoint != endpoints_.end());
    if (--endpoint->second.readers == 0) {
      endpoints_.erase(endpoint);
    }
  }

  auto watch(std::string const& name, rd_kafka_t& consumer)
    -> std::shared_ptr<void> {
    auto lock = std::scoped_lock{mutex_};
    auto& endpoint = endpoints_[name];
    endpoint.consumers.push_back(std::addressof(consumer));
    // The consumer may have been configured with a password that an attach
    // replaced since.
    update(consumer, endpoint);
    return std::shared_ptr<void>{
      std::addressof(consumer), [this, name](rd_kafka_t* consumer) {
        auto lock = std::scoped_lock{mutex_};
        auto endpoint = endpoints_.find(name);
        if (endpoint != endpoints_.end()) {
          std::erase(endpoint->second.consumers, consumer);
        }
      }};
  }

private:
  struct Endpoint {
    std::string username;
    std::string password;
    std::vector<rd_kafka_t*> consumers;
    size_t readers = 0;
    RawMutex attaching;
  };

  static auto update(rd_kafka_t& consumer, Endpoint const& endpoint) -> void {
    auto* error = rd_kafka_sasl_set_credentials(std::addressof(consumer),
                                                endpoint.username.c_str(),
                                                endpoint.password.c_str());
    if (error) {
      TENZIR_WARN("ingest: failed to update Kafka credentials: {}",
                  rd_kafka_error_string(error));
      rd_kafka_error_destroy(error);
    }
  }

  std::mutex mutex_;
  std::map<std::string, Endpoint> endpoints_;
};

/// Attaches to an ingest endpoint through the platform and reads it with the
/// credentials that come back.
class IngestConnector final : public KafkaConnector {
public:
  explicit IngestConnector(IngestArgs args) : args_{std::move(args)} {
  }

  IngestConnector(IngestConnector const&) = delete;
  auto operator=(IngestConnector const&) -> IngestConnector& = delete;
  IngestConnector(IngestConnector&&) = delete;
  auto operator=(IngestConnector&&) -> IngestConnector& = delete;

  ~IngestConnector() override {
    if (acquired_) {
      Attachments::instance().release(args_.name.inner);
    }
  }

  auto connect(OpCtx& ctx) -> Task<failure_or<Connection>> override {
    auto& attachments = Attachments::instance();
    attachments.acquire(args_.name.inner);
    acquired_ = true;
    // Every connection attaches, because a revoke deletes the Kafka user
    // that an earlier attach created, and only an attach recreates it.
    auto guard = co_await attachments.lock(args_.name.inner);
    auto attachment = co_await attach(ctx);
    if (not attachment) {
      co_return failure::promise();
    }
    attachments.store(args_.name.inner, *attachment);
    auto options = nova::Record{};
    auto brokers = std::string{};
    for (auto const& broker : attachment->brokers) {
      if (not brokers.empty()) {
        brokers += ',';
      }
      brokers += broker;
    }
    options["bootstrap.servers"] = nova::String{std::move(brokers)};
    options["security.protocol"]
      = nova::String{std::move(attachment->security_protocol)};
    options["sasl.mechanism"]
      = nova::String{std::move(attachment->sasl_mechanism)};
    options["sasl.username"] = nova::String{std::move(attachment->username)};
    options["sasl.password"] = nova::String{std::move(attachment->password)};
    options["group.id"] = nova::String{std::move(attachment->group)};
    // A new group reads what the endpoint buffered before the pipeline
    // started, rather than only what arrives after.
    options["auto.offset.reset"] = nova::String{"earliest"};
    co_return Connection{
      .topic = std::move(attachment->topic),
      .options = std::move(options),
    };
  }

  auto watch(rd_kafka_t& consumer) -> std::shared_ptr<void> override {
    return Attachments::instance().watch(args_.name.inner, consumer);
  }

private:
  auto attach(OpCtx& ctx) const -> Task<failure_or<IngestEndpointAttachment>> {
    auto platform = deployment_platform();
    if (not platform) {
      diagnostic::error("`ingest` requires a deployment connected to the "
                        "Tenzir Platform")
        .primary(args_.name)
        .emit(ctx);
      co_return failure::promise();
    }
    auto transport = co_await connect_platform_api(*platform);
    if (transport.is_err()) {
      diagnostic::error("failed to connect to the Tenzir Platform: {}",
                        std::move(transport).unwrap_err())
        .primary(args_.name)
        .emit(ctx);
      co_return failure::promise();
    }
    auto api = std::move(transport).unwrap();
    // Both were validated when the pipeline was parsed.
    auto deployment = DeploymentId::make(platform->deployment_id);
    auto name = IngestEndpointName::make(args_.name.inner);
    TENZIR_ASSERT(deployment.is_ok() and name.is_ok());
    auto group = Option<IngestEndpointGroupName>{};
    if (args_.group) {
      auto made = IngestEndpointGroupName::make(args_.group->inner);
      TENZIR_ASSERT(made.is_ok());
      group = std::move(made).unwrap();
    }
    auto attached = co_await call(
      *api, AttachIngestEndpoint{
              AttachIngestEndpointPath{std::move(deployment).unwrap(),
                                       std::move(name).unwrap()},
              AttachIngestEndpointQuery{std::move(group)}});
    if (attached.is_err()) {
      diagnostic::error("failed to attach to ingest endpoint `{}`: {}",
                        args_.name.inner,
                        describe(std::move(attached).unwrap_err()))
        .primary(args_.name)
        .emit(ctx);
      co_return failure::promise();
    }
    co_return std::move(attached).unwrap();
  }

  IngestArgs args_;
  bool acquired_ = false;
};

// Bounds how many parser pipelines run at once.
constexpr auto max_parsers = size_t{32};

auto demote_errors(diagnostic d) -> diagnostic {
  if (d.severity == severity::error) {
    d.severity = severity::warning;
  }
  return d;
}

// Keeps what `from_kafka` delivers, so that the parser pipelines get it.
class Collect final : public Push<nova::Events> {
public:
  auto operator()(nova::Events events) -> Task<void> override {
    collected.push_back(std::move(events));
    co_return;
  }

  std::vector<nova::Events> collected;
};

/// `from_kafka` on the endpoint's topic, with the credentials of an attach.
class Ingest final : public Operator<void, nova::Events> {
public:
  explicit Ingest(IngestArgs args)
    : parser_{std::exchange(args.parser, None{})},
      request_{args.request},
      operator_location_{args.operator_location},
      inner_{make_from_kafka_events([&] {
        auto result = FromKafkaArgs{};
        result.count_events = not parser_;
        result.hold_offsets = static_cast<bool>(parser_);
        result.connector = std::make_shared<IngestConnector>(std::move(args));
        result._json = true;
        result.schema_name = "tenzir.ingest";
        result.infer_under = "received_at";
        result.operator_name = "ingest";
        return result;
      }())} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await inner_->start(ctx);
    if (parser_) {
      events_read_
        = ctx.make_counter(MetricsLabel{"operator", "ingest"},
                           MetricsDirection::read, MetricsVisibility::external_,
                           MetricsUnit::events);
    }
  }

  auto snapshot(Serde& serde) -> void override {
    if (parser_) {
      // A restored operator would not have the parser pipelines that were
      // still running.
      diagnostic::error("`ingest` with a parser pipeline does not support "
                        "checkpoints yet")
        .primary(operator_location_)
        .throw_();
    }
    inner_->snapshot(serde);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    if (not parser_) {
      return inner_->await_task(dh);
    }
    return await_admitted(dh);
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not parser_) {
      co_await inner_->process_task(std::move(result), push, ctx);
      co_return;
    }
    auto admitted = std::move(result).as<Admitted>();
    auto collect = Collect{};
    auto delivered = inner_->delivered_batches();
    co_await inner_->process_task(std::move(admitted.result), collect, ctx);
    if (inner_->delivered_batches() == delivered) {
      TENZIR_ASSERT(collect.collected.empty());
      co_return;
    }
    auto& batch = batches_.emplace_back(Batch{.id = delivered + 1});
    for (auto& events : collect.collected) {
      nova::storage::for_each_true(events.mask, [&](nova::storage::Index row) {
        if (auto request = make_request(events, row, batch.id, ctx)) {
          backlog_.push_back(std::move(*request));
          ++batch.pending;
        }
      });
    }
    co_await spawn_parsers(ctx);
    if (not backlog_.empty()) {
      admission_ = std::move(admitted.permit);
    }
    co_await release_finished_batches(ctx);
  }

  auto process_sub(SubKeyView key, nova::Events events,
                   Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, ctx);
    auto const rows = events.active_count();
    co_await push(std::move(events));
    events_read_.add(rows);
  }

  auto finish_sub(SubKeyView key, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto parser = running_.find(as<uint64_t>(key));
    TENZIR_ASSERT(parser != running_.end());
    finish_request(parser->second);
    running_.erase(parser);
    co_await spawn_parsers(ctx);
    if (backlog_.empty()) {
      admission_ = None{};
    }
    co_await release_finished_batches(ctx);
  }

  auto post_commit(OpCtx& ctx) -> Task<void> override {
    return inner_->post_commit(ctx);
  }

  auto prepare_snapshot(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    return inner_->prepare_snapshot(push, ctx);
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    return inner_->finalize(push, ctx);
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    return inner_->stop(ctx);
  }

  auto state() -> OperatorState override {
    // The stream may end while the requests it delivered are still parsed.
    if (not batches_.empty()) {
      return OperatorState::normal;
    }
    return inner_->state();
  }

private:
  // The result of `await_task` while it holds back further batches.
  struct Admitted {
    SemaphorePermit permit;
    Any result;
  };

  // A request that waits for a parser pipeline.
  struct Request {
    uint64_t batch;
    record metadata;
    chunk_ptr body;
  };

  // A batch that `from_kafka` delivered, with the number of its requests that
  // are not parsed yet.
  struct Batch {
    uint64_t id = 0;
    size_t pending = 0;
  };

  auto await_admitted(diagnostic_handler& dh) const -> Task<Any> {
    auto permit = co_await admissions_->acquire();
    auto result = co_await inner_->await_task(dh);
    co_return Admitted{std::move(permit), std::move(result)};
  }

  auto make_request(nova::Events const& events, nova::storage::Index row,
                    uint64_t batch, OpCtx& ctx) const -> Option<Request> {
    auto value = nova::materialize_legacy(events.data.get(row));
    auto* metadata = try_as<record>(value);
    TENZIR_ASSERT(metadata);
    auto body = metadata->find("body");
    if (body == metadata->end() or not is<std::string>(body->second)) {
      diagnostic::warning("skipping a request without a body")
        .primary(operator_location_)
        .emit(ctx);
      return None{};
    }
    auto chunk = chunk::make(std::move(as<std::string>(body->second)));
    metadata->erase(body);
    return Request{
      .batch = batch,
      .metadata = std::move(*metadata),
      .body = std::move(chunk),
    };
  }

  auto spawn_parsers(OpCtx& ctx) -> Task<void> {
    while (running_.size() < max_parsers and not backlog_.empty()) {
      auto request = std::move(backlog_.front());
      backlog_.pop_front();
      auto pipeline = parser_->inner;
      pipeline.bind(request_, std::move(request.metadata));
      // A request that the parser cannot handle must not stop the others.
      auto dh = transforming_diagnostic_handler{ctx.dh(), demote_errors};
      auto plan = ir::make_plan(std::move(pipeline), tag_v<chunk_ptr>,
                                base_ctx{dh, ctx.reg()});
      if (not plan) {
        finish_request(request.batch);
        continue;
      }
      auto key = next_key_++;
      running_.emplace(key, request.batch);
      auto& sub = co_await ctx.spawn_sub(data{key}, std::move(*plan),
                                         DiagnosticBehavior::ErrorToWarning);
      auto& parser = as<SubHandle<chunk_ptr>>(sub);
      if (request.body->size() > 0) {
        // Fails only when the parser already stopped, which it reports itself.
        std::ignore = co_await parser.push(std::move(request.body));
      }
      co_await parser.close();
    }
  }

  auto finish_request(uint64_t batch) -> void {
    TENZIR_ASSERT(not batches_.empty());
    TENZIR_ASSERT(batch >= batches_.front().id);
    auto& pending = batches_[batch - batches_.front().id].pending;
    TENZIR_ASSERT(pending > 0);
    --pending;
  }

  // Commits the batches whose requests are all parsed, up to the first one
  // that is not, so that the committed offset never skips a request.
  auto release_finished_batches(OpCtx& ctx) -> Task<void> {
    auto released = Option<uint64_t>{};
    while (not batches_.empty() and batches_.front().pending == 0) {
      released = batches_.front().id;
      batches_.pop_front();
    }
    if (released) {
      co_await inner_->release_batches(*released, ctx);
    }
  }

  Option<located<ir::pipeline>> parser_;
  let_id request_;
  location operator_location_;
  Box<KafkaEventsSource> inner_;
  // Holds back the next batch while requests of the current one still wait
  // for a parser pipeline.
  mutable Arc<Semaphore> admissions_{std::in_place, size_t{1}};
  Option<SemaphorePermit> admission_;
  std::deque<Request> backlog_;
  std::deque<Batch> batches_;
  // The batch of each running parser pipeline, by its key.
  std::unordered_map<uint64_t, uint64_t> running_;
  uint64_t next_key_ = 0;
  MetricsCounter events_read_;
};

class IngestPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "ingest";
  }

  auto describe() const -> Description override {
    auto d = Describer<IngestArgs, Ingest>{};
    d.concurrent_output();
    auto name = d.positional("name", &IngestArgs::name);
    auto group = d.named("group", &IngestArgs::group);
    auto parser = d.pipeline(&IngestArgs::parser, SubOptimize::from_downstream,
                             {{"request", &IngestArgs::request}});
    d.operator_location(&IngestArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(name)) {
        if (auto made = IngestEndpointName::make(value->inner); made.is_err()) {
          diagnostic::error("invalid ingest endpoint name: {}",
                            std::move(made).unwrap_err().reason)
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(group)) {
        if (auto made = IngestEndpointGroupName::make(value->inner);
            made.is_err()) {
          diagnostic::error("invalid consumer group: {}",
                            std::move(made).unwrap_err().reason)
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(parser)) {
        auto output = value->inner.infer_type(tag_v<chunk_ptr>, ctx);
        if (output.is_error()) {
          return {};
        }
        if (output->is_not<nova::Events>()) {
          diagnostic::error("pipeline must return events")
            .primary(value->source.subloc(0, 1))
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::IngestPlugin)
