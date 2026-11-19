//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/from_kafka.hpp"

#include <tenzir/async/mutex.hpp>
#include <tenzir/controller/generated/api.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/platform_connection.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <fmt/format.h>
#include <librdkafka/rdkafka.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::kafka {

namespace {

struct IngestArgs {
  located<std::string> name;
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
    // A new group reads only what arrives after the pipeline started, rather
    // than everything the endpoint still buffers.
    options["auto.offset.reset"] = nova::String{"latest"};
    // An ad-hoc pipeline reads in a new group every time it runs, so nothing
    // would ever resume from what it commits.
    auto pipeline = ctx.pipeline();
    co_return Connection{
      .topic = std::move(attachment->topic),
      .options = std::move(options),
      .commit_offsets = not pipeline or not pipeline->adhoc,
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
    auto identity = co_await platform->identity(*api);
    if (identity.is_err()) {
      diagnostic::error("failed to learn which workspace this deployment "
                        "belongs to: {}",
                        std::move(identity).unwrap_err())
        .primary(args_.name)
        .emit(ctx);
      co_return failure::promise();
    }
    auto deployment = identity.unwrap().path();
    // Validated when the pipeline was parsed.
    auto name = IngestEndpointName::make(args_.name.inner);
    TENZIR_ASSERT(name.is_ok());
    // Each pipeline reads in a group of its own, so that the pipelines of a
    // deployment do not split the events between them.
    auto group = Option<IngestEndpointGroupName>{};
    if (auto pipeline = ctx.pipeline()) {
      auto made = IngestEndpointGroupName::make(
        fmt::format("{}/{}", deployment.deployment_id, pipeline->id));
      if (made.is_err()) {
        diagnostic::error("cannot derive a consumer group from pipeline "
                          "`{}`: {}",
                          pipeline->id, std::move(made).unwrap_err().reason)
          .primary(args_.name)
          .hint("give the pipeline a shorter path")
          .emit(ctx);
        co_return failure::promise();
      }
      group = std::move(made).unwrap();
    }
    auto attached = co_await call(
      *api, AttachIngestEndpoint{
              AttachIngestEndpointPath{
                std::move(deployment.org), std::move(deployment.workspace),
                std::move(deployment.deployment_id), std::move(name).unwrap()},
              AttachIngestEndpointQuery{std::move(group)}});
    if (attached.is_err()) {
      auto error = std::move(attached).unwrap_err();
      if (auto const* declared = try_as<AttachIngestEndpointError>(&error)) {
        if (auto const* unavailable = try_as<IngestUnavailableError>(declared)) {
          diagnostic::error("ingest endpoints are only available on "
                            "deployments that Tenzir hosts")
            .primary(args_.name)
            .note("this deployment is self-hosted or runs outside `{}`",
                  unavailable->region)
            .hint("run the pipeline on a Tenzir-hosted deployment in `{}`",
                  unavailable->region)
            .docs("https://tenzir.com/docs/explanations/ingest-endpoints")
            .emit(ctx);
          co_return failure::promise();
        }
      }
      diagnostic::error("failed to attach to ingest endpoint `{}`: {}",
                        args_.name.inner, describe(error))
        .primary(args_.name)
        .emit(ctx);
      co_return failure::promise();
    }
    co_return std::move(attached).unwrap();
  }

  IngestArgs args_;
  bool acquired_ = false;
};

/// `from_kafka` on the endpoint's topic, with the credentials of an attach.
class Ingest final : public Operator<void, nova::Events> {
public:
  explicit Ingest(IngestArgs args)
    : inner_{make_from_kafka_events([&] {
        auto result = FromKafkaArgs{};
        result.connector = std::make_shared<IngestConnector>(std::move(args));
        result._json = true;
        result.schema_name = "tenzir.ingest";
        result.infer_under = "received_at";
        result.operator_name = "ingest";
        return result;
      }())} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return inner_->start(ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    inner_->snapshot(serde);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    return inner_->await_task(dh);
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    return inner_->process_task(std::move(result), push, ctx);
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
    return inner_->state();
  }

private:
  Box<Operator<void, nova::Events>> inner_;
};

class IngestPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "ingest";
  }

  auto deployment_only() const -> bool override {
    return true;
  }

  auto describe() const -> Description override {
    auto d = Describer<IngestArgs, Ingest>{};
    auto name = d.positional("name", &IngestArgs::name);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(name)) {
        if (auto made = IngestEndpointName::make(value->inner); made.is_err()) {
          diagnostic::error("invalid ingest endpoint name: {}",
                            std::move(made).unwrap_err().reason)
            .primary(value->source)
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
