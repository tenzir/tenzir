//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"
#include "kafka/operator.hpp"
#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/aws_iam.hpp"
#include "tenzir/series_builder.hpp"

#include <tenzir/generator.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>

#include <atomic>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace tenzir::plugins::kafka {
namespace {

constexpr auto commit_timeout = 10s;

struct from_kafka_args {
  std::string topic;
  std::optional<located<uint64_t>> count;
  std::optional<location> exit;
  std::optional<located<std::string>> offset;
  std::uint64_t commit_batch_size = 1000;
  located<record> options;
  std::optional<located<std::string>> aws_region;
  std::optional<tenzir::aws_iam_options> aws;
  location operator_location;
  uint64_t jobs = 1;

  friend auto inspect(auto& f, from_kafka_args& x) -> bool {
    return f.object(x).fields(
      f.field("topic", x.topic), f.field("count", x.count),
      f.field("exit", x.exit), f.field("offset", x.offset),
      f.field("commit_batch_size", x.commit_batch_size),
      f.field("options", x.options), f.field("aws_region", x.aws_region),
      f.field("aws", x.aws), f.field("operator_location", x.operator_location),
      f.field("jobs", x.jobs));
  }
};

// Owns the shared synchronization state between the main coroutine and
// worker threads.
struct consume_synchronizer {
  std::deque<table_slice> outputs;
  std::mutex outputs_mutex;
  std::atomic<size_t> total_messages = 0;
  std::atomic<bool> shutdown = false;
  std::atomic<size_t> active_workers = 0;
  std::condition_variable cv;

  constexpr static auto backpressure_size = 42;

  auto emit(table_slice slice) -> void {
    auto lock = std::unique_lock{outputs_mutex};
    if (outputs.size() > backpressure_size) {
      cv.wait(lock, [this]() -> bool {
        return outputs.size() < backpressure_size;
      });
    }
    outputs.push_back(std::move(slice));
  }

  auto request_shutdown() -> void {
    shutdown.store(true, std::memory_order_relaxed);
  }

  auto is_shutdown() const -> bool {
    return shutdown.load(std::memory_order_relaxed);
  }

  auto count_message() -> size_t {
    return total_messages.fetch_add(1, std::memory_order_relaxed) + 1;
  }
};

// Per-thread worker that owns a consumer and runs the consume loop.
class consume_worker {
public:
  static auto make(configuration config, from_kafka_args const& args,
                   diagnostic_handler& dh, consume_synchronizer& sync)
    -> std::optional<consume_worker> {
    if (auto value = config.get("bootstrap.servers")) {
      TENZIR_INFO("kafka connecting to broker: {}", *value);
    }
    auto client = consumer::make(std::move(config));
    if (not client) {
      diagnostic::error("failed to create consumer: {}", client.error())
        .primary(args.operator_location)
        .emit(dh);
      return std::nullopt;
    }
    TENZIR_INFO("kafka subscribes to topic {}", args.topic);
    if (auto err = client->subscribe({args.topic}); err.valid()) {
      diagnostic::error("failed to subscribe to topic: {}", err)
        .primary(args.operator_location)
        .emit(dh);
      return std::nullopt;
    }
    return consume_worker{std::move(*client), args, dh, sync};
  }

  auto
  commit_pending(std::unordered_map<int32_t, std::shared_ptr<RdKafka::Message>>&
                   pending_messages) -> bool {
    for (auto const& [_, msg] : pending_messages) {
      if (not client_.commit(msg.get(), dh_, args_.operator_location)) {
        return false;
      }
    }
    pending_messages.clear();
    return true;
  }

  auto run() -> void {
    auto num_messages = size_t{0};
    auto last_commit_time = time::clock::now();
    auto pending_messages
      = std::unordered_map<int32_t, std::shared_ptr<RdKafka::Message>>{};
    // Optional distinguishes "no assignment fetched yet" from a legitimate
    // empty assignment (e.g., rebalancing or no partitions), which must not
    // reset EOF tracking.
    auto assigned_partitions = std::optional<std::unordered_set<int32_t>>{};
    auto eof_partitions = std::unordered_set<int32_t>{};
    auto const schema = type{
      "tenzir.kafka",
      record_type{
        {"message", string_type{}},
      },
    };
    auto const arrow_schema = schema.to_arrow_schema();
    auto b = string_type::make_arrow_builder(arrow_memory_pool());
    auto const finish_as_slice = [&] -> table_slice {
      auto const length = b->length();
      return table_slice{
        arrow::RecordBatch::Make(arrow_schema, length, {finish(*b)}),
      };
    };
    while (not sync_.is_shutdown()) {
      auto raw_msg = client_.consume_raw(500ms);
      TENZIR_ASSERT(raw_msg);
      auto const now = time::clock::now();
      auto const timed_out = now - last_commit_time >= commit_timeout;
      switch (raw_msg->err()) {
        case RdKafka::ERR_NO_ERROR: {
          auto partition = raw_msg->partition();
          pending_messages[partition] = std::move(raw_msg);
          auto& message = pending_messages[partition];
          check(b->Append(reinterpret_cast<const char*>(message->payload()),
                          detail::narrow<int32_t>(message->len())));
          ++num_messages;
          auto const global_count = sync_.count_message();
          auto const reached_count
            = args_.count and args_.count->inner == global_count;
          auto const full_batch = num_messages % args_.commit_batch_size == 0;
          if (full_batch or timed_out or reached_count) {
            last_commit_time = now;
            sync_.emit(finish_as_slice());
            if (not commit_pending(pending_messages)) {
              return;
            }
            if (reached_count) {
              sync_.request_shutdown();
              return;
            }
          }
          continue;
        }
        case RdKafka::ERR__TIMED_OUT: {
          if (not pending_messages.empty() and timed_out) {
            last_commit_time = now;
            sync_.emit(finish_as_slice());
            if (not commit_pending(pending_messages)) {
              return;
            }
          }
          continue;
        }
        case RdKafka::ERR__PARTITION_EOF: {
          auto assignment
            = client_.get_assignment(args_.topic, dh_, args_.operator_location);
          if (not assignment) {
            commit_pending(pending_messages);
            return;
          }
          if (assignment->empty()) {
            TENZIR_DEBUG("kafka partition {} reached EOF with no assignment",
                         raw_msg->partition());
            continue;
          }
          if (not assigned_partitions or *assigned_partitions != *assignment) {
            assigned_partitions = std::move(*assignment);
            eof_partitions.clear();
          }
          if (not assigned_partitions->contains(raw_msg->partition())) {
            TENZIR_DEBUG("kafka partition {} EOF not in assignment",
                         raw_msg->partition());
            continue;
          }
          eof_partitions.insert(raw_msg->partition());
          TENZIR_DEBUG("kafka partition {} reached EOF ({}/{} partitions EOF)",
                       raw_msg->partition(), eof_partitions.size(),
                       assigned_partitions->size());
          if (eof_partitions.size() == assigned_partitions->size()) {
            if (not pending_messages.empty()) {
              sync_.emit(finish_as_slice());
              last_commit_time = now;
              commit_pending(pending_messages);
            }
            return;
          }
          continue;
        }
        default: {
          if (not pending_messages.empty()) {
            auto ndh = transforming_diagnostic_handler{
              dh_,
              [](auto&& diag) {
                return std::move(diag)
                  .modify()
                  .severity(severity::warning)
                  .done();
              },
            };
            sync_.emit(finish_as_slice());
            last_commit_time = now;
            for (auto const& [_, msg] : pending_messages) {
              std::ignore
                = client_.commit(msg.get(), ndh, args_.operator_location);
            }
            pending_messages.clear();
          }
          diagnostic::error("unexpected kafka error: `{}`", raw_msg->errstr())
            .primary(args_.operator_location)
            .emit(dh_);
          return;
        }
      }
    }
    // Shutdown was requested; commit any remaining pending messages.
    if (not pending_messages.empty()) {
      sync_.emit(finish_as_slice());
      commit_pending(pending_messages);
    }
  }

private:
  consume_worker(consumer client, from_kafka_args const& args,
                 diagnostic_handler& dh, consume_synchronizer& sync)
    : client_{std::move(client)}, args_{args}, dh_{dh}, sync_{sync} {
  }

  consumer client_;
  from_kafka_args const& args_;
  diagnostic_handler& dh_;
  consume_synchronizer& sync_;
};

class from_kafka_operator final : public crtp_operator<from_kafka_operator> {
public:
  from_kafka_operator() = default;

  from_kafka_operator(from_kafka_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
    if (not config_.contains("group.id")) {
      config_["group.id"] = "tenzir";
    }
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    // Resolve secrets if explicit credentials or role are provided.
    auto resolved_creds = std::optional<tenzir::resolved_aws_credentials>{};
    if (args_.aws
        and (args_.aws->has_explicit_credentials() or args_.aws->role)) {
      resolved_creds.emplace();
      auto requests = args_.aws->make_secret_requests(*resolved_creds, dh);
      co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
    }
    // Use top-level aws_region if provided, otherwise fall back to aws_iam.
    if (args_.aws_region) {
      if (not resolved_creds) {
        resolved_creds.emplace();
      }
      resolved_creds->region = args_.aws_region->inner;
    }
    co_yield {};
    // Build a fully-configured configuration that workers will copy.
    auto tmp_cfg = configuration::make(config_, args_.aws, resolved_creds, dh);
    if (not tmp_cfg) {
      diagnostic::error("failed to create configuration: {}", tmp_cfg.error())
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    }
    {
      auto secrets = configure_or_request(args_.options, *tmp_cfg, dh);
      co_yield ctrl.resolve_secrets_must_yield(std::move(secrets));
    }
    // Configure per-consumer settings on tmp_cfg before spawning workers.
    if (args_.exit) {
      if (auto err = tmp_cfg->set("enable.partition.eof", "true");
          err.valid()) {
        diagnostic::error("failed to enable partition EOF: {}", err)
          .primary(args_.operator_location)
          .emit(dh);
        co_return;
      }
    }
    if (auto err = tmp_cfg->set("enable.auto.commit", "false"); err.valid()) {
      diagnostic::error("failed to disable auto-commit: {}", err)
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    }
    auto offset = RdKafka::Topic::OFFSET_STORED;
    if (args_.offset) {
      auto success = offset_parser()(args_.offset->inner, offset);
      TENZIR_ASSERT(success);
      TENZIR_INFO("kafka adjusts offset to {} ({})", args_.offset->inner,
                  offset);
    }
    if (auto err = tmp_cfg->set_rebalance_cb(offset); err.valid()) {
      diagnostic::error("failed to set rebalance callback: {}", err)
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    }
    auto const num_workers = std::max(args_.jobs, uint64_t{1});
    auto sync = consume_synchronizer{};
    sync.active_workers.store(num_workers, std::memory_order_relaxed);
    auto threads = std::vector<std::thread>{};
    threads.reserve(num_workers);
    auto guard = detail::scope_guard{[&]() noexcept {
      sync.request_shutdown();
      for (auto& thread : threads) {
        thread.join();
      }
    }};
    for (auto i = uint64_t{0}; i < num_workers; ++i) {
      threads.emplace_back([&, sdh = ctrl.shared_diagnostics()]() mutable {
        caf::detail::set_thread_name("kafka_consume");
        auto worker = consume_worker::make(*tmp_cfg, args_, sdh, sync);
        if (not worker) {
          sync.active_workers.fetch_sub(1, std::memory_order_release);
          return;
        }
        worker->run();
        sync.active_workers.fetch_sub(1, std::memory_order_release);
      });
    }
    while (sync.active_workers.load(std::memory_order_acquire) > 0) {
      auto lock = std::unique_lock{sync.outputs_mutex};
      if (sync.outputs.empty()) {
        lock.unlock();
        co_yield {};
        continue;
      }
      auto batch = std::move(sync.outputs);
      sync.outputs.clear();
      lock.unlock();
      sync.cv.notify_all();
      for (auto& slice : batch) {
        co_yield std::move(slice);
      }
    }
    guard.trigger();
    for (auto& slice : sync.outputs) {
      co_yield std::move(slice);
    }
  }

  auto detached() const -> bool override {
    return false;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "from_kafka";
  }

  friend auto inspect(auto& f, from_kafka_operator& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_),
                              f.field("config", x.config_));
  }

private:
  from_kafka_args args_;
  record config_;
};

class from_kafka final : public operator_plugin2<from_kafka_operator> {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `kafka.yaml` instead",
                               this->name())
        .to_error();
    }
    [&] {
      auto ptr = global_config.find("plugins");
      if (ptr == global_config.end()) {
        return;
      }
      const auto* plugin_config = try_as<record>(&ptr->second);
      if (not plugin_config) {
        return;
      }
      auto kafka_config_ptr = plugin_config->find("kafka");
      if (kafka_config_ptr == plugin_config->end()) {
        return;
      }
      const auto* kafka_config = try_as<record>(&kafka_config_ptr->second);
      if (not kafka_config or kafka_config->empty()) {
        return;
      }
      config_ = flatten(*kafka_config);
    }();
    if (not config_.contains("bootstrap.servers")) {
      config_["bootstrap.servers"] = "localhost";
    }
    if (not config_.contains("client.id")) {
      config_["client.id"] = "tenzir";
    }
    return caf::none;
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_kafka_args{};
    args.operator_location = inv.self.get_location();
    auto offset = std::optional<ast::expression>{};
    auto iam_opts = std::optional<located<record>>{};
    TRY(argument_parser2::operator_(name())
          .positional("topic", args.topic)
          .named("count", args.count)
          .named("exit", args.exit)
          .named("offset", offset, "string|int")
          .named_optional("options", args.options)
          .named("aws_region", args.aws_region)
          .named("aws_iam", iam_opts)
          .named_optional("commit_batch_size", args.commit_batch_size)
          .named_optional("_jobs", args.jobs)
          .parse(inv, ctx));
    if (args.commit_batch_size == 0) {
      diagnostic::error("`commit_batch_size` must be greater than 0")
        .primary(args.operator_location)
        .emit(ctx);
      return failure::promise();
    }
    if (iam_opts) {
      TRY(check_sasl_mechanism(args.options, ctx));
      TRY(check_sasl_mechanism(located{config_, iam_opts->source}, ctx));
      args.options.inner["sasl.mechanism"] = "OAUTHBEARER";
      TRY(args.aws, tenzir::aws_iam_options::from_record(
                      std::move(iam_opts).value(), ctx));
      // Region is required for Kafka MSK authentication.
      // Use top-level aws_region if provided, otherwise require aws_iam.region.
      if (not args.aws_region and not args.aws->region) {
        diagnostic::error(
          "`aws_region` is required for Kafka MSK authentication")
          .primary(args.aws->loc)
          .emit(ctx);
        return failure::promise();
      }
    }
    // Disable auto-commit to use manual commit for precise message counting
    if (args.options.inner.contains("enable.auto.commit")) {
      diagnostic::error("`enable.auto.commit` must not be specified")
        .primary(args.options)
        .note("`enable.auto.commit` is enforced to be `false`")
        .emit(ctx);
      return failure::promise();
    }
    if (offset) {
      TRY(auto evaluated, const_eval(offset.value(), ctx.dh()));
      constexpr auto f = detail::overload{
        [](const std::integral auto& value) -> std::optional<std::string> {
          return fmt::to_string(value);
        },
        [](const std::string& value) -> std::optional<std::string> {
          return value;
        },
        [](const auto&) -> std::optional<std::string> {
          return std::nullopt;
        }};
      auto result = tenzir::match(evaluated, f);
      if (not result) {
        diagnostic::error("expected `string` or `int`")
          .primary(offset->get_location())
          .emit(ctx);
        return failure::promise();
      }
      if (not offset_parser()(result.value())) {
        diagnostic::error("invalid `offset` value")
          .primary(offset->get_location())
          .note(
            "must be `beginning`, `end`, `store`, `<offset>` or `-<offset>`")
          .emit(ctx);
        return failure::promise();
      }
      args.offset = located{std::move(result).value(), offset->get_location()};
    }
    TRY(validate_options(args.options, ctx));
    return std::make_unique<from_kafka_operator>(std::move(args), config_);
  }

private:
  record config_;
};

} // namespace
} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::from_kafka)
