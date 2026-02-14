//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/async_consumer.hpp"
#include "kafka/configuration.hpp"
#include "kafka/message_builder.hpp"
#include "kafka/operator.hpp"
#include "tenzir/aws_iam.hpp"

#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/OperationCancelled.h>
#include <folly/executors/GlobalExecutor.h>
#include <librdkafka/rdkafkacpp.h>

#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tenzir::plugins::kafka {

namespace {

/// Maximum delay before flushing a partial commit batch.
constexpr auto commit_timeout = 10s;

/// Stores process-wide `from_kafka` defaults from `kafka.yaml`.
auto source_global_defaults() -> record& {
  static auto defaults = record{};
  return defaults;
}

struct FromKafkaArgs {
  std::string topic;
  std::optional<located<uint64_t>> count;
  std::optional<location> exit;
  std::optional<located<data>> offset;
  uint64_t commit_batch_size = 1000;
  located<record> options;
  std::optional<located<std::string>> aws_region;
  std::optional<located<record>> aws_iam;
};

auto parse_offset_value(const located<data>& input, int64_t& offset) -> bool {
  return match(
    input.inner,
    [&](const std::string& value) -> bool {
      return offset_parser()(value, offset);
    },
    [&](int64_t value) -> bool {
      if (value >= 0) {
        offset = value;
      } else {
        offset = RdKafka::Consumer::OffsetTail(-value);
      }
      return true;
    },
    [&](uint64_t value) -> bool {
      constexpr auto max = std::numeric_limits<int64_t>::max();
      if (value > static_cast<uint64_t>(max)) {
        return false;
      }
      offset = static_cast<int64_t>(value);
      return true;
    },
    [&](const auto&) -> bool {
      return false;
    });
}

/// Result envelope for await_task() to distinguish idle timeouts from EOF.
struct FromKafkaTaskResult {
  std::shared_ptr<RdKafka::Message> message;
  bool idle_timeout = false;
};

class FromKafkaOperator final : public Operator<void, table_slice> {
public:
  explicit FromKafkaOperator(FromKafkaArgs args) : args_{std::move(args)} {
  }
  FromKafkaOperator(FromKafkaOperator&&) = default;
  auto operator=(FromKafkaOperator&&) -> FromKafkaOperator& = default;
  FromKafkaOperator(const FromKafkaOperator&) = delete;
  auto operator=(const FromKafkaOperator&) -> FromKafkaOperator& = delete;

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await OperatorBase::start(ctx);
    if (done_) {
      co_return;
    }
    auto aws = std::optional<aws_iam_options>{};
    if (args_.aws_iam) {
      auto parsed = aws_iam_options::from_record(*args_.aws_iam, ctx.dh());
      if (not parsed) {
        done_ = true;
        co_return;
      }
      aws = std::move(*parsed);
      if (not args_.aws_region and not aws->region) {
        diagnostic::error(
          "`aws_region` is required for Kafka MSK authentication")
          .primary(args_.aws_iam->source)
          .emit(ctx);
        done_ = true;
        co_return;
      }
    }
    auto resolved_creds = std::optional<resolved_aws_credentials>{};
    if (aws and (aws->has_explicit_credentials() or aws->role)) {
      resolved_creds.emplace();
      auto requests = aws->make_secret_requests(*resolved_creds, ctx.dh());
      if (auto ok = co_await ctx.resolve_secrets(std::move(requests)); not ok) {
        done_ = true;
        co_return;
      }
    }
    if (args_.aws_region) {
      if (not resolved_creds) {
        resolved_creds.emplace();
      }
      resolved_creds->region = args_.aws_region->inner;
    }
    auto config = source_global_defaults();
    if (not config.contains("group.id")) {
      config["group.id"] = "tenzir";
    }
    auto cfg = configuration::make(config, aws, resolved_creds, ctx.dh());
    if (not cfg) {
      diagnostic::error("failed to create kafka configuration: {}", cfg.error())
        .emit(ctx);
      done_ = true;
      co_return;
    }
    cfg_ = std::move(*cfg);
    auto user_options = args_.options;
    if (aws) {
      user_options.inner["sasl.mechanism"] = "OAUTHBEARER";
    }
    if (auto ok = co_await ctx.resolve_secrets(
          configure_or_request(user_options, *cfg_, ctx.dh()));
        not ok) {
      done_ = true;
      co_return;
    }
    if (args_.exit) {
      if (auto err = cfg_->set("enable.partition.eof", "true"); err) {
        diagnostic::error("failed to enable partition EOF: {}", err).emit(ctx);
        done_ = true;
        co_return;
      }
    }
    if (auto err = cfg_->set("enable.auto.commit", "false"); err) {
      diagnostic::error("failed to disable auto-commit: {}", err).emit(ctx);
      done_ = true;
      co_return;
    }
    auto offset = int64_t{RdKafka::Topic::OFFSET_STORED};
    if (args_.offset and not parse_offset_value(*args_.offset, offset)) {
      diagnostic::error("invalid `offset` value")
        .primary(args_.offset->source)
        .note(
          "must be `beginning`, `end`, `stored`, `<offset>`, or `-<offset>`")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (auto err = cfg_->set_rebalance_cb(offset); err) {
      diagnostic::error("failed to set rebalance callback: {}", err).emit(ctx);
      done_ = true;
      co_return;
    }
    auto error = std::string{};
    auto* raw_consumer
      = RdKafka::KafkaConsumer::create(cfg_->underlying(), error);
    if (raw_consumer == nullptr) {
      diagnostic::error("failed to create kafka consumer: {}", error).emit(ctx);
      done_ = true;
      co_return;
    }
    consumer_.emplace(Box<RdKafka::KafkaConsumer>::from_unique_ptr(
      std::unique_ptr<RdKafka::KafkaConsumer>{raw_consumer}));
    if (auto err = (*consumer_)->subscribe({args_.topic});
        err != RdKafka::ERR_NO_ERROR) {
      diagnostic::error("failed to subscribe to topic: {}",
                        RdKafka::err2str(err))
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto* evb = folly::getGlobalIOExecutor()->getEventBase();
    auto queue = AsyncConsumerQueue::make(*evb, **consumer_);
    if (queue.is_err()) {
      diagnostic::error("failed to create async consumer queue: {}",
                        std::move(queue).unwrap_err())
        .emit(ctx);
      done_ = true;
      co_return;
    }
    queue_.emplace(std::move(queue).unwrap());
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (not queue_) {
      co_return FromKafkaTaskResult{};
    }
    try {
      auto token = co_await folly::coro::co_current_cancellation_token;
      if (token.isCancellationRequested()) {
        (*queue_)->request_stop();
        co_return FromKafkaTaskResult{};
      }
      auto timeout = std::optional<std::chrono::milliseconds>{};
      // Invariant: idle timeouts are only relevant while we hold unflushed
      // records, otherwise we can sleep until a real queue event arrives.
      if (messages_since_batch_commit_ > 0) {
        timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
          commit_timeout);
      }
      auto result = co_await (*queue_)->next(timeout);
      co_return FromKafkaTaskResult{
        .message = std::move(result.message),
        .idle_timeout = result.timed_out,
      };
    } catch (const folly::OperationCancelled&) {
      (*queue_)->request_stop();
      co_return FromKafkaTaskResult{};
    }
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto task_result = std::move(result).as<FromKafkaTaskResult>();
    if (task_result.idle_timeout) {
      co_await flush_pending(push);
      messages_since_batch_commit_ = 0;
      last_batch_time_ = time::clock::now();
      co_return;
    }
    auto message = std::move(task_result.message);
    if (not message) {
      done_ = true;
      co_return;
    }
    const auto now = time::clock::now();
    const auto timed_out = now - last_batch_time_ >= commit_timeout;
    switch (message->err()) {
      case RdKafka::ERR_NO_ERROR: {
        builder_.append(*message);
        current_uncommitted_offsets_[message->partition()] = message->offset();
        ++emitted_messages_;
        ++messages_since_batch_commit_;
        const auto reached_count
          = args_.count and args_.count->inner == emitted_messages_;
        const auto full_batch
          = messages_since_batch_commit_ >= args_.commit_batch_size;
        if (full_batch or timed_out or reached_count) {
          co_await flush_pending(push);
          messages_since_batch_commit_ = 0;
          last_batch_time_ = now;
          if (reached_count) {
            done_ = true;
            if (queue_) {
              (*queue_)->request_stop();
            }
          }
        }
        break;
      }
      case RdKafka::ERR__PARTITION_EOF: {
        if (not args_.exit) {
          break;
        }
        auto partitions = std::vector<RdKafka::TopicPartition*>{};
        if (auto err = (*consumer_)->assignment(partitions);
            err != RdKafka::ERR_NO_ERROR) {
          diagnostic::error("failed to get assignment: {}",
                            RdKafka::err2str(err))
            .emit(ctx);
          done_ = true;
          if (queue_) {
            (*queue_)->request_stop();
          }
          break;
        }
        auto assignment = std::unordered_set<int32_t>{};
        for (auto* partition : partitions) {
          if (partition and partition->topic() == args_.topic) {
            assignment.insert(partition->partition());
          }
        }
        RdKafka::TopicPartition::destroy(partitions);
        if (assignment.empty()) {
          break;
        }
        if (not assigned_partitions_ or *assigned_partitions_ != assignment) {
          assigned_partitions_ = std::move(assignment);
          eof_partitions_.clear();
        }
        if (not assigned_partitions_->contains(message->partition())) {
          break;
        }
        eof_partitions_.insert(message->partition());
        if (eof_partitions_.size() == assigned_partitions_->size()) {
          co_await flush_pending(push);
          done_ = true;
          if (queue_) {
            (*queue_)->request_stop();
          }
        }
        break;
      }
      default: {
        co_await flush_pending(push);
        diagnostic::error("unexpected kafka error: `{}`", message->errstr())
          .emit(ctx);
        done_ = true;
        if (queue_) {
          (*queue_)->request_stop();
        }
        break;
      }
    }
  }

  auto post_commit() -> Task<void> override {
    if (checkpoint_pending_offsets_.empty() or not consumer_) {
      co_return;
    }
    auto offsets = std::vector<RdKafka::TopicPartition*>{};
    offsets.reserve(checkpoint_pending_offsets_.size());
    for (const auto& [partition, offset] : checkpoint_pending_offsets_) {
      offsets.push_back(
        RdKafka::TopicPartition::create(args_.topic, partition, offset));
    }
    if (auto err = (*consumer_)->commitSync(offsets);
        err != RdKafka::ERR_NO_ERROR) {
      TENZIR_WARN("from_kafka: failed to commit offsets: {}",
                  RdKafka::err2str(err));
    }
    RdKafka::TopicPartition::destroy(offsets);
    checkpoint_pending_offsets_.clear();
  }

  auto snapshot(Serde& serde) -> void override {
    // Persist only recovery state that reflects consumed progress.
    serde("emitted_messages", emitted_messages_);
    serde("checkpoint_pending_offsets", checkpoint_pending_offsets_);
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  /// Advances the commit checkpoint to include all flushed offsets.
  auto move_current_to_checkpoint() -> void {
    for (const auto& [partition, offset] : current_uncommitted_offsets_) {
      checkpoint_pending_offsets_[partition] = offset + 1;
    }
    current_uncommitted_offsets_.clear();
  }

  /// Emits the current slice batch and marks offsets for post-commit.
  auto flush_pending(Push<table_slice>& push) -> Task<void> {
    if (builder_.empty()) {
      co_return;
    }
    co_await push(builder_.finish());
    move_current_to_checkpoint();
  }

  FromKafkaArgs args_;
  std::optional<configuration> cfg_;
  std::optional<Box<RdKafka::KafkaConsumer>> consumer_;
  mutable std::optional<Box<AsyncConsumerQueue>> queue_;
  KafkaMessageBuilder builder_;
  // Number of records emitted so far; used for `count=` resumption.
  size_t emitted_messages_ = 0;
  // Invariant: this counts unflushed records in `builder_` only.
  size_t messages_since_batch_commit_ = 0;
  // Invariant: tracks the last successful flush decision point.
  time last_batch_time_ = time::clock::now();
  // Invariant: highest consumed offset per partition since last flush.
  std::unordered_map<int32_t, int64_t> current_uncommitted_offsets_;
  // Invariant: committed offsets are stored as "next offset to consume".
  std::unordered_map<int32_t, int64_t> checkpoint_pending_offsets_;
  std::optional<std::unordered_set<int32_t>> assigned_partitions_;
  std::unordered_set<int32_t> eof_partitions_;
  bool done_ = false;
};

class FromKafkaPlugin final : public virtual OperatorPlugin {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `kafka.yaml` instead",
                               this->name())
        .to_error();
    }
    auto defaults = record{};
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
      defaults = flatten(*kafka_config);
    }();
    if (not defaults.contains("bootstrap.servers")) {
      defaults["bootstrap.servers"] = "localhost";
    }
    if (not defaults.contains("client.id")) {
      defaults["client.id"] = "tenzir";
    }
    source_global_defaults() = std::move(defaults);
    return caf::none;
  }

  auto name() const -> std::string override {
    return "from_kafka";
  }

  auto describe() const -> Description override {
    auto initial = FromKafkaArgs{};
    initial.options = located{record{}, location::unknown};
    auto d = Describer<FromKafkaArgs, FromKafkaOperator>{std::move(initial)};
    d.positional("topic", &FromKafkaArgs::topic);
    d.named("count", &FromKafkaArgs::count);
    d.named("exit", &FromKafkaArgs::exit);
    auto offset_arg = d.named("offset", &FromKafkaArgs::offset);
    auto options_arg = d.named_optional("options", &FromKafkaArgs::options);
    auto aws_region_arg = d.named("aws_region", &FromKafkaArgs::aws_region);
    auto aws_iam_arg = d.named("aws_iam", &FromKafkaArgs::aws_iam);
    auto batch_size_arg = d.named_optional("commit_batch_size",
                                           &FromKafkaArgs::commit_batch_size);
    d.validate([=](ValidateCtx& ctx) -> Empty {
      if (auto commit_batch_size = ctx.get(batch_size_arg); commit_batch_size) {
        if (*commit_batch_size == 0) {
          diagnostic::error("`commit_batch_size` must be greater than zero")
            .primary(
              ctx.get_location(batch_size_arg).value_or(location::unknown))
            .emit(ctx);
          return {};
        }
      }
      if (auto options = ctx.get(options_arg); options) {
        if (not validate_options(*options, ctx)) {
          return {};
        }
        if (options->inner.contains("enable.auto.commit")) {
          diagnostic::error("`enable.auto.commit` must not be specified")
            .primary(options->source)
            .note("`enable.auto.commit` is enforced to be `false`")
            .emit(ctx);
          return {};
        }
      }
      if (auto offset = ctx.get(offset_arg); offset) {
        auto parsed = int64_t{};
        if (not parse_offset_value(*offset, parsed)) {
          diagnostic::error("invalid `offset` value")
            .primary(offset->source)
            .note("must be `beginning`, `end`, `stored`, `<offset>`, or "
                  "`-<offset>`")
            .emit(ctx);
          return {};
        }
      }
      if (auto iam = ctx.get(aws_iam_arg); iam) {
        if (auto options = ctx.get(options_arg); options) {
          if (not check_sasl_mechanism(*options, ctx)) {
            return {};
          }
        }
        auto aws = aws_iam_options::from_record(*iam, ctx);
        if (not aws) {
          return {};
        }
        if (not ctx.get(aws_region_arg) and not aws->region) {
          diagnostic::error(
            "`aws_region` is required for Kafka MSK authentication")
            .primary(iam->source)
            .emit(ctx);
          return {};
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::FromKafkaPlugin)
