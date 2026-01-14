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
#include "tenzir/series_builder.hpp"

#include <tenzir/generator.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>

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
  std::optional<configuration::aws_iam_options> aws;
  location operator_location;

  friend auto inspect(auto& f, from_kafka_args& x) -> bool {
    return f.object(x).fields(
      f.field("topic", x.topic), f.field("count", x.count),
      f.field("exit", x.exit), f.field("offset", x.offset),
      f.field("commit_batch_size", x.commit_batch_size),
      f.field("options", x.options), f.field("aws", x.aws),
      f.field("operator_location", x.operator_location));
  }
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
    co_yield {};
    auto& dh = ctrl.diagnostics();
    auto cfg = configuration::make(config_, args_.aws, dh);
    if (not cfg) {
      diagnostic::error("failed to create configuration: {}", cfg.error())
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    }
    // If we want to exit when we're done, we need to tell Kafka to emit a
    // signal so that we know when to terminate.
    if (args_.exit) {
      if (auto err = cfg->set("enable.partition.eof", "true"); err.valid()) {
        diagnostic::error("failed to enable partition EOF: {}", err)
          .primary(args_.operator_location)
          .emit(dh);
        co_return;
      }
    }
    // Disable auto-commit to use manual commit for precise message counting
    if (auto err = cfg->set("enable.auto.commit", "false"); err.valid()) {
      diagnostic::error("failed to disable auto-commit: {}", err)
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    }
    // Adjust rebalance callback to set desired offset.
    auto offset = RdKafka::Topic::OFFSET_STORED;
    if (args_.offset) {
      auto success = offset_parser()(args_.offset->inner, offset);
      TENZIR_ASSERT(success); // validated earlier;
      TENZIR_INFO("kafka adjusts offset to {} ({})", args_.offset->inner,
                  offset);
    }
    if (auto err = cfg->set_rebalance_cb(offset); err.valid()) {
      diagnostic::error("failed to set rebalance callback: {}", err)
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    }
    // Override configuration with arguments.
    {
      auto secrets = configure_or_request(args_.options, *cfg, dh);
      co_yield ctrl.resolve_secrets_must_yield(std::move(secrets));
    }
    // Create the consumer.
    if (auto value = cfg->get("bootstrap.servers")) {
      TENZIR_INFO("kafka connecting to broker: {}", *value);
    }
    auto client = consumer::make(*cfg);
    if (not client) {
      diagnostic::error("failed to create consumer: {}", client.error())
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    };
    TENZIR_INFO("kafka subscribes to topic {}", args_.topic);
    if (auto err = client->subscribe({args_.topic}); err.valid()) {
      diagnostic::error("failed to subscribe to topic: {}", err)
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    }
    auto num_messages = size_t{0};
    auto last_commit_time = time::clock::now();
    auto last_good_message = std::shared_ptr<RdKafka::Message>{};

    // Track EOF status per partition for proper multi-partition handling
    auto partition_count = std::optional<size_t>{};
    auto eof_partition_count = size_t{0};

    const auto schema = type{
      "tenzir.kafka",
      record_type{
        {"message", string_type{}},
      },
    };
    const auto arrow_schema = schema.to_arrow_schema();
    auto b = string_type::make_arrow_builder(arrow_memory_pool());
    const auto finish_as_slice = [&] -> table_slice {
      const auto l = b->length();
      return table_slice{
        arrow::RecordBatch::Make(arrow_schema, l, {finish(*b)}),
      };
    };
    while (true) {
      auto raw_msg = client->consume_raw(500ms);
      TENZIR_ASSERT(raw_msg);
      const auto now = time::clock::now();
      switch (raw_msg->err()) {
        case RdKafka::ERR_NO_ERROR: {
          last_good_message = std::move(raw_msg);
          check(b->Append(
            reinterpret_cast<const char*>(last_good_message->payload()),
            detail::narrow<int32_t>(last_good_message->len())));
          // Manually commit this specific message after processing
          ++num_messages;
          if (last_good_message
              and (num_messages % args_.commit_batch_size == 0
                   or now - last_commit_time >= commit_timeout)) {
            last_commit_time = now;
            co_yield finish_as_slice();
            if (not client->commit(last_good_message.get(), dh,
                                   args_.operator_location)) {
              co_return;
            }
            last_good_message.reset();
            continue;
          }
          if (last_good_message and args_.count
              and args_.count->inner == num_messages) {
            co_yield finish_as_slice();
            std::ignore = client->commit(last_good_message.get(), dh,
                                         args_.operator_location);
            co_return;
          }
          co_yield {};
          continue;
        }
        case RdKafka::ERR__TIMED_OUT: {
          if (last_good_message and now - last_commit_time >= commit_timeout) {
            last_commit_time = now;
            co_yield finish_as_slice();
            if (not client->commit(last_good_message.get(), dh,
                                   args_.operator_location)) {
              co_return;
            }
            last_good_message.reset();
          } else {
            co_yield {};
          }
          continue;
        }
        case RdKafka::ERR__PARTITION_EOF: {
          // Get partition count if not already retrieved
          if (not partition_count) {
            auto pc = client->get_partition_count(args_.topic);
            if (not pc) {
              diagnostic::error("failed to get partition count: {}", pc.error())
                .primary(args_.operator_location)
                .emit(dh);
              co_return;
            }
            partition_count = *pc;
            TENZIR_DEBUG("kafka topic {} has {} partitions", args_.topic,
                         *partition_count);
          }
          ++eof_partition_count;
          TENZIR_DEBUG("kafka partition {} reached EOF ({}/{} partitions EOF)",
                       raw_msg->partition(), eof_partition_count,
                       *partition_count);
          // Only exit if all partitions have reached EOF
          if (eof_partition_count == *partition_count) {
            // Kafka allows the number of partitions to increase, so we need
            // to re-check here.
            auto pc = client->get_partition_count(args_.topic);
            if (not pc) {
              diagnostic::error("failed to get partition count: {}", pc.error())
                .primary(args_.operator_location)
                .emit(dh);
              co_return;
            }
            if (*pc == *partition_count) {
              if (last_good_message) {
                co_yield finish_as_slice();
                last_commit_time = now;
                std::ignore = client->commit(last_good_message.get(), dh,
                                             args_.operator_location);
              }
              co_yield {};
              co_return;
            }
          }
          co_yield {};
          continue;
        }
        default: {
          if (last_good_message) {
            auto ndh = transforming_diagnostic_handler{
              dh,
              [](auto&& diag) {
                return std::move(diag)
                  .modify()
                  .severity(severity::warning)
                  .done();
              },
            };
            co_yield finish_as_slice();
            last_commit_time = now;
            std::ignore = client->commit(last_good_message.get(), ndh,
                                         args_.operator_location);
          }
          diagnostic::error("unexpected kafka error: `{}`", raw_msg->errstr())
            .primary(args_.operator_location)
            .emit(dh);
          co_yield {};
          co_return;
        }
      }
    }
  }

  auto detached() const -> bool override {
    return true;
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
          .named("aws_iam", iam_opts)
          .named_optional("commit_batch_size", args.commit_batch_size)
          .parse(inv, ctx));
    if (iam_opts) {
      TRY(check_sasl_mechanism(args.options, ctx));
      TRY(check_sasl_mechanism(located{config_, iam_opts->source}, ctx));
      args.options.inner["sasl.mechanism"] = "OAUTHBEARER";
      TRY(args.aws, configuration::aws_iam_options::from_record(
                      std::move(iam_opts).value(), ctx));
    }
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
