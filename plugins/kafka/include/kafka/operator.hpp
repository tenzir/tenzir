//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "kafka/configuration.hpp"
#include "kafka/consumer.hpp"
#include "kafka/producer.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <chrono>
#include <string>
#include <utility>

using namespace std::chrono_literals;
namespace tenzir::plugins::kafka {

inline auto validate_options(const located<record>& r, diagnostic_handler& dh)
  -> failure_or<void> {
  for (const auto& [key, value] : r.inner) {
    const auto f = detail::overload{
      [](const concepts::arithmetic auto&) -> failure_or<void> {
        return {};
      },
      [](const std::string&) -> failure_or<void> {
        return {};
      },
      [](const secret&) -> failure_or<void> {
        return {};
      },
      [](const tenzir::pattern&) -> failure_or<void> {
        TENZIR_UNREACHABLE();
      },
      [&]<typename T>(const T&) -> failure_or<void> {
        diagnostic::error("options must be a record `{{ "
                          "string: number|string }}`")
          .primary(r.source, "key `{}` is `{}", key,
                   type_kind{tag_v<data_to_type_t<T>>})
          .emit(dh);
        return failure::promise();
      }};
    TRY(match(value, f));
  }
  return {};
}

inline auto check_sasl_mechanism(const located<record>& options,
                                 diagnostic_handler& dh) -> failure_or<void> {
  const auto it = options.inner.find("sasl.mechanism");
  if (it != options.inner.end()) {
    const auto* mechanism = try_as<std::string>(it->second);
    if (not mechanism) {
      diagnostic::error("option `sasl.mechanism` must be `string`")
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
    if (*mechanism != "OAUTHBEARER") {
      diagnostic::error("conflicting `sasl.mechanism`: `{}` "
                        "`but `aws_iam` requires `OAUTHBEARER`",
                        *mechanism)
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
  }
  const auto it_ms = options.inner.find("sasl.mechanisms");
  if (it_ms != options.inner.end()) {
    const auto* mechanisms = try_as<std::string>(it_ms->second);
    if (not mechanisms) {
      diagnostic::error("option `sasl.mechanisms` must be `string`")
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
    if (*mechanisms != "OAUTHBEARER") {
      diagnostic::error("conflicting `sasl.mechanisms`: `{}` "
                        "but `aws_iam` requires `OAUTHBEARER`",
                        *mechanisms)
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

// Valid values:
// - beginning | end | stored
// - <value>  (absolute offset)
// - -<value> (relative offset from end)
// - s@<value> (timestamp in ms to start at)
// - e@<value> (timestamp in ms to stop at (not included))
inline auto offset_parser() {
  using namespace parsers;
  using namespace parser_literals;
  auto beginning = "beginning"_p->*[] {
    return RdKafka::Topic::OFFSET_BEGINNING;
  };
  auto end = "end"_p->*[] {
    return RdKafka::Topic::OFFSET_END;
  };
  auto stored = "stored"_p->*[] {
    return RdKafka::Topic::OFFSET_STORED;
  };
  auto value = i64->*[](int64_t x) {
    return x >= 0 ? x : RdKafka::Consumer::OffsetTail(-x);
  };
  // TODO: support start and stop
  // auto start = "s@" >> i64;
  // auto stop = "3@" >> i64;
  // return beginning | end | stored | value | start | stop;
  return beginning | end | stored | value;
}

inline void
set_or_fail(const std::string& key, const std::string& value, location loc,
            kafka::configuration& cfg, diagnostic_handler& dh) {
  if (auto e = cfg.set(key, value)) {
    diagnostic::error("failed to set librdkafka option {}={}: {}", key, value,
                      e)
      .primary(loc)
      .emit(dh);
  }
};

[[nodiscard]] inline auto
configure_or_request(const located<record>& options, kafka::configuration& cfg,
                     diagnostic_handler& dh) -> std::vector<secret_request> {
  auto requests = std::vector<secret_request>{};

  for (const auto& [key, value] : options.inner) {
    match(
      value,
      [&](const concepts::arithmetic auto& v) {
        set_or_fail(key, fmt::to_string(v), options.source, cfg, dh);
      },
      [&](const std::string& s) {
        set_or_fail(key, s, options.source, cfg, dh);
      },
      [&](const secret& s) {
        requests.emplace_back(
          s, options.source,
          [&cfg, &dh, loc = options.source,
           key](const resolved_secret_value& v) -> failure_or<void> {
            TRY(auto str, v.utf8_view("options." + key, loc, dh));
            set_or_fail(key, std::string{str}, loc, cfg, dh);
            return {};
          });
      },
      [](const auto&) {
        /// This case should be covered by the early validation in `plugin::make`
        TENZIR_UNREACHABLE();
      });
  }
  return requests;
}

struct loader_args {
  std::string topic;
  std::optional<located<uint64_t>> count;
  std::optional<location> exit;
  std::optional<located<std::string>> offset;
  std::uint64_t commit_batch_size = 1000;
  duration commit_timeout = 10s;
  located<record> options;
  std::optional<configuration::aws_iam_options> aws;
  location operator_location;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("topic", x.topic), f.field("count", x.count),
              f.field("exit", x.exit), f.field("offset", x.offset),
              f.field("commit_batch_size", x.commit_batch_size),
              f.field("commit_timeout", x.commit_timeout),
              f.field("options", x.options), f.field("aws", x.aws),
              f.field("operator_location", x.operator_location));
  }
};

class kafka_loader final : public crtp_operator<kafka_loader> {
public:
  kafka_loader() = default;

  kafka_loader(loader_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
    if (! config_.contains("group.id")) {
      config_["group.id"] = "tenzir";
    }
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto& dh = ctrl.diagnostics();
    co_yield {};
    auto cfg = configuration::make(config_, args_.aws, dh);
    if (! cfg) {
      diagnostic::error("failed to create configuration: {}", cfg.error())
        .primary(args_.operator_location)
        .emit(dh);
      co_return;
    }
    // If we want to exit when we're done, we need to tell Kafka to emit a
    // signal so that we know when to terminate.
    if (args_.exit) {
      if (auto err = cfg->set("enable.partition.eof", "true")) {
        diagnostic::error("failed to enable partition EOF: {}", err)
          .primary(args_.operator_location)
          .emit(dh);
        co_return;
      }
    }
    // Disable auto-commit to use manual commit for precise message counting
    if (auto err = cfg->set("enable.auto.commit", "false")) {
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
    if (auto err = cfg->set_rebalance_cb(offset)) {
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
    if (! client) {
      diagnostic::error("failed to create consumer: {}", client.error())
        .primary(args_.operator_location)
        .emit(dh);
    };
    TENZIR_INFO("kafka subscribes to topic {}", args_.topic);
    if (auto err = client->subscribe({args_.topic})) {
      diagnostic::error("failed to subscribe to topic: {}", err)
        .primary(args_.operator_location)
        .emit(dh);
    }
    auto num_messages = size_t{0};
    auto last_commit_time = time::clock::now();
    auto last_good_message = std::shared_ptr<RdKafka::Message>{};

    // Track EOF status per partition for proper multi-partition handling
    auto partition_count = std::optional<size_t>{};
    auto eof_partition_count = size_t{0};

    while (true) {
      auto raw_msg = client->consume_raw(500ms);
      TENZIR_ASSERT(raw_msg);
      switch (raw_msg->err()) {
        case RdKafka::ERR_NO_ERROR: {
          last_good_message = std::move(raw_msg);
          // Create chunk from the message
          auto chunk = chunk::make(last_good_message->payload(),
                                   last_good_message->len(),
                                   [msg = last_good_message]() noexcept {});
          TENZIR_ASSERT(chunk);
          co_yield std::move(chunk);
          // Manually commit this specific message after processing
          ++num_messages;
          const auto now = time::clock::now();
          if (last_good_message
              and (num_messages % args_.commit_batch_size == 0
                   or last_commit_time - now >= args_.commit_timeout)) {
            last_commit_time = now;
            if (not client->commit(last_good_message.get(), dh,
                                   args_.operator_location)) {
              co_return;
            }
            last_good_message.reset();
          }
          if (last_good_message and args_.count
              and args_.count->inner == num_messages) {
            std::ignore = client->commit(last_good_message.get(), dh,
                                         args_.operator_location);
            co_return;
          }
          continue;
        }
        case RdKafka::ERR__TIMED_OUT: {
          const auto now = time::clock::now();
          if (last_good_message
              and last_commit_time - now >= args_.commit_timeout) {
            if (not client->commit(last_good_message.get(), dh,
                                   args_.operator_location)) {
              co_return;
            }
            last_good_message.reset();
          }
          co_yield {};
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
            // Kafka allows the number of partitions to increase, so we need to
            // re-check here.
            auto pc = client->get_partition_count(args_.topic);
            if (not pc) {
              diagnostic::error("failed to get partition count: {}", pc.error())
                .primary(args_.operator_location)
                .emit(dh);
              co_return;
            }
            if (*pc == *partition_count) {
              if (last_good_message) {
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
    return "load_kafka";
  }

  friend auto inspect(auto& f, kafka_loader& x) -> bool {
    return f.object(x)
      .pretty_name("kafka_loader")
      .fields(f.field("args", x.args_), f.field("config", x.config_));
  }

private:
  loader_args args_;
  record config_;
};

struct saver_args {
  std::string topic;
  std::optional<located<std::string>> key;
  std::optional<located<std::string>> timestamp;
  located<record> options;
  std::optional<configuration::aws_iam_options> aws;

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("topic", x.topic), f.field("key", x.key),
              f.field("timestamp", x.timestamp), f.field("options", x.options),
              f.field("aws", x.aws));
  }
};

class kafka_saver final : public crtp_operator<kafka_saver> {
public:
  kafka_saver() = default;

  kafka_saver(saver_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    co_yield {};
    auto cfg = configuration::make(config_, args_.aws, ctrl.diagnostics());
    if (not cfg) {
      diagnostic::error(cfg.error()).emit(ctrl.diagnostics());
      co_return;
    };
    // Override configuration with arguments.
    {
      auto secrets
        = configure_or_request(args_.options, *cfg, ctrl.diagnostics());
      co_yield ctrl.resolve_secrets_must_yield(std::move(secrets));
    }
    if (auto value = cfg->get("bootstrap.servers")) {
      TENZIR_INFO("kafka connecting to broker: {}", *value);
    }
    auto client = producer::make(*cfg);
    if (! client) {
      TENZIR_ERROR(client.error());
      diagnostic::error(client.error()).emit(ctrl.diagnostics());
    };
    auto guard = detail::scope_guard([client = *client]() mutable noexcept {
      TENZIR_VERBOSE("waiting 10 seconds to flush pending messages");
      if (auto err = client.flush(10s)) {
        TENZIR_WARN(err);
      }
      auto num_messages = client.queue_size();
      if (num_messages > 0) {
        TENZIR_ERROR("{} messages were not delivered", num_messages);
      }
    });
    auto topics = std::vector<std::string>{args_.topic};
    auto key = std::string{};
    if (args_.key) {
      key = args_.key->inner;
    }
    auto timestamp = time{};
    if (args_.timestamp) {
      auto result = parsers::time(args_.timestamp->inner, timestamp);
      TENZIR_ASSERT(result); // validated earlier
    }
    for (const auto& chunk : input) {
      if (not chunk or chunk->size() == 0) {
        co_yield {};
        continue;
      }
      for (const auto& topic : topics) {
        TENZIR_DEBUG("publishing {} bytes to topic {}", chunk->size(), topic);
        if (auto error
            = client->produce(topic, as_bytes(chunk), key, timestamp)) {
          diagnostic::error(error).emit(ctrl.diagnostics());
        }
      }
      // It's advised to call poll periodically to tell Kafka "you can flush
      // buffered messages if you like".
      client->poll(0ms);
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
    return "save_kafka";
  }

  friend auto inspect(auto& f, kafka_saver& x) -> bool {
    return f.object(x)
      .pretty_name("kafka_saver")
      .fields(f.field("args", x.args_), f.field("config", x.config_));
  }

private:
  saver_args args_;
  record config_;
};

} // namespace tenzir::plugins::kafka
