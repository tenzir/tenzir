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

using namespace std::chrono_literals;
namespace tenzir::plugins::kafka {

namespace {

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

struct loader_args {
  std::string topic;
  std::optional<located<uint64_t>> count;
  std::optional<location> exit;
  std::optional<located<std::string>> offset;
  located<std::vector<std::pair<std::string, std::string>>> options;
  configuration::aws_iam_options aws;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("topic", x.topic), f.field("count", x.count),
              f.field("exit", x.exit), f.field("offset", x.offset),
              f.field("options", x.options));
  }
};

class kafka_loader final : public crtp_operator<kafka_loader> {
public:
  kafka_loader() = default;

  kafka_loader(loader_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
    if (!config_.contains("group.id")) {
      config_["group.id"] = "tenzir";
    }
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    co_yield {};
    auto cfg = configuration::make(config_, args_.aws, ctrl.diagnostics());
    if (!cfg) {
      ctrl.diagnostics().emit(
        diagnostic::error("failed to create configuration: {}", cfg.error())
          .done());
      co_return;
    }
    // If we want to exit when we're done, we need to tell Kafka to emit a
    // signal so that we know when to terminate.
    if (args_.exit) {
      if (auto err = cfg->set("enable.partition.eof", "true")) {
        ctrl.diagnostics().emit(
          diagnostic::error("failed to enable partition EOF: {}", err).done());
      }
    }
    // Adjust rebalance callback to set desired offset.
    auto offset = RdKafka::Topic::OFFSET_END;
    if (args_.offset) {
      auto success = offset_parser()(args_.offset->inner, offset);
      TENZIR_ASSERT(success); // validated earlier;
      TENZIR_INFO("kafka adjusts offset to {} ({})", args_.offset->inner,
                  offset);
    }
    if (auto err = cfg->set_rebalance_cb(offset)) {
      ctrl.diagnostics().emit(
        diagnostic::error("failed to set rebalance callback: {}", err).done());
    }
    // Override configuration with arguments.
    if (not args_.options.inner.empty()) {
      for (const auto& [key, value] : args_.options.inner) {
        TENZIR_INFO("providing librdkafka option {}={}", key, value);
        if (auto err = cfg->set(key, value)) {
          diagnostic::error("failed to set librdkafka option {}={}: {}", key,
                            value, err)
            .primary(args_.options.source)
            .emit(ctrl.diagnostics());
        }
      }
    }
    // Create the consumer.
    if (auto value = cfg->get("bootstrap.servers")) {
      TENZIR_INFO("kafka connecting to broker: {}", *value);
    }
    auto client = consumer::make(*cfg);
    if (! client) {
      diagnostic::error("failed to create consumer: {}", client.error())
        .emit(ctrl.diagnostics());
    };
    TENZIR_INFO("kafka subscribes to topic {}", args_.topic);
    if (auto err = client->subscribe({args_.topic})) {
      diagnostic::error("failed to subscribe to topic: {}", err)
        .emit(ctrl.diagnostics());
    }
    // Setup the coroutine factory.
    auto num_messages = size_t{0};
    while (true) {
      auto msg = client->consume(500ms);
      if (! msg) {
        co_yield {};
        if (msg.error() == ec::timeout) {
          continue;
        }
        if (msg.error() == ec::end_of_input) {
          // FIXME: currently doesn't work for N partitions with N > 1.
          // Upgrade to a counter and only break out of the loop once this
          // signal has been received N times.
          break;
        }
        TENZIR_ERROR(msg.error());
        break;
      }
      co_yield *msg;
      if (args_.count && args_.count->inner == ++num_messages) {
        break;
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
  located<std::vector<std::pair<std::string, std::string>>> options;
  configuration::aws_iam_options aws;

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("topic", x.topic), f.field("key", x.key),
              f.field("timestamp", x.timestamp), f.field("options", x.options));
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
    if (!cfg) {
      diagnostic::error(cfg.error()).emit(ctrl.diagnostics());
    };
    // Override configuration with arguments.
    if (not args_.options.inner.empty()) {
      for (const auto& [key, value] : args_.options.inner) {
        TENZIR_INFO("providing librdkafka option {}={}", key, value);
        if (auto err = cfg->set(key, value)) {
          diagnostic::error("failed to set librdkafka option {}={}: {}", key,
                            value, err)
            .primary(args_.options.source)
            .throw_();
        }
      }
    }
    if (auto value = cfg->get("bootstrap.servers")) {
      TENZIR_INFO("kafka connecting to broker: {}", *value);
    }
    auto client = producer::make(*cfg);
    if (!client) {
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
    auto topics = std::vector<std::string>{std::move(args_.topic)};
    std::string key;
    if (args_.key) {
      key = args_.key->inner;
    }
    time timestamp;
    if (args_.timestamp) {
      auto result = parsers::time(args_.timestamp->inner, timestamp);
      TENZIR_ASSERT(result); // validated earlier
    }
    for (auto chunk : input) {
      if (!chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      for (const auto& topic : topics) {
        TENZIR_DEBUG("publishing {} bytes to topic {}", chunk->size(), topic);
        if (auto error
            = client->produce(topic, as_bytes(*chunk), key, timestamp)) {
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
} // namespace
} // namespace tenzir::plugins::kafka
