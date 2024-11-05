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
#include <tenzir/die.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <chrono>
#include <string>

using namespace std::chrono_literals;
namespace tenzir::plugins::kafka {

namespace {

// Default topic if the user doesn't provide one.
constexpr auto default_topic = "tenzir";

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
  std::optional<located<std::string>> topic;
  std::optional<located<size_t>> count;
  std::optional<location> exit;
  std::optional<located<std::string>> offset;
  std::optional<located<std::string>> options;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("topic", x.topic), f.field("count", x.count),
              f.field("exit", x.exit), f.field("offset", x.offset),
              f.field("options", x.options));
  }
};

class kafka_loader final : public plugin_loader {
public:
  kafka_loader() = default;

  kafka_loader(loader_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
    if (!config_.contains("group.id")) {
      config_["group.id"] = "tenzir";
    }
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto cfg = configuration::make(config_);
    if (!cfg) {
      ctrl.diagnostics().emit(
        diagnostic::error("failed to create configuration: {}", cfg.error())
          .done());
      return {};
    }
    // If we want to exit when we're done, we need to tell Kafka to emit a
    // signal so that we know when to terminate.
    if (args_.exit) {
      if (auto err = cfg->set("enable.partition.eof", "true")) {
        ctrl.diagnostics().emit(
          diagnostic::error("failed to enable partition EOF: {}", err).done());
        return {};
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
      return {};
    }
    // Override configuration with arguments.
    if (args_.options) {
      std::vector<std::pair<std::string, std::string>> options;
      if (!parsers::kvp_list(args_.options->inner, options)) {
        ctrl.diagnostics().emit(
          diagnostic::error("invalid list of key=value pairs")
            .primary(args_.options->source)
            .done());
        return {};
      }
      for (const auto& [key, value] : options) {
        TENZIR_INFO("providing librdkafka option {}={}", key, value);
        if (auto err = cfg->set(key, value)) {
          ctrl.diagnostics().emit(
            diagnostic::error("failed to set librdkafka option {}={}: {}", key,
                              value, err)
              .primary(args_.options->source)
              .done());
          return {};
        }
      }
    }
    // Create the consumer.
    if (auto value = cfg->get("bootstrap.servers")) {
      TENZIR_INFO("kafka connects to broker: {}", *value);
    }
    auto client = consumer::make(*cfg);
    if (!client) {
      ctrl.diagnostics().emit(
        diagnostic::error("failed to create consumer: {}", client.error())
          .done());
      return {};
    };
    auto topic = args_.topic ? args_.topic->inner : default_topic;
    TENZIR_INFO("kafka subscribes to topic {}", topic);
    if (auto err = client->subscribe({topic})) {
      ctrl.diagnostics().emit(
        diagnostic::error("failed to subscribe to topic: {}", err).done());
      return {};
    }
    // Setup the coroutine factory.
    auto make
      = [](loader_args args, consumer client) mutable -> generator<chunk_ptr> {
      auto num_messages = size_t{0};
      while (true) {
        auto msg = client.consume(500ms);
        if (!msg) {
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
        if (args.count && args.count->inner == ++num_messages) {
          break;
        }
      }
    };
    return make(args_, std::move(*client));
  }

  auto name() const -> std::string override {
    return "kafka";
  }

  auto default_parser() const -> std::string override {
    return "json";
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
  std::optional<located<std::string>> topic;
  std::optional<located<std::string>> key;
  std::optional<located<std::string>> timestamp;
  std::optional<located<std::string>> options;

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("topic", x.topic), f.field("key", x.key),
              f.field("timestamp", x.timestamp), f.field("options", x.options));
  }
};

class kafka_saver final : public plugin_saver {
public:
  kafka_saver() = default;

  kafka_saver(saver_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto cfg = configuration::make(config_);
    if (!cfg) {
      TENZIR_ERROR("kafka failed to create configuration: {}", cfg.error());
      return cfg.error();
    };
    // Override configuration with arguments.
    if (args_.options) {
      std::vector<std::pair<std::string, std::string>> options;
      if (!parsers::kvp_list(args_.options->inner, options)) {
        diagnostic::error("invalid list of key=value pairs")
          .primary(args_.options->source)
          .throw_();
      }
      for (const auto& [key, value] : options) {
        TENZIR_INFO("providing librdkafka option {}={}", key, value);
        if (auto err = cfg->set(key, value)) {
          diagnostic::error("failed to set librdkafka option {}={}: {}", key,
                            value, err)
            .primary(args_.options->source)
            .throw_();
        }
      }
    }
    if (auto value = cfg->get("bootstrap.servers")) {
      TENZIR_INFO("kafka connects to broker: {}", *value);
    }
    auto client = producer::make(*cfg);
    if (!client) {
      TENZIR_ERROR(client.error());
      return client.error();
    };
    auto guard = caf::detail::make_scope_guard([client = *client]() mutable {
      TENZIR_VERBOSE("waiting 10 seconds to flush pending messages");
      if (auto err = client.flush(10s)) {
        TENZIR_WARN(err);
      }
      auto num_messages = client.queue_size();
      if (num_messages > 0) {
        TENZIR_ERROR("{} messages were not delivered", num_messages);
      }
    });
    auto topic = args_.topic ? args_.topic->inner : default_topic;
    auto topics = std::vector<std::string>{std::move(topic)};
    std::string key;
    if (args_.key) {
      key = args_.key->inner;
    }
    time timestamp;
    if (args_.timestamp) {
      auto result = parsers::time(args_.timestamp->inner, timestamp);
      TENZIR_ASSERT(result); // validated earlier
    }
    return [&ctrl, client = *client, key = std::move(key), ts = timestamp,
            topics = std::move(topics),
            guard = std::make_shared<decltype(guard)>(std::move(guard))](
             chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0) {
        return;
      }
      for (const auto& topic : topics) {
        TENZIR_DEBUG("publishing {} bytes to topic {}", chunk->size(), topic);
        if (auto error = client.produce(topic, as_bytes(*chunk), key, ts)) {
          diagnostic::error(error).emit(ctrl.diagnostics());
          return;
        }
      }
      // It's advised to call poll periodically to tell Kafka "you can flush
      // buffered messages if you like".
      client.poll(0ms);
    };
  }

  auto name() const -> std::string override {
    return "kafka";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
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
