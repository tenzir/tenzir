//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"
#include "kafka/consumer.hpp"
#include "kafka/message.hpp"
#include "kafka/producer.hpp"

#include <vast/argument_parser.hpp>
#include <vast/chunk.hpp>
#include <vast/concept/parseable/numeric.hpp>
#include <vast/concept/parseable/string.hpp>
#include <vast/concept/parseable/vast/option_set.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/data.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/die.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>

#include <charconv>
#include <chrono>
#include <string>

using namespace std::chrono_literals;

namespace vast::plugins::kafka {

namespace {

// Default topic if the user doesn't provide one.
constexpr auto default_topic = "tenzir";

// Valid values:
// - beginning | end | stored |
// - <value>  (absolute offset) |
// - -<value> (relative offset from end)
// - s@<value> (timestamp in ms to start at)
// - e@<value> (timestamp in ms to stop at (not included))
auto offset_parser() {
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

auto kvp_parser() {
  using namespace parsers;
  using namespace parser_literals;
  using parsers::printable;
  auto key = *(printable - '=');
  auto value = *(printable - ',');
  auto kvp = key >> '=' >> value;
  return kvp % ',';
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
    if (!config_.contains("group.id"))
      config_["group.id"] = "tenzir";
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
    // signal so that we known when to terminate.
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
      VAST_ASSERT(success); // validated earlier;
      VAST_INFO("kafka adjusts offset to {} ({})", args_.offset->inner, offset);
    }
    if (auto err = cfg->set_rebalance_cb(offset)) {
      ctrl.diagnostics().emit(
        diagnostic::error("failed to set rebalance callback: {}", err).done());
      return {};
    }
    // Override configuration with arguments.
    if (args_.options) {
      std::vector<std::pair<std::string, std::string>> options;
      if (!kvp_parser()(args_.options->inner, options)) {
        ctrl.diagnostics().emit(
          diagnostic::error("invalid list of key=value pairs")
            .primary(args_.options->source)
            .done());
        return {};
      }
      for (const auto& [key, value] : options) {
        VAST_INFO("providing librdkafka option {}={}", key, value);
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
    auto client = consumer::make(*cfg);
    if (!client) {
      ctrl.diagnostics().emit(
        diagnostic::error("failed to create consumer: {}", client.error())
          .done());
      return {};
    };
    if (auto value = cfg->get("bootstrap.servers")) {
      VAST_INFO("kafka consumer connected to: {}", *value);
    }
    auto topic = args_.topic ? args_.topic->inner : default_topic;
    VAST_INFO("kafka subscribes to topic {}", topic);
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
          if (msg.error() == ec::timeout)
            continue;
          if (msg.error() == ec::end_of_input)
            // FIXME: currently doesn't work for N partitions with N > 1.
            // Upgrade to a counter and only break out of the loop once this
            // signal has been received N times.
            break;
          VAST_ERROR(msg.error());
          break;
        }
        auto payload = chunk::copy(msg->payload());
        co_yield payload;
        if (args.count && args.count->inner == ++num_messages)
          break;
      }
    };
    return make(args_, std::move(*client));
  }

  auto to_string() const -> std::string override {
    auto result = name();
    result += fmt::format(" --topic {}", args_.topic);
    if (args_.count)
      result += fmt::format(" --count {}", args_.count->inner);
    if (args_.exit)
      result += "--exit";
    if (args_.offset)
      result += fmt::format(" --offset {}", args_.offset->inner);
    if (args_.options)
      result += fmt::format(" --set {}", args_.options->inner);
    return result;
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

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("topic", x.topic), f.field("key", x.key));
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
      VAST_ERROR("kafka failed to create configuration: {}", cfg.error());
      return cfg.error();
    };
    if (auto value = cfg->get("bootstrap.servers")) {
      VAST_INFO("kafka consumer connected to: {}", *value);
    }
    auto client = producer::make(*cfg);
    if (!client) {
      VAST_ERROR(client.error());
      return client.error();
    };
    auto guard = caf::detail::make_scope_guard([client = *client]() mutable {
      VAST_VERBOSE("waiting 10 seconds to flush pending messages");
      if (auto err = client.flush(10s))
        VAST_WARN(err);
      auto num_messages = client.queue_size();
      if (num_messages > 0)
        VAST_ERROR("{} messages were not delivered", num_messages);
    });
    auto topic = args_.topic ? args_.topic->inner : default_topic;
    auto topics = std::vector<std::string>{std::move(topic)};
    std::string key;
    if (args_.key)
      key = args_.key->inner;
    return [&ctrl, client = *client, key = std::move(key),
            topics = std::move(topics),
            guard = std::make_shared<decltype(guard)>(std::move(guard))](
             chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0) {
        return;
      }
      for (const auto& topic : topics) {
        VAST_DEBUG("publishing {} bytes to topic {}", chunk->size(), topic);
        if (auto error = client.produce(topic, key, as_bytes(*chunk))) {
          ctrl.abort(std::move(error));
          return;
        }
      }
      client.poll(0ms);
    };
  }

  // FIXME: why can't I override this for savers when it's possible for
  // loaders? auto to_string() const -> std::string override {
  //  auto result = name();
  //  result += fmt::format(" --topic {}", args_.topic);
  //  return result;
  //}

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

class plugin final : public virtual loader_plugin<kafka_loader>,
                     saver_plugin<kafka_saver> {
public:
  auto initialize(const record& config, const record& /* global_config */)
    -> caf::error override {
    config_ = config;
    if (!config_.contains("bootstrap.servers"))
      config_["bootstrap.servers"] = "localhost";
    if (!config_.contains("client.id"))
      config_["client.id"] = "tenzir";
    return caf::none;
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/next/connectors/{}", name())};
    auto args = loader_args{};
    parser.add("-t,--topic", args.topic, "<topic>");
    parser.add("-c,--count", args.count, "<n>");
    parser.add("-e,--exit", args.exit);
    parser.add("-o,--offset", args.offset, "<offset>");
    // We use -X because that's standard in Kafka applications, cf. kcat.
    parser.add("-X,--set", args.options, "<key=value>,...");
    parser.parse(p);
    if (args.offset) {
      if (!offset_parser()(args.offset->inner))
        diagnostic::error("invalid `--offset` value")
          .primary(args.offset->source)
          .note("valid values are:")
          .note("- beginning")
          .note("- end")
          .note("- store")
          .note("- <value> (absolute offset)")
          .note("- -<value> (relative offset from end)")
          .throw_();
    }
    return std::make_unique<kafka_loader>(std::move(args), config_);
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/next/connectors/{}", name())};
    auto args = saver_args{};
    parser.add("-t,--topic", args.topic, "<topic>");
    parser.add("-k,--key", args.key, "<key>");
    parser.parse(p);
    return std::make_unique<kafka_saver>(std::move(args), config_);
  }

  auto name() const -> std::string override {
    return "kafka";
  }

private:
  record config_;
};

} // namespace

} // namespace vast::plugins::kafka

VAST_REGISTER_PLUGIN(vast::plugins::kafka::plugin)
