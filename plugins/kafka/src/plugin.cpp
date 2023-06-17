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

#include <vast/chunk.hpp>
#include <vast/concept/parseable/vast/option_set.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/data.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/die.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>

#include <chrono>
#include <string>

using namespace std::chrono_literals;

namespace vast::plugins::kafka {

namespace {

/// The configuration for the `kafka` operator.
struct plugin_configuration {
  std::vector<std::string> options;

  template <class Inspector>
  friend auto inspect(Inspector& f, plugin_configuration& x) {
    return detail::apply_all(f, x.options);
  }

  static inline auto schema() noexcept -> const record_type& {
    static auto result = record_type{
      {"options", list_type{string_type{}}},
    };
    return result;
  }
};

class plugin : public virtual loader_plugin, saver_plugin {
public:
  auto initialize(const record& config, const record& global_config)
    -> caf::error override {
    return caf::none;
  }

  auto
  make_loader(std::span<std::string const> args, operator_control_plane&) const
    -> caf::expected<generator<chunk_ptr>> override {
    auto f = [=]() -> generator<chunk_ptr> {
      auto topics = std::vector<std::string>{"test"};
      auto cfg = configuration::make();
      if (!cfg) {
        VAST_ERROR("kafka failed to create configuration: {}", cfg.error());
        co_return;
      };
      auto client = consumer::make(*cfg);
      if (!client) {
        VAST_ERROR(client.error());
        co_return;
      };
      if (auto value = cfg->get("bootstrap.servers")) {
        VAST_INFO("kafka consumer connected to: {}", *value);
      }
      VAST_INFO("kafka subscribes to topics {}", topics);
      if (auto err = client->subscribe(topics)) {
        VAST_ERROR(err);
        co_return;
      }
      while (true) {
        auto msg = client->consume(500ms);
        if (!msg) {
          if (msg.error() != ec::timeout)
            VAST_WARN(msg.error());
          co_yield {};
          continue;
        }
        auto payload = chunk::copy(msg->payload());
        if (auto err = client->commit(*msg)) {
          VAST_WARN("kafka failed to commit message: {}", *payload);
          co_yield {};
          continue;
        }
        co_yield payload;
      }
    };
    return std::invoke(f);
  }

  auto make_saver(std::span<std::string const> /* args */, printer_info,
                  operator_control_plane& ctrl) const
    -> caf::expected<saver> override {
    auto topics = std::vector<std::string>{"test"};
    auto cfg = configuration::make();
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
    return [&ctrl, topics = std::move(topics), client = *client,
            guard = std::make_shared<decltype(guard)>(std::move(guard))](
             chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0) {
        return;
      }
      for (const auto& topic : topics) {
        VAST_DEBUG("publishing {} bytes to topic {}", chunk->size(), topic);
        if (auto error = client.produce(topic, as_bytes(*chunk))) {
          ctrl.abort(std::move(error));
          return;
        }
      }
      client.poll(0ms);
    };
  }

  auto default_parser(std::span<std::string const>) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"json", {}};
  }

  auto default_printer(std::span<std::string const>) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"json", {}};
  }

  auto saver_does_joining() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "kafka";
  }
};

} // namespace

} // namespace vast::plugins::kafka

VAST_REGISTER_PLUGIN(vast::plugins::kafka::plugin)
