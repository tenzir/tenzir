#pragma once
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/argument_parser2.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <google/cloud/pubsub/subscriber.h>

namespace tenzir::plugins::google_cloud_pubsub {

namespace pubsub = ::google::cloud::pubsub;

class loader final : public plugin_loader {
public:
  struct args {
    located<std::string> project_id;
    located<std::string> subscription_id;
    std::optional<located<duration>> timeout
      = located{duration::zero(), location::unknown};
    std::optional<located<duration>> yield_timeout
      = located{defaults::import::batch_timeout, location::unknown};

    auto add_to(argument_parser& parser) -> void {
      parser.add(project_id, "<project-id>");
      parser.add(subscription_id, "<subscription-id>");
      parser.add("--timeout", timeout, "<duration>");
    }

    auto add_to(argument_parser2& parser) -> void {
      parser.add("project_id", project_id);
      parser.add("subscription_id", subscription_id);
      parser.add("timeout", timeout);
      parser.add("_yield_timeout", timeout);
    }

    friend auto inspect(auto& f, args& x) -> bool {
      return f.object(x).fields(f.field("project_id", x.project_id),
                                f.field("topic_id", x.subscription_id),
                                f.field("timeout", x.timeout),
                                f.field("_yield_timeout", x.yield_timeout));
    }
  };

  loader() = default;

  loader(args args) : args_{std::move(args)} {
    TENZIR_ASSERT(args_.timeout);
    TENZIR_ASSERT(args_.yield_timeout);
    if (args_.timeout->inner < duration::zero()) {
      diagnostic::error("timeout duration may not be negative")
        .primary(args_.timeout->source)
        .throw_();
    }
    if (args_.timeout->inner == duration::zero()) {
      args_.timeout->inner = std::chrono::years{100};
    }
    if (args_.yield_timeout->inner <= duration::zero()) {
      diagnostic::error("_yield_timeout must be larger than zero")
        .primary(args_.yield_timeout->source)
        .throw_();
    }
  }
  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return [](operator_control_plane& ctrl,
              args args) mutable -> generator<chunk_ptr> {
      auto subscription = pubsub::Subscription(args.project_id.inner,
                                               args.subscription_id.inner);
      auto connection
        = pubsub::MakeSubscriberConnection(std::move(subscription));
      auto subscriber = pubsub::Subscriber(std::move(connection));
      auto chunks = std::vector<chunk_ptr>{};
      std::mutex chunks_mut;
      auto last_message_time = std::chrono::system_clock::now();
      auto append_chunk = [&](const std::string& data) {
        std::scoped_lock guard{chunks_mut};
        last_message_time = std::chrono::system_clock::now();
        chunks.push_back(chunk::copy(data));
      };
      auto session = subscriber.Subscribe(
        [&](pubsub::Message const& m, pubsub::AckHandler h) {
          append_chunk(m.data());
          std::move(h).ack();
        });
      while (session.valid()) {
        for (auto&& chunk : chunks) {
          co_yield std::move(chunk);
        }
        auto result = session.wait_for(args.yield_timeout->inner);
        if (result == std::future_status::ready) {
          // This should never happen
          break;
        }
        auto now = std::chrono::system_clock::now();
        if (now - last_message_time > args.timeout->inner) {
          break;
        }
      }
      if (session.is_ready()) {
        auto status = session.get();
        if (not status.ok()) {
          diagnostic::error("google-cloud-subscriber: {}", status.message())
            .emit(ctrl.diagnostics());
        }
      } else if (session.valid()) {
        session.cancel();
      }
      for (auto&& chunk : chunks) {
        co_yield std::move(chunk);
      }
    }(ctrl, args_);
  }

  auto name() const -> std::string override {
    return "google-cloud-pubsub";
  }

  friend auto inspect(auto& f, loader& x) -> bool {
    return f.apply(x.args_);
  }

private:
  args args_;
};

} // namespace tenzir::plugins::google_cloud_pubsub
