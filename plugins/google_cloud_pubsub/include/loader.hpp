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
#include <fmt/core.h>
#include <google/cloud/pubsub/subscriber.h>

namespace tenzir::plugins::google_cloud_pubsub {

namespace pubsub = ::google::cloud::pubsub;

constexpr auto yield_timeout = std::chrono::seconds(1);

class loader final : public plugin_loader {
public:
  struct args {
    located<std::string> project_id;
    located<std::string> subscription_id;

    auto add_to(argument_parser& parser) -> void {
      parser.add(project_id, "<project-id>");
      parser.add(subscription_id, "<subscription-id>");
    }

    auto add_to(argument_parser2& parser) -> void {
      parser.add("project_id", project_id);
      parser.add("subscription_id", subscription_id);
    }

    friend auto inspect(auto& f, args& x) -> bool {
      return f.object(x).fields(f.field("project_id", x.project_id),
                                f.field("topic_id", x.subscription_id));
    }
  };

  loader() = default;

  loader(args args) : args_{std::move(args)} {
  }
  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return
      [](operator_control_plane&, args args) mutable -> generator<chunk_ptr> {
        auto subscriber = pubsub::Subscriber(pubsub::MakeSubscriberConnection(
          pubsub::Subscription(args.project_id.inner,
                               args.subscription_id.inner)));
        auto chunks = std::vector<chunk_ptr>{};
        std::mutex chunks_mut;
        auto append_chunk = [&](const std::string& data) {
          std::scoped_lock guard{chunks_mut};
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
          auto result = session.wait_for(yield_timeout);
          if (result == std::future_status::ready) {
            break;
          }
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
