//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/weak_run_delayed.hpp"

#include <tenzir/concepts.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/location.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/variant.hpp>

#include <google/cloud/future.h>
#include <google/cloud/pubsub/publisher.h>

#include <chrono>
#include <string_view>
#include <vector>

namespace tenzir::plugins::google_cloud_pubsub {

namespace {

namespace pubsub = ::google::cloud::pubsub;

struct to_args {
  location op;
  located<std::string> project_id;
  located<std::string> topic_id;
  ast::expression message;

  friend auto inspect(auto& f, to_args& x) -> bool {
    return f.object(x).fields(f.field("op", x.op),
                              f.field("project_id", x.project_id),
                              f.field("topic_id", x.topic_id),
                              f.field("message", x.message));
  }
};

class to_google_cloud_pubsub_operator final
  : public crtp_operator<to_google_cloud_pubsub_operator> {
public:
  to_google_cloud_pubsub_operator() = default;

  explicit to_google_cloud_pubsub_operator(to_args args)
    : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    co_yield {};
    auto topic = pubsub::Topic(args_.project_id.inner, args_.topic_id.inner);
    auto connection = pubsub::MakePublisherConnection(std::move(topic));
    auto publisher = pubsub::Publisher(std::move(connection));
    auto& dh = ctrl.diagnostics();
    constexpr auto timeout = std::chrono::seconds{30};
    using publish_future
      = google::cloud::future<google::cloud::StatusOr<std::string>>;
    struct pending_publish {
      publish_future future;
      std::chrono::steady_clock::time_point started_at;
    };
    auto pending = std::vector<pending_publish>{};
    auto still_pending = std::vector<pending_publish>{};
    auto batch_start = std::chrono::steady_clock::now();
    const auto publish_view = [&](std::string_view view) {
      pending.emplace_back(
        publisher.Publish(
          pubsub::MessageBuilder{}.SetData(std::string{view}).Build()),
        batch_start);
    };
    auto timeout_warned = false;
    const auto flush_pending =
      [&](const std::chrono::steady_clock::time_point& now) {
        still_pending.clear();
        still_pending.reserve(pending.size());
        for (auto& entry : pending) {
          if (entry.future.is_ready()) {
            auto id = entry.future.get();
            if (not id) {
              diagnostic::error("failed to publish: {}", id.status().message())
                .primary(args_.op)
                .emit(dh);
            }
            continue;
          }
          if (not timeout_warned and now - entry.started_at >= timeout) {
            diagnostic::warning("reached a {} timeout while trying to publish",
                                timeout)
              .primary(args_.op)
              .emit(dh);
            timeout_warned = true;
          }
          still_pending.push_back(std::move(entry));
        }
        pending.swap(still_pending);
      };
    auto finalize_guard = detail::scope_guard{[&]() noexcept {
      // Flush all pending publishes before destruction. This ensures that gRPC
      // has completed all in-flight operations before the publisher is
      // destroyed.
      publisher.Flush();
      for (auto& [future, _] : pending) {
        auto s = future.wait_for(timeout);
        if (s != std::future_status::ready) {
          if (not timeout_warned) {
            diagnostic::warning("reached a {} timeout while trying to publish",
                                timeout)
              .primary(args_.op)
              .emit(dh);
            timeout_warned = true;
          }
          continue;
        }
        auto id = future.get();
        if (not id) {
          diagnostic::error("failed to publish: {}", id.status().message())
            .primary(args_.op)
            .emit(dh);
        }
      }
    }};
    for (const auto& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      batch_start = std::chrono::steady_clock::now();
      for (const auto& messages : eval(args_.message, slice, dh)) {
        match(
          *messages.array,
          [&](const arrow::StringArray& array) {
            for (auto i = int64_t{}; i < array.length(); ++i) {
              if (array.IsNull(i)) {
                diagnostic::warning("expected `string`, got `null`")
                  .primary(args_.message)
                  .emit(dh);
                continue;
              }
              publish_view(array.GetView(i));
            }
          },
          [&](const auto&) {
            diagnostic::warning("expected `string`, got `{}`",
                                messages.type.kind())
              .primary(args_.message)
              .note("event is skipped")
              .emit(dh);
          });
      }
      flush_pending(std::chrono::steady_clock::now());
      co_yield {};
    }
    finalize_guard.trigger();
  }

  auto name() const -> std::string override {
    return "to_google_cloud_pubsub";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, to_google_cloud_pubsub_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  to_args args_;
};

} // namespace

class to_plugin final
  : public operator_plugin2<to_google_cloud_pubsub_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = to_args{};
    TRY(argument_parser2::operator_(name())
          .named("project_id", args.project_id)
          .named("topic_id", args.topic_id)
          .named("message", args.message, "string")
          .parse(inv, ctx));
    args.op = inv.self.get_location();
    return std::make_unique<to_google_cloud_pubsub_operator>(std::move(args));
  }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::to_plugin)
