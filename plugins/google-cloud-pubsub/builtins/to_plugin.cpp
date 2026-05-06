//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/location.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/variant.hpp>

#include <google/cloud/future.h>
#include <google/cloud/internal/future_coroutines.h>
#include <google/cloud/pubsub/publisher.h>

#include <chrono>
#include <deque>
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

// Note: We intentionally do not flush the publisher in its destructor.
// During regular shutdown, finalize() handles flushing and awaiting all
// inflight futures. During abnormal shutdown (hard stop), a destructor-only
// flush would be insufficient since it doesn't wait for inflight futures
// to complete.

class ToGoogleCloudPubsub final : public Operator<table_slice, void> {
public:
  using publish_future
    = google::cloud::future<google::cloud::StatusOr<std::string>>;

  struct InflightPublish {
    publish_future future;
    size_t bytes = 0;
  };

  explicit ToGoogleCloudPubsub(to_args args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_google_cloud_pubsub"},
                         MetricsDirection::write, MetricsVisibility::external_);
    auto topic = pubsub::Topic(args_.project_id.inner, args_.topic_id.inner);
    publisher_.emplace(pubsub::MakePublisherConnection(std::move(topic)));
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (input.rows() == 0 or not publisher_) {
      co_return;
    }
    auto& dh = ctx.dh();
    for (const auto& messages : eval(args_.message, input, dh)) {
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
            const auto data = array.GetView(i);
            inflight_.push_back(InflightPublish{
              .future = publisher_->Publish(
                pubsub::MessageBuilder{}.SetData(std::string{data}).Build()),
              .bytes = data.size(),
            });
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
    // Prune completed publish futures to prevent unbounded memory growth.
    while (not inflight_.empty() and inflight_.front().future.is_ready()) {
      auto publish = std::move(inflight_.front());
      inflight_.pop_front();
      auto result = publish.future.get();
      handle_publish_result(result, publish.bytes, dh);
    }
    co_return;
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    co_await drain_inflight(ctx.dh());
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    co_await drain_inflight(ctx.dh());
    co_return FinalizeBehavior::done;
  }

private:
  auto handle_publish_result(google::cloud::StatusOr<std::string>& result,
                             size_t bytes, diagnostic_handler& dh) -> void {
    if (not result) {
      diagnostic::error("failed to publish: {}", result.status().message())
        .emit(dh);
      return;
    }
    if (bytes > 0) {
      bytes_write_counter_.add(bytes);
    }
  }

  auto drain_inflight(diagnostic_handler& dh) -> Task<void> {
    if (publisher_) {
      publisher_->Flush();
    }
    while (not inflight_.empty()) {
      auto publish = std::move(inflight_.front());
      inflight_.pop_front();
      auto result = co_await std::move(publish.future);
      handle_publish_result(result, publish.bytes, dh);
    }
  }

  to_args args_;
  Option<pubsub::Publisher> publisher_;
  std::deque<InflightPublish> inflight_;
  MetricsCounter bytes_write_counter_;
};

} // namespace

class to_plugin final
  : public virtual operator_plugin2<to_google_cloud_pubsub_operator>,
    public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
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

  auto describe() const -> Description override {
    auto d = Describer<to_args, ToGoogleCloudPubsub>{};
    d.operator_location(&to_args::op);
    d.named("project_id", &to_args::project_id);
    d.named("topic_id", &to_args::topic_id);
    d.named("message", &to_args::message, "string");
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::to_plugin)
