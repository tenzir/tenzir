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
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
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

// Note: We intentionally do not flush the publisher in its destructor.
// During regular shutdown, finalize() handles flushing and awaiting all
// inflight futures. During abnormal shutdown (hard stop), a destructor-only
// flush would be insufficient since it doesn't wait for inflight futures
// to complete.

template <class Input>
class ToGoogleCloudPubsub final : public Operator<Input, void> {
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
    if constexpr (std::same_as<Input, nova::Events>) {
      auto evaluator = co_await nova::Evaluator::make(args_.message, ctx);
      if (not evaluator) {
        co_return;
      }
      evaluator_ = std::move(*evaluator);
    }
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_google_cloud_pubsub"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_google_cloud_pubsub"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    auto topic = pubsub::Topic(args_.project_id.inner, args_.topic_id.inner);
    publisher_.emplace(pubsub::MakePublisherConnection(std::move(topic)));
    co_return;
  }

  auto process(Input input, OpCtx& ctx) -> Task<void> override {
    if (not publisher_) {
      co_return;
    }
    auto& dh = ctx.dh();
    if constexpr (std::same_as<Input, nova::Events>) {
      auto messages = evaluator_->eval(input, nova::EvalCtx{dh});
      for (auto row : nova::storage::true_bits(input.mask)) {
        match(messages.get(row), [&]<class Tag>(nova::RowView<Tag> value) {
          if constexpr (std::same_as<Tag, nova::String>) {
            inflight_.push_back(InflightPublish{
              .future = publisher_->Publish(
                pubsub::MessageBuilder{}.SetData(std::string{*value}).Build()),
              .bytes = (*value).size(),
            });
          } else {
            diagnostic::warning("expected `string`, got `{}`",
                                nova::Type<Tag>::static_name)
              .primary(args_.message)
              .note("event is skipped")
              .emit(dh);
          }
        });
      }
    } else {
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
    events_write_counter_.add(1);
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
  Option<nova::Evaluator> evaluator_;
  Option<pubsub::Publisher> publisher_;
  std::deque<InflightPublish> inflight_;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
};

} // namespace

class to_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_google_cloud_pubsub";
  }

  auto describe() const -> Description override {
    auto d = Describer<to_args, ToGoogleCloudPubsub<table_slice>,
                       ToGoogleCloudPubsub<nova::Events>>{};
    d.operator_location(&to_args::op);
    d.named("project_id", &to_args::project_id);
    d.named("topic_id", &to_args::topic_id);
    d.named("message", &to_args::message, "string");
    // Every instance opens its own publisher connection, and the sink does
    // not set ordering keys, so Pub/Sub provides no ordering guarantee that
    // replication could weaken.
    d.parallelizable();
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::to_plugin)
