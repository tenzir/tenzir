//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series_builder.hpp"

#include <tenzir/async.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <folly/coro/BoundedQueue.h>
#include <google/cloud/pubsub/subscriber.h>
#include <google/cloud/pubsub/subscription_admin_client.h>

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "uri_transform.hpp"

namespace tenzir::plugins::google_cloud_pubsub {

namespace {

namespace pubsub = ::google::cloud::pubsub;

struct from_args {
  located<std::string> project_id;
  located<std::string> subscription_id;
  located<duration> yield_timeout
    = located{defaults::import::batch_timeout, location::unknown};
  Option<ast::field_path> metadata_field;
  bool ordered = true;
  location operator_location;

  auto add_to(argument_parser2& parser) -> void {
    parser.named("project_id", project_id);
    parser.named("subscription_id", subscription_id);
    parser.named_optional("_yield_timeout", yield_timeout);
    parser.named("metadata_field", metadata_field);
  }

  auto validate(diagnostic_handler& dh) -> failure_or<void> {
    if (yield_timeout.source != location::unknown) {
      diagnostic::warning("`_yield_timeout` is deprecated and has no effect")
        .primary(yield_timeout.source)
        .emit(dh);
    }
    return {};
  }

  friend auto inspect(auto& f, from_args& x) -> bool {
    return f.object(x).fields(f.field("project_id", x.project_id),
                              f.field("subscription_id", x.subscription_id),
                              f.field("_yield_timeout", x.yield_timeout),
                              f.field("metadata_field", x.metadata_field),
                              f.field("ordered", x.ordered),
                              f.field("operator_location",
                                      x.operator_location));
  }
};

class from_google_cloud_pubsub_operator final
  : public crtp_operator<from_google_cloud_pubsub_operator> {
public:
  from_google_cloud_pubsub_operator() = default;

  explicit from_google_cloud_pubsub_operator(from_args args)
    : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    // Setup subscription
    auto subscription = pubsub::Subscription(args_.project_id.inner,
                                             args_.subscription_id.inner);
    const auto ordering_enabled = args_.ordered and [&]() {
      auto admin_client = pubsub::SubscriptionAdminClient(
        pubsub::MakeSubscriptionAdminConnection());
      auto subscription_info = admin_client.GetSubscription(subscription);
      if (not subscription_info.ok()) {
        return false;
      }
      return subscription_info->enable_message_ordering();
    }
    ();
    auto connection = pubsub::MakeSubscriberConnection(
      std::move(subscription),
      google::cloud::Options{}.set<pubsub::MaxConcurrencyOption>(1));
    auto subscriber = pubsub::Subscriber(std::move(connection));
    // The pubsub library does not conclusively specify that only one callback
    // will be *executed* at a time. Hence we still have a mutex here to be safe.
    auto builder_mut = std::mutex{};
    auto msb = multi_series_builder{
      {.settings={
         .ordered = ordering_enabled,
         .raw = true,
       }},
      ctrl.diagnostics(),
    };
    auto session = subscriber.Subscribe(
      [&](pubsub::Message const& m, pubsub::AckHandler h) {
        {
          auto guard = std::scoped_lock{builder_mut};
          auto event = msb.record();
          event.field("message").data(m.data());
          if (args_.metadata_field) {
            auto meta = event.field(*args_.metadata_field).record();
            meta.field("message_id").data(m.message_id());
            meta.field("publish_time").data(time{m.publish_time()});
            auto attrs = meta.field("attributes").record();
            for (const auto& [key, value] : m.attributes()) {
              attrs.field(key).data(value);
            }
          }
        }
        std::move(h).ack();
      });
    auto shared_diagnostics = ctrl.shared_diagnostics();
    auto session_guard = detail::scope_guard{[&]() noexcept {
      if (not session.valid()) {
        return;
      }
      if (not session.is_ready()) {
        // Initiate cancellation of the subscription.
        session.cancel();
      }
      // Always wait for the session to fully stop. This is critical to ensure
      // that gRPC background threads are no longer accessing captured locals
      // (builder_mut, msb, args_) before they are destroyed.
      const auto session_status = session.get();
      if (not session_status.ok()
          and session_status.code() != google::cloud::StatusCode::kCancelled) {
        diagnostic::error("google-cloud-subscriber: {}",
                          session_status.message())
          .primary(args_.operator_location)
          .emit(shared_diagnostics);
      }
    }};
    while (session.valid()) {
      if (session.is_ready()) {
        break;
      }
      // Must hold the mutex while collecting slices to prevent concurrent
      // modification by the callback. We collect slices under the lock, then
      // yield outside.
      auto slices = [&] {
        auto guard = std::scoped_lock{builder_mut};
        return msb.yield_ready_as_table_slice();
      }();
      auto yielded = false;
      for (auto&& s : slices) {
        yielded = true;
        co_yield std::move(s);
      }
      if (not yielded) {
        co_yield {};
      }
    }
    session_guard.trigger();
    for (auto&& s : msb.finalize_as_table_slice()) {
      co_yield std::move(s);
    }
  }

  auto name() const -> std::string override {
    return "from_google_cloud_pubsub";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order order) const
    -> optimize_result override {
    auto args = args_;
    args.ordered = order == event_order::ordered;

    return {
      std::nullopt, order,
      std::make_unique<from_google_cloud_pubsub_operator>(std::move(args_))};
  }

  friend auto inspect(auto& f, from_google_cloud_pubsub_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  from_args args_;
};

// Holds state shared between the operator and the GCP subscriber callback.
// Uses a mutex because GCP only limits the number of callbacks *scheduled*,
// not the number actually executing at a time, even with
// MaxConcurrencyOption(1).
// See
// https://cloud.google.com/cpp/docs/reference/pubsub/latest/structgoogle_1_1cloud_1_1pubsub_1_1MaxConcurrencyOption
struct MessageData {
  std::string data;
  std::string message_id;
  time publish_time;
  std::unordered_map<std::string, std::string> attributes;
};

// TODO: We acknowledge messages immediately upon receipt rather than deferring
// acks until after a checkpoint commit. This means we provide at-most-once
// delivery semantics instead of at-least-once. The proper approach would be to
// hold AckHandlers and only ack after a successful checkpoint commit (with
// deduplication via seen_message_ids to handle redeliveries). However, until we
// have guarantees on checkpoint frequency, deferring acks would lead to
// unbounded memory growth from accumulating AckHandlers and message IDs between
// checkpoints.

struct SharedState {
  folly::coro::BoundedQueue<MessageData> queue{1024};
  Notify notify;
  std::atomic<bool> session_done{false};
  std::string session_error;
};

// RAII wrapper that cancels the GCP subscriber session on destruction and
// blocks until all background threads stop accessing shared state. This
// ensures no callbacks run after the operator is destroyed.
struct SessionHandle {
  google::cloud::future<google::cloud::Status> fut;

  SessionHandle() = default;
  explicit SessionHandle(google::cloud::future<google::cloud::Status> f)
    : fut{std::move(f)} {
  }
  SessionHandle(const SessionHandle&) = delete;
  auto operator=(const SessionHandle&) -> SessionHandle& = delete;
  SessionHandle(SessionHandle&&) = default;
  auto operator=(SessionHandle&&) -> SessionHandle& = default;

  ~SessionHandle() noexcept {
    if (not fut.valid()) {
      return;
    }
    if (not fut.is_ready()) {
      fut.cancel();
    }
    // Wait with a timeout to avoid blocking indefinitely during shutdown.
    fut.wait_for(std::chrono::seconds{5});
  }
};

class FromGoogleCloudPubsub final : public Operator<void, table_slice> {
public:
  explicit FromGoogleCloudPubsub(from_args args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    bytes_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_google_cloud_pubsub"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_read_counter_
      = ctx.make_counter(MetricsLabel{"operator", "from_google_cloud_pubsub"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    auto subscription = pubsub::Subscription(args_.project_id.inner,
                                             args_.subscription_id.inner);
    // Detect whether the subscription has message ordering enabled.
    auto ordering_enabled = [&]() -> bool {
      auto admin_client = pubsub::SubscriptionAdminClient(
        pubsub::MakeSubscriptionAdminConnection());
      auto sub_info = admin_client.GetSubscription(subscription);
      if (not sub_info.ok()) {
        return false;
      }
      return sub_info->enable_message_ordering();
    }();
    ordering_enabled_ = ordering_enabled;

    auto connection = pubsub::MakeSubscriberConnection(
      std::move(subscription),
      google::cloud::Options{}.set<pubsub::MaxConcurrencyOption>(1));
    auto subscriber = pubsub::Subscriber(std::move(connection));

    auto shared = shared_;
    auto has_metadata = args_.metadata_field.is_some();
    // Subscribe and attach a .then() callback to detect session end. The
    // callback runs on a GCP background thread when the session terminates
    // (e.g., due to an error), setting session_done and notifying the operator.
    auto session_fut = subscriber.Subscribe(
      [shared, has_metadata](pubsub::Message const& m, pubsub::AckHandler h) {
        auto msg = MessageData{
          .data = std::string{m.data()},
          .message_id = m.message_id(),
          .publish_time = time{m.publish_time()},
          .attributes = {},
        };
        if (has_metadata) {
          for (const auto& [key, value] : m.attributes()) {
            msg.attributes.emplace(key, value);
          }
        }
        if (shared->queue.try_enqueue(std::move(msg))) {
          std::move(h).ack();
        } else {
          std::move(h).nack();
        }
        shared->notify.notify_one();
      });
    session_.emplace(SessionHandle{
      std::move(session_fut)
        .then([shared](google::cloud::future<google::cloud::Status> f) {
          auto status = f.get();
          if (not status.ok()
              and status.code() != google::cloud::StatusCode::kCancelled) {
            shared->session_error = status.message();
          }
          shared->session_done.store(true, std::memory_order_release);
          shared->notify.notify_one();
          return status;
        })});
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
    }
    if (not shared_->queue.empty()
        or shared_->session_done.load(std::memory_order_acquire)) {
      co_return {};
    }
    co_await shared_->notify.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    // Drain all available messages from the queue.
    auto messages = std::vector<MessageData>{};
    while (auto msg = shared_->queue.try_dequeue()) {
      messages.push_back(std::move(*msg));
    }

    if (not messages.empty()) {
      auto msb = multi_series_builder{
        {.settings={
           .ordered = ordering_enabled_,
           .raw = true,
         }},
        ctx.dh(),
      };
      for (const auto& msg : messages) {
        if (not msg.data.empty()) {
          bytes_read_counter_.add(msg.data.size());
        }
        auto event = msb.record();
        event.field("message").data(msg.data);
        if (args_.metadata_field) {
          auto meta = event.field(*args_.metadata_field).record();
          meta.field("message_id").data(msg.message_id);
          meta.field("publish_time").data(msg.publish_time);
          auto attrs = meta.field("attributes").record();
          for (const auto& [key, value] : msg.attributes) {
            attrs.field(key).data(value);
          }
        }
      }
      for (auto&& slice : msb.finalize_as_table_slice()) {
        auto const rows = slice.rows();
        co_await push(std::move(slice));
        events_read_counter_.add(rows);
      }
    }

    // Check whether the session ended (set by the .then() callback).
    // Only transition to done when the queue is also empty, since a message
    // callback may have enqueued items just before the session terminated.
    if (shared_->session_done.load(std::memory_order_acquire)
        and shared_->queue.empty()) {
      auto error = std::move(shared_->session_error);
      if (not error.empty()) {
        diagnostic::error("google-cloud-subscriber: {}", error)
          .primary(args_.operator_location)
          .emit(ctx);
      }
      done_ = true;
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  from_args args_;
  bool ordering_enabled_ = false;
  bool done_ = false;
  Option<SessionHandle> session_;
  std::shared_ptr<SharedState> shared_ = std::make_shared<SharedState>();
  MetricsCounter bytes_read_counter_;
  MetricsCounter events_read_counter_;
};

} // namespace

class from_plugin final
  : public virtual operator_plugin2<from_google_cloud_pubsub_operator>,
    public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_args{};
    auto parser = argument_parser2::operator_(name());
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
    TRY(args.validate(ctx));
    args.operator_location = inv.self.get_location();
    return std::make_unique<from_google_cloud_pubsub_operator>(std::move(args));
  }

  auto describe() const -> Description override {
    auto d = Describer<from_args, FromGoogleCloudPubsub>{};
    d.operator_location(&from_args::operator_location);
    d.named("project_id", &from_args::project_id);
    d.named("subscription_id", &from_args::subscription_id);
    d.named("metadata_field", &from_args::metadata_field);
    d.named_optional("_yield_timeout", &from_args::yield_timeout);
    return d.without_optimize();
  }

  // auto load_properties() const -> load_properties_t override {
  //   return {
  //     .schemes = {"gcps"},
  //     .accepts_pipeline = false,
  //     .strip_scheme = true,
  //     .transform_uri = make_uri_transform("subscription_id"),
  //   };
  // }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::from_plugin)
