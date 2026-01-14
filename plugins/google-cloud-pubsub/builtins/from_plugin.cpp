//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series_builder.hpp"

#include <tenzir/defaults.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <google/cloud/pubsub/subscriber.h>
#include <google/cloud/pubsub/subscription_admin_client.h>

#include "uri_transform.hpp"

namespace tenzir::plugins::google_cloud_pubsub {

namespace {

namespace pubsub = ::google::cloud::pubsub;

struct from_args {
  located<std::string> project_id;
  located<std::string> subscription_id;
  located<duration> yield_timeout
    = located{defaults::import::batch_timeout, location::unknown};
  std::optional<ast::field_path> metadata_field;
  bool ordered = true;
  location operator_location;

  auto add_to(argument_parser2& parser) -> void {
    parser.named("project_id", project_id);
    parser.named("subscription_id", subscription_id);
    parser.named_optional("_yield_timeout", yield_timeout);
    parser.named("metadata_field", metadata_field);
  }

  auto validate(diagnostic_handler& dh) -> failure_or<void> {
    if (yield_timeout.inner <= duration::zero()) {
      diagnostic::error("_yield_timeout must be larger than zero")
        .primary(yield_timeout.source)
        .emit(dh);
      return failure::promise();
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
    while (session.valid()) {
      if (session.is_ready()) {
        break;
      }
      auto yielded = false;
      for (auto&& s : msb.yield_ready_as_table_slice()) {
        yielded = true;
        co_yield std::move(s);
      }
      if (not yielded) {
        co_yield {};
      }
    }
    if (session.valid()) {
      if (not session.is_ready()) {
        // Initiate cancellation of the subscription.
        session.cancel();
      }
      // Always wait for the session to fully stop. This is critical to ensure
      // that gRPC background threads are no longer accessing captured locals
      // (builder_mut, msb, args_) before they are destroyed.
      auto status = session.get();
      if (not status.ok()
          and status.code() != google::cloud::StatusCode::kCancelled) {
        diagnostic::error("google-cloud-subscriber: {}", status.message())
          .primary(args_.operator_location)
          .emit(ctrl.diagnostics());
      }
    }
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

} // namespace

class from_plugin final
  : public operator_plugin2<from_google_cloud_pubsub_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_args{};
    auto parser = argument_parser2::operator_(name());
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
    TRY(args.validate(ctx));
    args.operator_location = inv.self.get_location();
    return std::make_unique<from_google_cloud_pubsub_operator>(std::move(args));
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
