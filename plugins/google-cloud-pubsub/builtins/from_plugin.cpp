//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/defaults.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <google/cloud/pubsub/subscriber.h>

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
                              f.field("metadata_field", x.metadata_field));
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
    // Define output schema
    const auto output_type = type{
      "tenzir.google_cloud_pubsub",
      record_type{
        {"message", string_type{}},
      },
    };
    // Setup subscription
    auto subscription = pubsub::Subscription(args_.project_id.inner,
                                             args_.subscription_id.inner);
    auto connection = pubsub::MakeSubscriberConnection(std::move(subscription));
    auto subscriber = pubsub::Subscriber(std::move(connection));
    // Thread-safe series builders
    auto message_builder = series_builder{output_type};
    auto metadata_builder = series_builder{};
    std::mutex builder_mut;
    auto last_yield_time = std::chrono::system_clock::now();
    // Subscribe with callback that writes directly to builders
    auto session = subscriber.Subscribe(
      [&](pubsub::Message const& m, pubsub::AckHandler h) {
        std::scoped_lock guard{builder_mut};
        // Build message record
        message_builder.record().field("message", m.data());
        // Build metadata record if needed
        if (args_.metadata_field) {
          auto meta = metadata_builder.record();
          meta.field("message_id", m.message_id());
          meta.field("publish_time", time{m.publish_time().time_since_epoch()});
          auto attrs = meta.field("attributes").record();
          for (const auto& [key, value] : m.attributes()) {
            attrs.field(key, value);
          }
        }
        std::move(h).ack();
      });
    // Main loop with periodic flushing
    while (session.valid()) {
      // Wait for yield_timeout
      if (session.is_ready()) {
        break;
      }
      auto now = std::chrono::system_clock::now();
      if (now - last_yield_time > args_.yield_timeout.inner) {
        auto guard = std::scoped_lock{builder_mut};
        // Yield accumulated messages if any
        if (message_builder.length() > 0) {
          auto slice = message_builder.finish_assert_one_slice();
          if (args_.metadata_field) {
            auto metadata = metadata_builder.finish_assert_one_array();
            slice = assign(*args_.metadata_field, std::move(metadata), slice,
                           ctrl.diagnostics());
          }
          co_yield std::move(slice);
        }
      }
      co_yield {};
    }
    // Cleanup and error handling
    if (session.is_ready()) {
      auto status = session.get();
      if (not status.ok()) {
        diagnostic::error("google-cloud-subscriber: {}", status.message())
          .emit(ctrl.diagnostics());
      }
    } else if (session.valid()) {
      session.cancel();
    }
    // Final flush of remaining messages
    {
      std::scoped_lock guard{builder_mut};
      if (message_builder.length() > 0) {
        auto slice = message_builder.finish_assert_one_slice();
        if (args_.metadata_field) {
          auto metadata = metadata_builder.finish_assert_one_array();
          slice = assign(*args_.metadata_field, std::move(metadata), slice,
                         ctrl.diagnostics());
        }
        co_yield std::move(slice);
      }
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "from_google_cloud_pubsub";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
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
