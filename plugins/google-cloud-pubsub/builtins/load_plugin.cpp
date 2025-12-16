//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/diagnostics.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <google/cloud/pubsub/subscriber.h>

#include "uri_transform.hpp"

namespace tenzir::plugins::google_cloud_pubsub {

namespace {

namespace pubsub = ::google::cloud::pubsub;

struct loader_args {
  located<std::string> project_id;
  located<std::string> subscription_id;
  located<duration> timeout = located{duration::zero(), location::unknown};
  located<duration> yield_timeout
    = located{defaults::import::batch_timeout, location::unknown};

  auto add_to(argument_parser2& parser) -> void {
    parser.named("project_id", project_id);
    parser.named("subscription_id", subscription_id);
    parser.named_optional("timeout", timeout);
    parser.named_optional("_yield_timeout", timeout);
  }

  auto validate(diagnostic_handler& dh) -> failure_or<void> {
    if (timeout.inner < duration::zero()) {
      diagnostic::error("timeout duration may not be negative")
        .primary(timeout.source)
        .emit(dh);
      return failure::promise();
    }
    if (timeout.inner == duration::zero()) {
      timeout.inner = std::chrono::years{100};
    }
    if (yield_timeout.inner <= duration::zero()) {
      diagnostic::error("_yield_timeout must be larger than zero")
        .primary(yield_timeout.source)
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, loader_args& x) -> bool {
    return f.object(x).fields(f.field("project_id", x.project_id),
                              f.field("topic_id", x.subscription_id),
                              f.field("timeout", x.timeout),
                              f.field("_yield_timeout", x.yield_timeout));
  }
};

class load_operator final : public crtp_operator<load_operator> {
public:
  load_operator() = default;

  explicit load_operator(loader_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    co_yield {};
    auto subscription = pubsub::Subscription(args_.project_id.inner,
                                             args_.subscription_id.inner);
    auto connection = pubsub::MakeSubscriberConnection(std::move(subscription));
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
      auto result = session.wait_for(args_.yield_timeout.inner);
      if (result == std::future_status::ready) {
        // This should never happen
        break;
      }
      auto now = std::chrono::system_clock::now();
      if (now - last_message_time > args_.timeout.inner) {
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
  }

  auto detached() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "tql2.load_google_cloud_pubsub";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  loader_args args_;
};

} // namespace

class load_plugin final : public operator_plugin2<load_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    diagnostic::warning("`load_google_cloud_pubsub` is deprecated; use "
                        "`from_google_cloud_pubsub` instead")
      .primary(inv.self)
      .emit(ctx);
    auto args = loader_args{};
    auto parser = argument_parser2::operator_("load_google_cloud_pubsub");
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<load_operator>(std::move(args));
  }

  // virtual auto load_properties() const -> load_properties_t override {
  //   return {
  //     .schemes = {"gcps"},
  //     .accepts_pipeline = false,
  //     .strip_scheme = true,
  //     .transform_uri = make_uri_transform("subscription_id"),
  //   };
  // }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::load_plugin)
