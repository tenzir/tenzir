//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/diagnostics.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <google/cloud/pubsub/publisher.h>

#include "uri_transform.hpp"

namespace tenzir::plugins::google_cloud_pubsub {

namespace {

namespace pubsub = ::google::cloud::pubsub;

struct saver_args {
  located<std::string> project_id;
  located<std::string> topic_id;

  auto add_to(argument_parser2& parser) -> void {
    parser.named("project_id", project_id);
    parser.named("topic_id", topic_id);
  }

  friend auto inspect(auto& f, saver_args& x) -> bool {
    return f.object(x).fields(f.field("project_id", x.project_id),
                              f.field("topic_id", x.topic_id));
  }
};

class save_operator final : public crtp_operator<save_operator> {
public:
  save_operator() = default;

  explicit save_operator(saver_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    co_yield {};
    auto topic = pubsub::Topic(args_.project_id.inner, args_.topic_id.inner);
    auto connection = pubsub::MakePublisherConnection(std::move(topic));
    auto publisher = pubsub::Publisher(std::move(connection));
    for (auto chunk : input) {
      if (not chunk or chunk->size() == 0) {
        co_yield {};
        continue;
      }
      auto message
        = pubsub::MessageBuilder{}
            .SetData(std::string{reinterpret_cast<const char*>(chunk->data()),
                                 chunk->size()})
            .Build();
      auto future = publisher.Publish(std::move(message));
      constexpr auto timeout = std::chrono::seconds{30};
      auto res = future.wait_for(timeout);
      if (res != std::future_status::ready) {
        diagnostic::error("google cloud publisher reached a {} timeout",
                          timeout)
          .emit(ctrl.diagnostics());
      }
      auto id = future.get();
      if (not id) {
        diagnostic::error("google-cloud-publisher: {}", id.status().message())
          .emit(ctrl.diagnostics());
      }
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "tql2.save_google_cloud_pubsub";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, save_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  saver_args args_;
};

} // namespace

class save_plugin final : public operator_plugin2<save_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    diagnostic::warning("`save_google_cloud_pubsub` is deprecated; use "
                        "`to_google_cloud_pubsub` instead")
      .primary(inv.self)
      .emit(ctx);
    auto args = saver_args{};
    auto parser = argument_parser2::operator_("save_google_cloud_pubsub");
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
    return std::make_unique<save_operator>(std::move(args));
  }

  // virtual auto save_properties() const -> save_properties_t override {
  //   return {
  //     .schemes = {"gcps"},
  //     .accepts_pipeline = false,
  //     .strip_scheme = true,
  //     .transform_uri = make_uri_transform("topic_id"),
  //   };
  // }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::save_plugin)
