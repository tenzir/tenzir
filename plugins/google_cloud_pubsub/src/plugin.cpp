//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

#include "loader.hpp"
#include "saver.hpp"

namespace tenzir::plugins::google_cloud_pubsub {

// class plugin final : public virtual saver_plugin<saver>,
//                      public virtual loader_plugin<loader> {
// public:
//   auto parse_saver(parser_interface& p) const
//     -> std::unique_ptr<plugin_saver> override {
//     auto parser = argument_parser{
//       name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
//     auto args = saver::args{};
//     args.add_to(parser);
//     parser.parse(p);
//     return std::make_unique<saver>(std::move(args));
//   }

//   auto parse_loader(parser_interface& p) const
//     -> std::unique_ptr<plugin_loader> override {
//     auto parser = argument_parser{
//       name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
//     auto args = loader::args{};
//     args.add_to(parser);
//     parser.parse(p);
//     return std::make_unique<loader>(std::move(args));
//   }

//   auto name() const -> std::string override {
//     return "google-cloud-pubsub";
//   }
// };

class sink_op final : public crtp_operator<sink_op> {
public:
  sink_op() = default;

  sink_op(saver::args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "publish_google";
  }

  auto
  operator()(generator<chunk_ptr> input,
             operator_control_plane& ctrl) const -> generator<std::monostate> {
    auto topic = pubsub::Topic(args_.project_id.inner, args_.topic_id.inner);
    auto connection = pubsub::MakePublisherConnection(std::move(topic));
    auto publisher = pubsub::Publisher(std::move(connection));
    for (auto&& chunk : input) {
      if (not chunk) {
        co_yield {};
        continue;
      }
      auto message
        = pubsub::MessageBuilder{}
            .SetData(std::string{reinterpret_cast<const char*>(chunk->data()),
                                 chunk->size()})
            .Build();
      auto future = publisher.Publish(std::move(message));
      auto id = future.get();
      if (not id) {
        diagnostic::warning("{}", *id).emit(ctrl.diagnostics());
      }
    }
  }

  auto internal() const -> bool override {
    return true;
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, sink_op& x) -> bool {
    return f.apply(x.args_);
  }

  saver::args args_;
};

class publish_plugin final : public virtual operator_plugin<sink_op> {
  // public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.sink = true};
  }

  // auto
  // make(invocation inv, session ctx) const -> failure_or<operator_ptr>
  // override {
  //   auto parser = argument_parser2::operator_("publish_google");
  //   auto args = saver::args{};
  //   args.add_to(parser);
  //   TRY(parser.parse(inv, ctx));
  //   return std::make_unique<sink_op>(std::move(args));
  // }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = saver::args{};
    args.add_to(parser);
    parser.parse(p);
    return std::make_unique<sink_op>(std::move(args));
  }
};

} // namespace tenzir::plugins::google_cloud_pubsub

// TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::publish_plugin)
