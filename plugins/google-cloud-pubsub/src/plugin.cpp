//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <fmt/core.h>

#include "loader.hpp"
#include "saver.hpp"

namespace tenzir::plugins::google_cloud_pubsub {

class plugin final : public virtual saver_plugin<saver>,
                     public virtual loader_plugin<loader> {
public:
  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = saver::args{};
    args.add_to(parser);
    parser.parse(p);
    return std::make_unique<saver>(std::move(args));
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = loader::args{};
    args.add_to(parser);
    parser.parse(p);
    return std::make_unique<loader>(std::move(args));
  }

  auto name() const -> std::string override {
    return "google-cloud-pubsub";
  }
};

} // namespace tenzir::plugins::google_cloud_pubsub

TENZIR_REGISTER_PLUGIN(tenzir::plugins::google_cloud_pubsub::plugin)
