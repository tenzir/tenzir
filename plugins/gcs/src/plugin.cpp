//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <operator.hpp>

namespace tenzir::plugins::gcs {

namespace {

class plugin final : public virtual loader_plugin<gcs_loader>,
                     public virtual saver_plugin<gcs_saver> {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return caf::none;
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = gcs_args{};
    parser.add("--anonymous", args.anonymous);
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("gs://")) {
      args.uri.inner = fmt::format("gs://{}", args.uri.inner);
    }
    return std::make_unique<gcs_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = gcs_args{};
    parser.add("--anonymous", args.anonymous);
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("gs://")) {
      args.uri.inner = fmt::format("gs://{}", args.uri.inner);
    }
    return std::make_unique<gcs_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "gcs";
  }

  auto supported_uri_schemes() const -> std::vector<std::string> override {
    return {"gs"};
  }
};

} // namespace
} // namespace tenzir::plugins::gcs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::gcs::plugin)
