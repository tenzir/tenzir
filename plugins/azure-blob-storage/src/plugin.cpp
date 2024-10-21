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

namespace tenzir::plugins::abs {

class plugin final : public virtual saver_plugin<abs_saver>,
                     public virtual loader_plugin<abs_loader> {
public:
  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto uri = located<std::string>{};
    parser.add(uri, "<uri>");
    parser.parse(p);
    auto out = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri.inner, &out);
    if (auto s = opts.status(); s != arrow::Status::OK()) {
      diagnostic::error("Failed to parse URI {}", s.ToString())
        .primary(uri)
        .throw_();
    }
    return std::make_unique<abs_saver>(std::move(uri));
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto uri = located<std::string>{};
    parser.add(uri, "<uri>");
    parser.parse(p);
    auto out = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri.inner, &out);
    if (auto s = opts.status(); s != arrow::Status::OK()) {
      diagnostic::error("Failed to parse URI {}", s.ToString())
        .primary(uri)
        .throw_();
    }
    return std::make_unique<abs_loader>(std::move(uri));
  }

  auto name() const -> std::string override {
    return "azure-blob-storage";
  }

  auto supported_uri_schemes() const -> std::vector<std::string> override {
    return {"abfs", "abfss"};
  }
};

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::plugin)
