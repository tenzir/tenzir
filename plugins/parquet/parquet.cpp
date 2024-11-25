//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/operator.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/drain_bytes.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::parquet {

namespace {

class plugin final : public virtual parser_plugin<parquet_parser>,
                     public virtual printer_plugin<parquet_printer> {
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"parquet", "https://docs.tenzir.com/"
                                             "formats/parquet"};
    parser.parse(p);
    return std::make_unique<parquet_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{"parquet", "https://docs.tenzir.com/"
                                             "formats/parquet"};
    auto options = parquet_options{};
    parser.add("--compression-level", options.compression_level, "<level>");
    parser.add("--compression-type", options.compression_type, "<type>");
    parser.parse(p);
    return std::make_unique<parquet_printer>(std::move(options));
  }

  [[nodiscard]] std::string name() const override {
    return "parquet";
  }
};
} // namespace

} // namespace tenzir::plugins::parquet

// Finally, register our plugin.
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::plugin)
