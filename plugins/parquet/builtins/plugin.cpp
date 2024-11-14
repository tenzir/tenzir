//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "parquet/operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::parquet {

namespace {

class read_plugin final
  : public virtual operator_plugin2<parser_adapter<parquet_parser>> {
public:
  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(this->name()).parse(inv, ctx));
    return std::make_unique<parser_adapter<parquet_parser>>(parquet_parser{});
  }
};

class write_plugin final
  : public virtual operator_plugin2<writer_adapter<parquet_printer>> {
public:
  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto options = parquet_options{};
    TRY(argument_parser2::operator_(this->name())
          .add("compression_level", options.compression_level)
          .add("compression_type", options.compression_type)
          .parse(inv, ctx));
    return std::make_unique<writer_adapter<parquet_printer>>(parquet_printer{});
  }
};
} // namespace
} // namespace tenzir::plugins::parquet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::read_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::write_plugin)
