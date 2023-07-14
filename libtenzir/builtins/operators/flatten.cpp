//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser.hpp"
#include "tenzir/pipeline.hpp"

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::flatten {

namespace {

constexpr auto default_flatten_separator = ".";

class flatten_operator final : public crtp_operator<flatten_operator> {
public:
  flatten_operator() = default;

  flatten_operator(std::string separator) : separator_{std::move(separator)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto seen = std::unordered_set<type>{};
    for (auto&& slice : input) {
      auto result = tenzir::flatten(slice, separator_);
      // We only warn once per schema that we had to rename a set of fields.
      if (seen.insert(slice.schema()).second
          && not result.renamed_fields.empty()) {
        ctrl.warn(
          caf::make_error(ec::convert_error,
                          fmt::format("the flatten operator renamed fields due "
                                      "to conflicting names: {}",
                                      fmt::join(result.renamed_fields, ", "))));
      }
      co_yield std::move(result).slice;
    }
  }

  auto to_string() const -> std::string override {
    return fmt::format("flatten '{}'", separator_);
  }

  auto name() const -> std::string override {
    return "flatten";
  }

  friend auto inspect(auto& f, flatten_operator& x) -> bool {
    return f.apply(x.separator_);
  }

private:
  std::string separator_ = default_flatten_separator;
};

class plugin final : public virtual operator_plugin<flatten_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser
      = argument_parser{"flatten", "https://docs.tenzir.com/next/operators/"
                                   "transformations/flatten"};
    auto sep = std::optional<located<std::string>>{};
    parser.add(sep, "<separator>");
    parser.parse(p);
    auto separator = (sep) ? sep->inner : default_flatten_separator;
    return std::make_unique<flatten_operator>(separator);
  }
};

} // namespace

} // namespace tenzir::plugins::flatten

TENZIR_REGISTER_PLUGIN(tenzir::plugins::flatten::plugin)
