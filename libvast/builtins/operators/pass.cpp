//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/argument_parser.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>

namespace vast::plugins::pass {

namespace {

// Does nothing with the input.
class pass_operator final : public crtp_operator<pass_operator> {
public:
  template <operator_input_batch T>
  auto operator()(T x) const -> T {
    return x;
  }

  auto predicate_pushdown(expression const& expr) const
    -> std::optional<std::pair<expression, operator_ptr>> override {
    return std::pair{expr, std::make_unique<pass_operator>(*this)};
  }

  auto name() const -> std::string override {
    return "pass";
  }

  friend auto inspect(auto& f, pass_operator& x) -> bool {
    (void)f, (void)x;
    return true;
  }
};

class plugin final : public virtual operator_plugin<pass_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    argument_parser{"pass"}.parse(p);
    return std::make_unique<pass_operator>();
  }
};

} // namespace

} // namespace vast::plugins::pass

VAST_REGISTER_PLUGIN(vast::plugins::pass::plugin)
