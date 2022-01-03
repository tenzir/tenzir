//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/select.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/type.h>
#include <caf/expected.hpp>

namespace vast {

select_step::select_step(std::string expr) : expression_(caf::no_error) {
  auto e = to<vast::expression>(std::move(expr));
  if (!e) {
    // TODO: Implement a better way to report errors, for example, a separate
    // setup() function.
    VAST_ERROR("the select step cannot use the expression: '{}', reason: '{}'",
               expr, e.error());
    expression_ = e;
    return;
  }
  expression_ = normalize_and_validate(*e);
  if (!expression_) {
    VAST_ERROR("the select step cannot validate the expression: '{}', reason: "
               "'{}'",
               expr, expression_.error());
  }
}

caf::expected<table_slice> select_step::operator()(table_slice&& slice) const {
  if (!expression_)
    return expression_.error();
  auto tailored_expr = tailor(*expression_, slice.layout());
  if (!tailored_expr)
    return tailored_expr.error();
  auto new_slice = filter(slice, *tailored_expr);
  if (new_slice)
    return *new_slice;
  return caf::make_error(ec::invalid_result, "the filter function did not "
                                             "return a slice");
}

class select_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "select";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings& opts) const override {
    auto expr = caf::get_if<std::string>(&opts, "expression");
    if (!expr)
      return caf::make_error(ec::invalid_configuration,
                             "key 'expression' is missing or not a string in "
                             "configuration for select step");
    return std::make_unique<select_step>(*expr);
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::select_step_plugin)
