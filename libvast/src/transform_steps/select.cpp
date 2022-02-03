//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/select.hpp"

#include "vast/arrow_table_slice_builder.hpp"
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

caf::error
select_step::add(type layout, std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_TRACE("select step adds batch");
  if (!expression_) {
    transformed_.clear();
    return expression_.error();
  }
  auto tailored_expr = tailor(*expression_, layout);
  if (!tailored_expr) {
    transformed_.clear();
    return tailored_expr.error();
  }
  // TODO: Replace this with an Arrow-native filter function as soon as we are
  // able to directly evaluate expressions on a record batch.
  auto new_slice
    = filter(arrow_table_slice_builder::create(batch, layout), *tailored_expr);
  if (new_slice) {
    auto as_batch = to_record_batch(*new_slice);
    transformed_.emplace_back(new_slice->layout(), std::move(as_batch));
  }
  return caf::none;
}

caf::expected<std::vector<transform_batch>> select_step::finish() {
  VAST_DEBUG("select step finished transformation");
  return std::exchange(transformed_, {});
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
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
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
