// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/filter.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/convertible/data.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/transform_steps/select.hpp"

#include <arrow/type.h>
#include <caf/expected.hpp>

namespace vast {

class filter_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "filter";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record& options) const override {
    if (!options.contains("expression"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'expression' is missing in configuration for "
                             "filter step");
    auto config = to<select_step_configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<select_step>(std::move(*config),
                                         select_step::mode::filter);
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::filter_step_plugin)
