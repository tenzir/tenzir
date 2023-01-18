//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>

namespace vast::plugins::identity {

namespace {

// Does nothing with the input.
class identity_operator : public pipeline_operator {
public:
  identity_operator() noexcept = default;

  caf::error
  add(type schema, std::shared_ptr<arrow::RecordBatch> batch) override {
    VAST_TRACE("identity operator adds batch");
    transformed_.emplace_back(schema, std::move(batch));
    return caf::none;
  }

  caf::expected<std::vector<pipeline_batch>> finish() override {
    VAST_DEBUG("identity operator finished transformation");
    return std::exchange(transformed_, {});
  }

private:
  /// The slices being transformed.
  std::vector<pipeline_batch> transformed_ = {};
};

class plugin final : public virtual pipeline_operator_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "identity";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const record&) const override {
    return std::make_unique<identity_operator>();
  }

  [[nodiscard]] std::pair<std::string_view,
                          caf::expected<std::unique_ptr<pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
    // '... | identity | ...'
    // '... | identity'
    //                ^ we start here
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    using parsers::space, parsers::eoi;
    const auto optional_ws = ignore(*space);
    const auto p = optional_ws >> ('|' | eoi);
    if (!p(f, l, unused)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "identity operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<identity_operator>(),
    };
  }
};

} // namespace

} // namespace vast::plugins::identity

VAST_REGISTER_PLUGIN(vast::plugins::identity::plugin)
