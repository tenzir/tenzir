//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/plugin.hpp>
#include <vast/transform_step.hpp>
#include <vast/type.hpp>

#include <arrow/table.h>

namespace vast::plugins::rename {

/// The configuration of the rename transform step.
struct configuration {
  struct name_mapping {
    std::string from = {};
    std::string to = {};

    template <class Inspector>
    friend auto inspect(Inspector& f, name_mapping& x) {
      return f(x.from, x.to);
    }

    static inline const record_type& layout() noexcept {
      static auto result = record_type{
        {"from", string_type{}},
        {"to", string_type{}},
      };
      return result;
    }
  };

  std::vector<name_mapping> layout_names = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.layout_names);
  }

  static inline const record_type& layout() noexcept {
    // layout-names:
    //   - from: zeek.conn
    //     to: zeek.aggregated_conn
    //   - from: suricata.flow
    //     to: suricata.aggregated_flow
    static auto result = record_type{
      {
        "layout-names",
        list_type{name_mapping::layout()},
      },
    };
    return result;
  }
};

class rename_step : public transform_step {
public:
  rename_step(configuration config) : config_{std::move(config)} {
    // nop
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    auto name_mapping
      = std::find_if(config_.layout_names.begin(), config_.layout_names.end(),
                     [&](const auto& name_mapping) noexcept {
                       return name_mapping.from == layout.name();
                     });
    if (name_mapping == config_.layout_names.end()) {
      transformed_batches_.emplace_back(std::move(layout), std::move(batch));
      return caf::none;
    }
    auto rename_layout = [&](const concrete_type auto& pruned_layout) {
      VAST_ASSERT(!layout.has_attributes());
      return type{name_mapping->to, pruned_layout};
    };
    layout = caf::visit(rename_layout, layout);
    auto schema = make_arrow_schema(layout);
    batch
      = arrow::RecordBatch::Make(schema, batch->num_rows(), batch->columns());
    transformed_batches_.emplace_back(std::move(layout), std::move(batch));
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override {
    return std::exchange(transformed_batches_, {});
  }

private:
  /// Cache for transformed batches.
  std::vector<transform_batch> transformed_batches_ = {};

  /// Step-specific configuration, including the layout name mapping.
  configuration config_ = {};
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual transform_plugin {
public:
  caf::error initialize(data options) override {
    // We don't use any plugin-specific configuration under
    // vast.plugins.rename, so nothing is needed here.
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    if (const auto* rec = caf::get_if<record>(&options))
      if (rec->empty())
        return caf::none;
    return caf::make_error(ec::invalid_configuration, "expected empty "
                                                      "configuration under "
                                                      "vast.plugins.rename");
  }

  /// The name is how the transform step is addressed in a transform definition.
  [[nodiscard]] const char* name() const override {
    return "rename";
  };

  /// This is called once for every time this transform step appears in a
  /// transform definition. The configuration for the step is opaquely
  /// passed as the first argument.
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const caf::settings& options) const override {
    auto rec = to<record>(options);
    if (!rec)
      return rec.error();
    auto config = to<configuration>(*rec);
    if (!config)
      return config.error();
    return std::make_unique<rename_step>(std::move(*config));
  }
};

} // namespace vast::plugins::rename

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::rename::plugin)
