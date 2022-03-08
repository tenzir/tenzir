//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/hash.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/convertible/data.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/hash/default_hash.hpp"
#include "vast/hash/hash_append.hpp"
#include "vast/optional.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/array/builder_binary.h>
#include <arrow/scalar.h>
#include <fmt/format.h>

namespace vast {

hash_step::hash_step(hash_step_configuration configuration)
  : config_(std::move(configuration)) {
}

caf::error
hash_step::add(type layout, std::shared_ptr<arrow::RecordBatch> batch) {
  VAST_TRACE("hash step adds batch");
  // Get the target field if it exists.
  const auto& layout_rt = caf::get<record_type>(layout);
  auto column_offset = layout_rt.resolve_key(config_.field);
  if (!column_offset) {
    transformed_.emplace_back(layout, std::move(batch));
    return caf::none;
  }
  auto column_index = layout_rt.flat_index(*column_offset);
  // Compute the hash values.
  auto column = batch->column(detail::narrow_cast<int>(column_index));
  auto cb = arrow_table_slice_builder::column_builder::make(
    type{string_type{}}, arrow::default_memory_pool());
  for (int i = 0; i < batch->num_rows(); ++i) {
    const auto& item = column->GetScalar(i);
    auto h = default_hash{};
    hash_append(h, item.ValueOrDie()->ToString());
    if (config_.salt)
      hash_append(h, *config_.salt);
    auto digest = h.finish();
    auto x = fmt::format("{:x}", digest);
    cb->add(std::string_view{x});
  }
  auto hashes_column = cb->finish();
  auto result_batch
    = batch->AddColumn(batch->num_columns(), config_.out, hashes_column);
  if (!result_batch.ok()) {
    transformed_.emplace_back(std::move(layout), nullptr);
    return caf::none;
  }
  // Adjust layout.
  auto adjusted_layout_rt = layout_rt.transform({{
    {layout_rt.num_fields() - 1},
    record_type::insert_after({{config_.out, string_type{}}}),
  }});
  VAST_ASSERT(adjusted_layout_rt); // adding a field cannot fail.
  auto adjusted_layout = type{*adjusted_layout_rt};
  adjusted_layout.assign_metadata(layout);
  transformed_.emplace_back(std::move(adjusted_layout),
                            result_batch.ValueOrDie());
  return caf::none;
}

caf::expected<std::vector<transform_batch>> hash_step::finish() {
  VAST_DEBUG("hash step finished transformation");
  return std::exchange(transformed_, {});
}

class hash_step_plugin final : public virtual transform_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "hash";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record& opts) const override {
    auto config = to<hash_step_configuration>(opts);
    if (!config)
      return config.error(); // FIXME: Better error message?
    return std::make_unique<hash_step>(std::move(*config));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::hash_step_plugin)
