//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/transform_steps/hash.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/hashable/default_hash.hpp"
#include "vast/concept/hashable/hash_append.hpp"
#include "vast/error.hpp"
#include "vast/optional.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"

#include <arrow/array/builder_binary.h>
#include <arrow/scalar.h>
#include <fmt/format.h>

namespace vast {

hash_step::hash_step(const std::string& fieldname, const std::string& out,
                     const std::optional<std::string>& salt)
  : field_(fieldname), out_(out), salt_(salt) {
}

caf::expected<table_slice> hash_step::operator()(table_slice&& slice) const {
  auto layout = slice.layout();
  auto offset = layout.resolve(field_);
  if (!offset)
    return std::move(slice);
  // We just got the offset from `layout`, so we can safely dereference.
  auto column_index = *layout.flat_index_at(*offset);
  layout.fields.emplace_back(out_, legacy_string_type{});
  auto builder_ptr
    = factory<table_slice_builder>::make(slice.encoding(), layout);
  auto builder_error
    = caf::make_error(ec::unspecified, "pseudonymize step: unknown error "
                                       "in table slice builder");
  for (size_t i = 0; i < slice.rows(); ++i) {
    vast::data out_hash;
    for (size_t j = 0; j < slice.columns(); ++j) {
      const auto& item = slice.at(i, j);
      if (j == column_index) {
        auto h = default_hash{};
        hash_append(h, item);
        if (salt_)
          hash_append(h, *salt_);
        auto digest = static_cast<default_hash::result_type>(h);
        out_hash = fmt::format("{:x}", digest);
      }
      if (!builder_ptr->add(item))
        return builder_error;
    }
    if (!builder_ptr->add(out_hash))
      return builder_error;
  }
  return builder_ptr->finish();
}

[[nodiscard]] std::pair<vast::legacy_record_type,
                        std::shared_ptr<arrow::RecordBatch>>
hash_step::operator()(vast::legacy_record_type layout,
                      std::shared_ptr<arrow::RecordBatch> batch) const {
  // Get the target field if it exists.
  auto offset = layout.resolve(field_);
  if (!offset)
    return std::make_pair(std::move(layout), std::move(batch));
  auto flat_index = layout.flat_index_at(*offset);
  VAST_ASSERT(flat_index); // We just got this from `layout`.
  auto column_index = static_cast<int>(*flat_index);
  // Compute the hash values.
  auto column = batch->column(column_index);
  auto cb = arrow_table_slice_builder::column_builder::make(
    legacy_string_type{}, arrow::default_memory_pool());
  for (int i = 0; i < batch->num_rows(); ++i) {
    const auto& item = column->GetScalar(i);
    auto h = default_hash{};
    hash_append(h, item.ValueOrDie()->ToString());
    if (salt_)
      hash_append(h, *salt_);
    auto digest = static_cast<default_hash::result_type>(h);
    auto x = fmt::format("{:x}", digest);
    cb->add(std::string_view{x});
  }
  auto hashes_column = cb->finish();
  auto result_batch
    = batch->AddColumn(batch->num_columns(), out_, hashes_column);
  if (!result_batch.ok())
    return std::make_pair(std::move(layout), nullptr);
  // Adjust layout.
  layout.fields.emplace_back(out_, legacy_string_type{});
  return std::make_pair(std::move(layout), result_batch.ValueOrDie());
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
  [[nodiscard]] caf::expected<transform_step_ptr>
  make_transform_step(const caf::settings& opts) const override {
    auto field = caf::get_if<std::string>(&opts, "field");
    if (!field)
      return caf::make_error(ec::invalid_configuration,
                             "key 'field' is missing or not a string in "
                             "configuration for delete step");
    auto out = caf::get_if<std::string>(&opts, "out");
    if (!out)
      return caf::make_error(ec::invalid_configuration,
                             "key 'out' is missing or not a string in "
                             "configuration for delete step");
    auto salt = caf::get_if<std::string>(&opts, "salt");
    return std::make_unique<hash_step>(*field, *out, to_std(std::move(salt)));
  }
};

} // namespace vast

VAST_REGISTER_PLUGIN(vast::hash_step_plugin)
