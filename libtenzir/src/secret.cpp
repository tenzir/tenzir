//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/fbs/data.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/type.hpp"

#include <vector>

namespace tenzir {

/// Ensure consistency between the enum and its backing representation in arrow/fb
static_assert(std::same_as<typename secret_type::builder_type::arrow_enum_type,
                           ::arrow::Int32Type>);
static_assert(
  std::same_as<detail::secret_enum_underlying_type, std::int32_t>);
static_assert(
  std::same_as<detail::secret_enum_underlying_type, std::int32_t>);
using fbs_type = decltype(std::declval<fbs::data::Secret>().type());
static_assert(std::same_as<fbs_type, std::int32_t>);

namespace {

constexpr static auto strip_secret_transformation = [](
  struct record_type::field field, std::shared_ptr<arrow::Array> array) noexcept
  -> std::vector<
    std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
  const auto& et = as<secret_type>(field.type);
  auto builder = secret_type::make_arrow_builder(arrow::default_memory_pool());
  for (const auto& value :
       values(et, as<type_to_arrow_array_t<secret_type>>(*array))) {
    if (!value) {
      check(builder->AppendNull());
      continue;
    }
    check(append_builder(secret_type{}, *builder, clear(*value)));
  }
  return {{
    {field.name, std::move(field.type)},
    check(builder->Finish()),
  }};
};

} // namespace

table_slice clear_secrets(table_slice slice) {
  if (slice.rows() == 0) {
    return slice;
  }
  auto type = as<record_type>(slice.schema());
  // Strip all secret values, if there are any
  auto transformations = std::vector<indexed_transformation>{};
  for (const auto& [field, index] : type.leaves()) {
    if (!is<secret_type>(field.type)) {
      continue;
    }
    transformations.emplace_back(index, strip_secret_transformation);
  }
  return transform_columns(slice, std::move(transformations));
}

} // namespace tenzir
