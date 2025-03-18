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
#include "tenzir/secret_store.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/type.hpp"

#include <vector>

namespace tenzir {

namespace detail {

/// Ensure consistency between the enum and its backing representation in arrow/fb
static_assert(std::same_as<typename secret_type::builder_type::arrow_enum_type,
                           ::arrow::Int32Type>);
static_assert(std::same_as<detail::secret_enum_underlying_type, std::int32_t>);
static_assert(
  std::same_as<std::underlying_type_t<secret_source_type>, std::int32_t>);
static_assert(
  std::same_as<std::underlying_type_t<secret_encoding>, std::int32_t>);

using fbs_source_type
  = decltype(std::declval<fbs::data::Secret>().source_type());
using fbs_encoding = decltype(std::declval<fbs::data::Secret>().encoding());
static_assert(std::same_as<fbs_source_type, std::int32_t>);
static_assert(std::same_as<fbs_encoding, std::int32_t>);

template class secret_common<std::string>;
template class secret_common<std::string_view>;

} // namespace detail

} // namespace tenzir
