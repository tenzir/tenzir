//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/hash/hash.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <cstdint>

namespace vast::detail {

// We use "0" as "type-erased" hash digest for NULL. Unlike in Arrow, a NULL
// value is not typed in VAST.
constexpr auto nil_hash_digest = 0;

// @relates hash_array
template <basic_type Type, hash_algorithm HashAlgorithm = default_hash>
uint64_t hash_scalar(view<type_to_data_t<type>> x) {
  return seeded_hash<HashAlgorithm>{Type::type_index}(x);
}

} // namespace vast::detail
