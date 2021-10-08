//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concepts.hpp"

#include <cstddef>
#include <span>
#include <utility>

namespace vast {

/// A hash algorithm that supports incremental computation of a hash digest
/// in a construct-add-finish manner.
template <class HashAlgorithm>
concept incremental_hash = requires(HashAlgorithm h) {
  typename HashAlgorithm::result_type;
  h(std::declval<const void*>(), std::declval<size_t>());
  static_cast<typename HashAlgorithm::result_type>(h);
};

/// A hash algorithm that exposes a one-shot computation of a hash digest over
/// a byte sequence.
template <class HashAlgorithm>
concept one_shot_hash = requires(HashAlgorithm h) {
  // clang-format off
  typename HashAlgorithm::result_type;
  { h.make(std::declval<const void*>(), std::declval<size_t>()) }
    -> concepts::same_as<typename HashAlgorithm::result_type>;
  // clang-format on
};

} // namespace vast
