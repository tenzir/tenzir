//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/error.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <cstddef>
#include <span>

namespace vast::detail {

template <class T, size_t Extent = std::dynamic_extent>
caf::expected<T> legacy_deserialize(std::span<const std::byte, Extent> bytes) {
  return caf::make_error(ec::unimplemented); //, Ts &&xs...)
}

} // namespace vast::detail
