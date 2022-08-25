//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/optional.hpp>

#include <optional>

namespace vast {

template <typename T>
std::optional<T> to_std(const T* ptr) {
  if (ptr)
    return *ptr;
  return std::nullopt;
}

} // namespace vast
