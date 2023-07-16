//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <optional>

namespace tenzir {

template <typename T>
std::optional<T> to_optional(const T* ptr) {
  if (ptr)
    return *ptr;
  return std::nullopt;
}

} // namespace tenzir
