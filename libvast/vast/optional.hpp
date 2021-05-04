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
std::optional<T> to_std(caf::optional<T>&& opt) {
  std::optional<T> result;
  if (opt)
    result = std::move(*opt);
  return result;
}

} // namespace vast

