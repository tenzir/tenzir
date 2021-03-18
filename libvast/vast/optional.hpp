// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/optional.hpp>

#include <optional>

namespace vast {

/// A drop-in replacement for C++17's std::optional.
using caf::optional;

template <typename T>
std::optional<T> to_std(caf::optional<T>&& opt) {
  std::optional<T> result;
  if (opt)
    result = std::move(*opt);
  return result;
}

} // namespace vast

