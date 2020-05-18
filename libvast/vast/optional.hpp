/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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

