//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/time_synopsis.hpp"

namespace vast {

time_synopsis::time_synopsis(vast::type x)
  : min_max_synopsis<time>{std::move(x), time::max(), time::min()} {
  // nop
}

time_synopsis::time_synopsis(time start, time end)
  : min_max_synopsis<time>{time_type{}, start, end} {
}

bool time_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(time_synopsis))
    return false;
  auto& dref = static_cast<const time_synopsis&>(other);
  return type() == dref.type() && min() == dref.min() && max() == dref.max();
}

} // namespace vast
