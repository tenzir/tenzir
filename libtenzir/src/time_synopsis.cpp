//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/time_synopsis.hpp"

namespace tenzir {

time_synopsis::time_synopsis(tenzir::type x)
  : min_max_synopsis<time>{std::move(x), time::max(), time::min()} {
  // nop
}

time_synopsis::time_synopsis(time start, time end)
  : min_max_synopsis<time>{tenzir::type{time_type{}}, start, end} {
}

synopsis_ptr time_synopsis::clone() const {
  return std::make_unique<time_synopsis>(min(), max());
}

bool time_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(time_synopsis))
    return false;
  auto& dref = static_cast<const time_synopsis&>(other);
  return type() == dref.type() && min() == dref.min() && max() == dref.max();
}

} // namespace tenzir
