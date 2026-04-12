//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/duration_synopsis.hpp"

namespace tenzir {

duration_synopsis::duration_synopsis(tenzir::type x)
  : min_max_synopsis<duration>{std::move(x), duration::max(), duration::min()} {
  // nop
}

duration_synopsis::duration_synopsis(duration start, duration end)
  : min_max_synopsis<duration>{tenzir::type{duration_type{}}, start, end} {
}

synopsis_ptr duration_synopsis::clone() const {
  return std::make_unique<duration_synopsis>(min(), max());
}

bool duration_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(duration_synopsis)) {
    return false;
  }
  auto& dref = static_cast<const duration_synopsis&>(other);
  return type() == dref.type() and min() == dref.min() and max() == dref.max();
}

} // namespace tenzir
