//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/double_synopsis.hpp"

namespace tenzir {

double_synopsis::double_synopsis(tenzir::type x)
  : min_max_synopsis<double>{std::move(x),
                             std::numeric_limits<double>::infinity(),
                             -std::numeric_limits<double>::infinity()} {
  // nop
}

double_synopsis::double_synopsis(double start, double end)
  : min_max_synopsis<double>{tenzir::type{double_type{}}, start, end} {
}

synopsis_ptr double_synopsis::clone() const {
  return std::make_unique<double_synopsis>(min(), max());
}

bool double_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(double_synopsis))
    return false;
  auto& dref = static_cast<const double_synopsis&>(other);
  return type() == dref.type() && min() == dref.min() && max() == dref.max();
}

} // namespace tenzir
