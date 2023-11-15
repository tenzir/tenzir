//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/int64_synopsis.hpp"

namespace tenzir {

int64_synopsis::int64_synopsis(tenzir::type x)
  : min_max_synopsis<int64_t>{std::move(x), std::numeric_limits<int64_t>::max(),
                              std::numeric_limits<int64_t>::min()} {
  // nop
}

int64_synopsis::int64_synopsis(int64_t start, int64_t end)
  : min_max_synopsis<int64_t>{tenzir::type{int64_type{}}, start, end} {
}

synopsis_ptr int64_synopsis::clone() const {
  return std::make_unique<int64_synopsis>(min(), max());
}

bool int64_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(int64_synopsis))
    return false;
  auto& dref = static_cast<const int64_synopsis&>(other);
  return type() == dref.type() && min() == dref.min() && max() == dref.max();
}

} // namespace tenzir
