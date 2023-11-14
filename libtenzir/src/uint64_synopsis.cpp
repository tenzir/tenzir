//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/uint64_synopsis.hpp"

namespace tenzir {

uint64_synopsis::uint64_synopsis(tenzir::type x)
  : min_max_synopsis<uint64_t>{std::move(x),
                               std::numeric_limits<uint64_t>::max(),
                               std::numeric_limits<uint64_t>::min()} {
  // nop
}

uint64_synopsis::uint64_synopsis(uint64_t start, uint64_t end)
  : min_max_synopsis<uint64_t>{tenzir::type{uint64_type{}}, start, end} {
}

synopsis_ptr uint64_synopsis::clone() const {
  return std::make_unique<uint64_synopsis>(min(), max());
}

bool uint64_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(uint64_synopsis))
    return false;
  auto& dref = static_cast<const uint64_synopsis&>(other);
  return type() == dref.type() && min() == dref.min() && max() == dref.max();
}

} // namespace tenzir
