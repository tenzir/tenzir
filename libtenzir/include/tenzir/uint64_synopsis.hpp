//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/min_max_synopsis.hpp"
#include "tenzir/synopsis.hpp"

namespace tenzir {

class uint64_synopsis final : public min_max_synopsis<uint64_t> {
public:
  uint64_synopsis(tenzir::type x);

  uint64_synopsis(uint64_t start, uint64_t end);

  [[nodiscard]] synopsis_ptr clone() const override;

  [[nodiscard]] bool equals(const synopsis& other) const noexcept override;
};

} // namespace tenzir
