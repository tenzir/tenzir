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

class int64_synopsis final : public min_max_synopsis<int64_t> {
public:
  int64_synopsis(tenzir::type x);

  int64_synopsis(int64_t start, int64_t end);

  [[nodiscard]] synopsis_ptr clone() const override;

  [[nodiscard]] bool equals(const synopsis& other) const noexcept override;
};

} // namespace tenzir
