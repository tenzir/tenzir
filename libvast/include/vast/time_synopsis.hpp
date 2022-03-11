//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/min_max_synopsis.hpp"
#include "vast/synopsis.hpp"

namespace vast {

class time_synopsis final : public min_max_synopsis<time> {
public:
  time_synopsis(vast::type x);

  time_synopsis(time start, time end);

  [[nodiscard]] synopsis_ptr clone() const override;

  [[nodiscard]] bool equals(const synopsis& other) const noexcept override;
};

} // namespace vast
