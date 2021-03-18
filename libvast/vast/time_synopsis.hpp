// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/min_max_synopsis.hpp"
#include "vast/synopsis.hpp"

namespace vast {

class time_synopsis final : public min_max_synopsis<time> {
public:
  time_synopsis(vast::type x);

  time_synopsis(time start, time end);

  bool equals(const synopsis& other) const noexcept override;
};

} // namespace vast
