//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

class epsilon_printer : public printer<epsilon_printer> {
public:
  using attribute = unused_type;

  template <class Iterator>
  bool print(Iterator&, unused_type) const {
    return true;
  }
};

namespace printers {

auto const eps = epsilon_printer{};

} // namespace printers
} // namespace vast
