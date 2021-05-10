//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/die.hpp"

#include <cstdlib>
#include <iostream>

namespace vast {

[[noreturn]] void die(const std::string& msg) {
  if (!msg.empty())
    std::cerr << "\nERROR: " << msg << std::endl;
  std::abort();
}

} // namespace vast
