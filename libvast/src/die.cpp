// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include <cstdlib>
#include <iostream>

#include "vast/die.hpp"

namespace vast {

[[noreturn]] void die(const std::string& msg) {
  if (!msg.empty())
    std::cerr << "\nERROR: " << msg << std::endl;
  std::abort();
}

} // namespace vast
