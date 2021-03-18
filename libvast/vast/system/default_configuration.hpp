// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"
#include "vast/system/configuration.hpp"

namespace vast::system {

struct default_configuration : system::configuration {
  default_configuration();
};

} // namespace vast::system
