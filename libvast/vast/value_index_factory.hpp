// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/factory.hpp"
#include "vast/fwd.hpp"

#include <caf/fwd.hpp>

namespace vast {

template <>
struct factory_traits<value_index> {
  using result_type = value_index_ptr;
  using key_type = type;
  using signature = result_type (*)(type, caf::settings);

  static void initialize();

  static key_type key(const type& x);
};

} // namespace vast
