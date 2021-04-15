//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/factory.hpp"
#include "vast/value_index.hpp"

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
