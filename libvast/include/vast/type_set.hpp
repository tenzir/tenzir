//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/stable_set.hpp"
#include "vast/type.hpp"

namespace vast {

struct type_set : detail::stable_set<type> {
  using super = detail::stable_set<type>;
  using super::super;

  template <class Inspector>
  friend auto inspect(Inspector& f, type_set& x) {
    return f.object(x)
      .pretty_name("vast.type_set")
      .fields(f.field("value", static_cast<super&>(x)));
  }
};

} // namespace vast
