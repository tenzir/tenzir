//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

namespace vast {

/// An open interval of IDs.
struct id_range {
  id_range(id from, id to) : first(from), last(to) {
    // nop
  }
  id_range(id id) : id_range(id, id + 1) {
    // nop
  }
  id first{0u};
  id last{0u};
};

} // namespace vast
