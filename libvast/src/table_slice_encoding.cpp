//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/table_slice_encoding.hpp"

#include "vast/die.hpp"

namespace vast {

std::string to_string(table_slice_encoding encoding) noexcept {
  switch (encoding) {
    case table_slice_encoding::none:
      return "none";
    case table_slice_encoding::arrow:
      return "arrow";
    case table_slice_encoding::msgpack:
      return "msgpack";
  }
  // Make gcc happy, this code is actually unreachable.
  die("unhandled table slice encoding");
}

} // namespace vast
