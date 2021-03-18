// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
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
