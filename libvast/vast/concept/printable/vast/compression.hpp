/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_CONCEPT_PRINTABLE_VAST_COMPRESSION_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_COMPRESSION_HPP

#include "vast/compression.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"

namespace vast {

struct compression_printer : printer<compression_printer> {
  using attribute = compression;

  template <typename Iterator>
  bool print(Iterator& out, const compression& method) const {
    using namespace printers;
    switch (method) {
      case compression::null:
        return str.print(out, "null");
      case compression::lz4:
        return str.print(out, "lz4");
#ifdef VAST_HAVE_SNAPPY
      case compression::snappy:
        return str.print(out, "snappy");
#endif
    }
    return false;
  }
};

template <>
struct printer_registry<compression> {
  using type = compression_printer;
};

} // namespace vast

#endif
