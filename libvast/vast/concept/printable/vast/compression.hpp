#ifndef VAST_CONCEPT_PRINTABLE_VAST_COMPRESSION_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_COMPRESSION_HPP

#include "vast/compression.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"

namespace vast {

struct compression_printer : printer<compression_printer> {
  using attribute = compression;

  template <typename Iterator>
  bool print(Iterator& out, compression const& method) const {
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
