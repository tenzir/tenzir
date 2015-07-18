#ifndef VAST_CONCEPT_PRINTABLE_VAST_FILESYSTEM_H
#define VAST_CONCEPT_PRINTABLE_VAST_FILESYSTEM_H

#include "vast/filesystem.h"
#include "vast/concept/printable/core/printer.h"

namespace vast {

struct path_printer : printer<path_printer>
{
  using attribute = path;

  template <typename Iterator>
  bool print(Iterator& out, path const& p) const
  {
    return printers::str.print(out, p.str());
  }
};

template <>
struct printer_registry<path>
{
  using type = path_printer;
};

} // namespace vast

#endif
