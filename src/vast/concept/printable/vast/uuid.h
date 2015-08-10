#ifndef VAST_CONCEPT_PRINTABLE_VAST_UUID_H
#define VAST_CONCEPT_PRINTABLE_VAST_UUID_H

#include "vast/access.h"
#include "vast/uuid.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/string/any.h"
#include "vast/util/coding.h"

namespace vast {

template <>
struct access::printer<uuid> : vast::printer<access::printer<uuid>> {
  using attribute = uuid;

  template <typename Iterator>
  bool print(Iterator& out, uuid const& u) const {
    using namespace printers;
    for (size_t i = 0; i < 16; ++i) {
      auto& byte = u.id_[i];
      if (!(any.print(out, util::byte_to_char((byte >> 4) & 0x0f))
            && any.print(out, util::byte_to_char(byte & 0x0f))))
        return false;
      if (i == 3 || i == 5 || i == 7 || i == 9)
        if (!any.print(out, '-'))
          return false;
    }
    return true;
  }
};

template <>
struct printer_registry<uuid> {
  using type = access::printer<uuid>;
};

} // namespace vast

#endif
