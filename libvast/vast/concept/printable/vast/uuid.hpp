#ifndef VAST_CONCEPT_PRINTABLE_VAST_UUID_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_UUID_HPP

#include "vast/access.hpp"
#include "vast/uuid.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/detail/coding.hpp"

namespace vast {

template <>
struct access::printer<uuid> : vast::printer<access::printer<uuid>> {
  using attribute = uuid;

  template <typename Iterator>
  bool print(Iterator& out, uuid const& u) const {
    using namespace printers;
    for (size_t i = 0; i < 16; ++i) {
      auto& byte = u.id_[i];
      if (!(any.print(out, detail::byte_to_char((byte >> 4) & 0x0f))
            && any.print(out, detail::byte_to_char(byte & 0x0f))))
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
