#ifndef VAST_CONCEPT_PRINTABLE_VAST_UUID_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_UUID_HPP

#include "vast/access.hpp"
#include "vast/uuid.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/detail/coding.hpp"

namespace vast {

template <>
struct access::printer<uuid> : vast::printer<access::printer<uuid>> {
  using attribute = uuid;

  template <typename Iterator>
  bool print(Iterator& out, uuid const& u) const {
    static auto byte = printers::any << printers::any;
    for (size_t i = 0; i < 16; ++i) {
      auto hi = detail::byte_to_char((u.id_[i] >> 4) & 0x0f);
      auto lo = detail::byte_to_char(u.id_[i] & 0x0f);
      if (!byte(out, hi, lo))
        return false;
      if (i == 3 || i == 5 || i == 7 || i == 9)
        if (!printers::chr<'-'>(out))
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
