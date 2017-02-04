#ifndef VAST_CONCEPT_PRINTABLE_VAST_ATTRIBUTE_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_ATTRIBUTE_HPP

#include "vast/attribute.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string.hpp"

namespace vast {

using namespace std::string_literals;

struct attribute_printer : printer<attribute_printer> {
  using attribute = vast::attribute;

  template <typename Iterator>
  bool print(Iterator& out, vast::attribute const& attr) const {
    using namespace printers;
    auto prepend_eq = [](std::string const& x) { return '=' + x; };
    auto p = '&'_P << str << -(str ->* prepend_eq);
    return p(out, attr.key, attr.value);
  }
};

template <>
struct printer_registry<attribute> {
  using type = attribute_printer;
};

} // namespace vast

#endif
