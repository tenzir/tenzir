#ifndef VAST_CONCEPT_PRINTABLE_CORE_EPSILON_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_EPSILON_HPP

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

class epsilon_printer : public printer<epsilon_printer> {
public:
  using attribute = unused_type;

  template <typename Iterator>
  bool print(Iterator&, unused_type) const {
    return true;
  }
};

namespace printers {

auto const eps = epsilon_printer{};

} // namespace printers
} // namespace vast

#endif

