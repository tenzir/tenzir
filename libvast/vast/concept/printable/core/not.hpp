#ifndef VAST_CONCEPT_PRINTABLE_CORE_NOT_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_NOT_HPP

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

template <typename Printer>
class not_printer : public printer<not_printer<Printer>> {
public:
  using attribute = unused_type;

  explicit not_printer(Printer p) : printer_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool print(Iterator& out, Attribute const&) const {
    return !printer_.print(out, unused);
  }

private:
  Printer printer_;
};

} // namespace vast

#endif

