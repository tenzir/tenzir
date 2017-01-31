#ifndef VAST_CONCEPT_PRINTABLE_CORE_IGNORE_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_IGNORE_HPP

#include <type_traits>

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

/// Wraps a printer and ignores its attribute.
template <class Printer>
class ignore_printer : public printer<ignore_printer<Printer>> {
public:
  using attribute = unused_type;

  explicit ignore_printer(Printer p) : printer_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute&) const {
    return printer_.print(out, unused);
  }

private:
  Printer printer_;
};

template <class Printer>
auto ignore(Printer&& p)
-> std::enable_if_t<
     is_printer<std::decay_t<Printer>>::value,
     ignore_printer<std::decay_t<Printer>>
   > {
  return ignore_printer<std::decay_t<Printer>>{std::forward<Printer>(p)};
}

} // namespace vast

#endif
