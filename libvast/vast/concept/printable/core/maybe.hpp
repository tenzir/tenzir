#ifndef VAST_CONCEPT_PRINTABLE_CORE_MAYBE_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_MAYBE_HPP

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

/// Like ::optional_printer, but exposes `T` instead of `optional<T>` as
/// attribute.
template <typename Printer>
class maybe_printer : public printer<maybe_printer<Printer>> {
public:
  using attribute = typename Printer::attribute;

  explicit maybe_printer(Printer p)
    : printer_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool print(Iterator& out, Attribute const& a) const {
    printer_.print(out, a);
    return true;
  }

private:
  Printer printer_;
};

} // namespace vast

#endif

