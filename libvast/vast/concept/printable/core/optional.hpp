#ifndef VAST_CONCEPT_PRINTABLE_CORE_OPTIONAL_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_OPTIONAL_HPP

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"
#include "vast/optional.hpp"

namespace vast {

template <typename Printer>
class optional_printer : public printer<optional_printer<Printer>> {
public:
  using inner_attribute =
    typename detail::attr_fold<typename Printer::attribute>::type;

  using attribute =
    std::conditional_t<
      std::is_same<inner_attribute, unused_type>{},
      unused_type,
      optional<inner_attribute>
    >;

  explicit optional_printer(Printer p)
    : printer_{std::move(p)} {
  }

  template <typename Iterator>
  bool print(Iterator& out, unused_type) const {
    printer_.print(out, unused);
    return true;
  }

  template <typename Iterator, typename Attribute>
  bool print(Iterator& out, Attribute const& a) const {
    return !a || printer_.print(out, *a);
  }

private:
  Printer printer_;
};

} // namespace vast

#endif

