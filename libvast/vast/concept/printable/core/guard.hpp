#ifndef VAST_CONCEPT_PRINTABLE_CORE_GUARD_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_GUARD_HPP

#include "vast/concept/support/detail/guard_traits.hpp"

namespace vast {

/// Attaches a guard expression to a printer that must succeed before the
/// printer executes.
/// @tparam Printer The printer to augment with a guard expression.
/// @tparam Guard A function that either takes no arguments or the attribute by
///               const-reference and returns `bool`.
template <typename Printer, typename Guard>
class guard_printer : public printer<guard_printer<Printer, Guard>> {
public:
  using attribute = typename Printer::attribute;

  guard_printer(Printer p, Guard fun) : printer_{std::move(p)}, guard_(fun) {
  }

  template <typename Iterator>
  bool print(Iterator& out, unused_type) const {
    return guard_() && printer_.print(out, unused);
  }

  template <typename Iterator, typename Attribute, typename G = Guard>
  auto print(Iterator& out, Attribute const& a) const
  -> std::enable_if_t<detail::guard_traits<G>::no_args_returns_bool, bool> {
    return guard_() && printer_.print(out, a);
  }

  template <typename Iterator, typename Attribute, typename G = Guard>
  auto print(Iterator& out, Attribute const& a) const
  -> std::enable_if_t<detail::guard_traits<G>::one_arg_returns_bool, bool> {
    return guard_(a) && printer_.print(out, a);
  }

private:
  Printer printer_;
  Guard guard_;
};

} // namespace vast

#endif

