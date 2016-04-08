#ifndef VAST_CONCEPT_PARSEABLE_CORE_DIFFERENCE_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_DIFFERENCE_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <typename Lhs, typename Rhs>
class difference_parser : public parser<difference_parser<Lhs, Rhs>> {
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;
  using attribute = lhs_attribute;

  difference_parser(Lhs lhs, Rhs rhs)
    : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    auto save = f;
    if (!rhs_.parse(f, l, unused))
      return lhs_.parse(f, l, a); // Invoke LHS only if RHS doesn't fail.
    f = save;
    return false;
  }

private:
  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast

#endif
