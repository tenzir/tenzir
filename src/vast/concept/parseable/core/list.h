#ifndef VAST_CONCEPT_PARSEABLE_CORE_LIST_H
#define VAST_CONCEPT_PARSEABLE_CORE_LIST_H

#include <vector>

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/detail/container.h"

namespace vast {

template <typename Lhs, typename Rhs>
class list_parser : public parser<list_parser<Lhs, Rhs>> {
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;
  using container = detail::container<lhs_attribute>;
  using attribute = typename container::attribute;

  list_parser(Lhs lhs, Rhs rhs) : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    if (!container::parse(lhs_, f, l, a))
      return false;
    auto save = f;
    while (rhs_.parse(f, l, unused) && container::parse(lhs_, f, l, a))
      save = f;
    f = save;
    return true;
  }

private:
  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast

#endif
