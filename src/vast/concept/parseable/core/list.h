#ifndef VAST_CONCEPT_PARSEABLE_CORE_LIST_H
#define VAST_CONCEPT_PARSEABLE_CORE_LIST_H

#include <vector>

#include "vast/concept/parseable/core/parser.h"

namespace vast {

template <typename Lhs, typename Rhs>
class list_parser : public parser<list_parser<Lhs, Rhs>>
{
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;
  using attribute = std::vector<lhs_attribute>;

  list_parser(Lhs lhs, Rhs rhs)
    : lhs_{std::move(lhs)},
      rhs_{std::move(rhs)}
  {
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    auto save = f;
    if (! lhs_.parse(f, l, unused))
    {
      f = save;
      return false;
    }
    save = f;
    while (rhs_.parse(f, l, unused) && lhs_.parse(f, l, unused))
      save = f;
    f = save;
    return true;
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    auto save = f;
    lhs_attribute elem;
    if (! lhs_.parse(f, l, elem))
    {
      f = save;
      return false;
    }
    a.push_back(std::move(elem));
    save = f;
    while (rhs_.parse(f, l, unused) && lhs_.parse(f, l, elem))
    {
      a.push_back(std::move(elem));
      save = f;
    }
    f = save;
    return true;
  }

private:
  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast

#endif
