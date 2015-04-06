#ifndef VAST_CONCEPT_PARSEABLE_CORE_OR_H
#define VAST_CONCEPT_PARSEABLE_CORE_OR_H

#include <type_traits>

#include "vast/concept/parseable/core/parser.h"
#include "vast/util/variant.h"

namespace vast {

// TODO: implement this parser properly for more than two types, right now it's
// not yet fully composable.
template <typename Lhs, typename Rhs>
class or_parser : public parser<or_parser<Lhs, Rhs>>
{
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;

  // LHS = unused && RHS = unused  =>  unused
  // LHS = T && RHS = unused       =>  LHS
  // LHS = unused && RHS = T       =>  RHS
  // LHS = T && RHS = T            =>  T
  // LHS = T && RHS = U            =>  variant<T, U>
  using attribute =
    std::conditional_t<
      std::is_same<lhs_attribute, unused_type>{}
        && std::is_same<rhs_attribute, unused_type>{},
      unused_type,
      std::conditional_t<
        std::is_same<lhs_attribute, unused_type>{},
        rhs_attribute,
        std::conditional_t<
          std::is_same<rhs_attribute, unused_type>{},
          lhs_attribute,
          std::conditional_t<
            std::is_same<lhs_attribute, rhs_attribute>{},
            lhs_attribute,
            util::variant<lhs_attribute, rhs_attribute>
          >
        >
      >
    >;

  or_parser(Lhs const& lhs, Rhs const& rhs)
    : lhs_{lhs},
      rhs_{rhs}
  {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    auto save = f;
    lhs_attribute al;
    if (lhs_.parse(f, l, al))
    {
      a = std::move(al);
      return true;
    }
    f = save;
    rhs_attribute ar;
    if (rhs_.parse(f, l, ar))
    {
      a = std::move(ar);
      return true;
    }
    f = save;
    return false;
  }

private:
  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast

#endif
