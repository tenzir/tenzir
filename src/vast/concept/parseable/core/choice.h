#ifndef VAST_CONCEPT_PARSEABLE_CORE_CHOICE_H
#define VAST_CONCEPT_PARSEABLE_CORE_CHOICE_H

#include <type_traits>

#include "vast/concept/parseable/core/parser.h"
#include "vast/util/variant.h"

namespace vast {

template <typename Lhs, typename Rhs>
class choice_parser;

template <typename>
struct is_choice_parser : std::false_type {};

template <typename Lhs, typename Rhs>
struct is_choice_parser<choice_parser<Lhs, Rhs>> : std::true_type {};

/// Attempts to parse either LHS or RHS.
template <typename Lhs, typename Rhs>
class choice_parser : public parser<choice_parser<Lhs, Rhs>>
{
  template <typename T, typename U>
  struct lazy_concat
  {
    using type = util::tl_concat_t<typename T::types, typename U::types>;
  };

  template <typename T, typename U>
  struct lazy_push_back
  {
    using type = util::tl_push_back_t<typename T::types, U>;
  };

  template <typename... Ts>
  struct lazy_type_list
  {
    using type = util::type_list<Ts...>;
  };

  template <typename T, typename U>
  using variant_type_list =
    util::tl_distinct_t<
      typename std::conditional_t<
        util::is_variant<T>{} && util::is_variant<U>{},
        lazy_concat<T, U>,
        std::conditional_t<
          util::is_variant<T>{},
          lazy_push_back<T, U>,
          std::conditional_t<
            util::is_variant<U>{},
            lazy_push_back<U, T>,
            lazy_type_list<T, U>
          >
        >
      >::type
    >;

  template <typename T, typename U>
  using make_flat_variant = util::make_variant_over<variant_type_list<T, U>>;

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
            make_flat_variant<lhs_attribute, rhs_attribute>
          >
        >
      >
    >;

  choice_parser(Lhs const& lhs, Rhs const& rhs)
    : lhs_{lhs},
      rhs_{rhs}
  {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    auto save = f;
    if (parse_left<Lhs>(f, l, a))
      return true;
    f = save;
    if (parse_right(f, l, a))
      return true;
    f = save;
    return false;
  }

private:
  template <typename Left, typename Iterator, typename Attribute>
  auto parse_left(Iterator& f, Iterator const& l, Attribute& a) const
    -> std::enable_if_t<is_choice_parser<Left>{}, bool>
  {
    return lhs_.parse(f, l, a); // recurse
  }

  template <typename Left, typename Iterator>
  auto parse_left(Iterator& f, Iterator const& l, unused_type) const
    -> std::enable_if_t<! is_choice_parser<Left>{}, bool>
  {
    return lhs_.parse(f, l, unused);
  }

  template <typename Left, typename Iterator, typename Attribute>
  auto parse_left(Iterator& f, Iterator const& l, Attribute& a) const
    -> std::enable_if_t<! is_choice_parser<Left>{}, bool>
  {
    lhs_attribute al;
    if (! lhs_.parse(f, l, al))
      return false;
    a = std::move(al);
    return true;
  }

  template <typename Iterator>
  bool parse_right(Iterator& f, Iterator const& l, unused_type) const
  {
    return rhs_.parse(f, l, unused);
  }

  template <typename Iterator, typename Attribute>
  auto parse_right(Iterator& f, Iterator const& l, Attribute& a) const
  {
    rhs_attribute ar;
    if (! rhs_.parse(f, l, ar))
      return false;
    a = std::move(ar);
    return true;
  }

  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast

#endif
