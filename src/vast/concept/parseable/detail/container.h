#ifndef VAST_CONCEPT_PARSEABLE_DETAIL_CONTAINER_H
#define VAST_CONCEPT_PARSEABLE_DETAIL_CONTAINER_H

#include <vector>
#include <type_traits>

#include "vast/concept/parseable/detail/attr_fold.h"

namespace vast {
namespace detail {

template <typename Elem>
struct container {
  using vector_type = std::vector<Elem>;
  using attribute = typename attr_fold<vector_type>::type;

  template <typename T>
  struct lazy_value_type {
    using value_type = T;
  };

  using value_type =
    typename std::conditional_t<
      std::is_same<attribute, std::decay_t<unused_type>>{},
      lazy_value_type<unused_type>,
      attribute
    >::value_type;

  static constexpr bool modified = std::is_same<vector_type, attribute>{};

  template <typename Container, typename T>
  static void push_back(Container& c, T&& x) {
    c.insert(c.end(), std::move(x));
  }

  template <typename Container>
  static void push_back(Container&, unused_type) {
    // nop
  }

  template <typename T>
  static void push_back(unused_type, T&&) {
    // nop
  }

  template <typename Parser, typename Iterator>
  static bool parse(Parser const& p, Iterator& f, Iterator const& l,
                    unused_type) {
    return p.parse(f, l, unused);
  }

  template <typename Parser, typename Iterator, typename Attribute>
  static bool parse(Parser const& p, Iterator& f, Iterator const& l,
                    Attribute& a) {
    value_type x;
    if (!p.parse(f, l, x))
      return false;
    push_back(a, std::move(x));
    return true;
  }
};

} // namespace detail
} // namespace vast

#endif
