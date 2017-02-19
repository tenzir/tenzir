#ifndef VAST_CONCEPT_PARSEABLE_DETAIL_CONTAINER_HPP
#define VAST_CONCEPT_PARSEABLE_DETAIL_CONTAINER_HPP

#include <vector>
#include <type_traits>

#include "vast/concept/support/detail/attr_fold.hpp"

namespace vast {
namespace detail {

template <typename T>
struct is_pair : std::false_type {};

template <typename T, typename U>
struct is_pair<std::pair<T, U>> : std::true_type {};

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

  static constexpr bool modified = std::is_same<vector_type, attribute>::value;

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
    return p(f, l, unused);
  }

  template <typename Parser, typename Iterator, typename Attribute>
  static auto parse(Parser const& p, Iterator& f, Iterator const& l,
                    Attribute& a)
    -> std::enable_if_t<!is_pair<typename Attribute::value_type>{}, bool> {
    value_type x;
    if (!p(f, l, x))
      return false;
    push_back(a, std::move(x));
    return true;
  }

  template <typename Parser, typename Iterator, typename Attribute>
  static auto parse(Parser const& p, Iterator& f, Iterator const& l,
                    Attribute& a)
    -> std::enable_if_t<is_pair<typename Attribute::value_type>{}, bool> {
    using pair_type =
      std::pair<
        std::remove_const_t<typename Attribute::value_type::first_type>,
        typename Attribute::value_type::second_type
      >;
    pair_type pair;
    if (!p(f, l, pair))
      return false;
    push_back(a, std::move(pair));
    return true;
  }
};

} // namespace detail
} // namespace vast

#endif
