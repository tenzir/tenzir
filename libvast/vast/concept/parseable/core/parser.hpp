#ifndef VAST_CONCEPT_PARSEABLE_CORE_PARSER_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_PARSER_HPP

#include <type_traits>
#include <iterator>
#include <tuple>

#include "vast/concept/support/unused_type.hpp"

#include "vast/detail/type_traits.hpp"

namespace vast {

template <typename, typename>
class action_parser;

template <typename, typename>
class guard_parser;

template <typename Derived>
struct parser {
  template <typename Action>
  auto then(Action fun) const {
    return action_parser<Derived, Action>{derived(), fun};
  }

  template <typename Action>
  auto operator->*(Action fun) const {
    return then(fun);
  }

  template <typename Guard>
  auto with(Guard fun) const {
    return guard_parser<Derived, Guard>{derived(), fun};
  }

  // FIXME: don't ignore ADL.
  template <typename Range, typename Attribute = unused_type>
  auto operator()(Range&& r, Attribute& a = unused) const
  -> decltype(std::begin(r), std::end(r), bool()) {
    auto f = std::begin(r);
    auto l = std::end(r);
    return derived().parse(f, l, a);
  }

  // FIXME: don't ignore ADL.
  template <typename Range, typename A0, typename A1, typename... As>
  auto operator()(Range&& r, A0& a0, A1& a1, As&... as) const
  -> decltype(std::begin(r), std::end(r), bool()) {
    auto t = std::tie(a0, a1, as...);
    return operator()(r, t);
  }

  template <typename Iterator, typename Attribute>
  auto operator()(Iterator& f, Iterator const& l, Attribute& a) const
  -> decltype(*f, ++f, f == l, bool()) {
    return derived().parse(f, l, a);
  }

  template <typename Iterator, typename A0, typename A1, typename... As>
  auto operator()(Iterator& f, Iterator const& l, A0& a0, A1& a1,
                  As&... as) const
  -> decltype(*f, ++f, f == l, bool()) {
    auto t = std::tie(a0, a1, as...);
    return derived().parse(f, l, t);
  }

private:
  Derived const& derived() const {
    return static_cast<Derived const&>(*this);
  }
};

/// Associates a parser for a given type. To register a parser with a type, one
/// needs to specialize this struct and expose a member `type` with the
/// concrete parser type.
/// @tparam T the type to register a parser with.
template <typename T, typename = void>
struct parser_registry;

/// Retrieves a registered parser.
template <typename T>
using make_parser = typename parser_registry<T>::type;

namespace detail {

struct has_parser {
  template <typename T>
  static auto test(T*) -> std::is_class<typename parser_registry<T>::type>;

  template <typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Checks whether the parser registry has a given type registered.
template <typename T>
struct has_parser : decltype(detail::has_parser::test<T>(0)) {};

/// Checks whether a given type is-a parser, i.e., derived from ::vast::parser.
template <typename T>
using is_parser = std::is_base_of<parser<T>, T>;

} // namespace vast

#endif
