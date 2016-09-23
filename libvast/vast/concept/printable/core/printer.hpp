#ifndef VAST_CONCEPT_PRINTABLE_CORE_PRINTER_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_PRINTER_HPP

#include <type_traits>
#include <iterator>

#include "vast/concept/support/unused_type.hpp"

namespace vast {

template <typename, typename>
class action_printer;

template <typename Derived>
struct printer {
  template <typename Action>
  auto before(Action fun) const {
    return action_printer<Derived, Action>{derived(), fun};
  }

  template <typename Action>
  auto operator->*(Action fun) const {
    return before(fun);
  }

  template <typename Range, typename Attribute = unused_type>
  auto operator()(Range&& r, Attribute const& a = unused) const
    -> decltype(std::back_inserter(r), bool()) {
    auto out = std::back_inserter(r);
    return derived().print(out, a);
  }

private:
  Derived const& derived() const {
    return static_cast<Derived const&>(*this);
  }
};

/// Associates a printer for a given type. To register a printer with a type,
/// one needs to specialize this struct and expose a member `type` with the
/// concrete printer type.
/// @tparam T the type to register a printer with.
template <typename T, typename = void>
struct printer_registry;

/// Retrieves a registered printer.
template <typename T>
using make_printer = typename printer_registry<T>::type;

namespace detail {

struct has_printer {
  template <typename T>
  static auto test(T* x)
    -> decltype(typename printer_registry<T>::type(), std::true_type());

  template <typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Checks whether the printer registry has a given type registered.
template <typename T>
struct has_printer : decltype(detail::has_printer::test<T>(0)) {};

/// Checks whether a given type is-a printer, i.e., derived from
/// ::vast::printer.
template <typename T>
using is_printer = std::is_base_of<printer<T>, T>;

} // namespace vast

#endif
