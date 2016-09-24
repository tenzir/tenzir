#ifndef VAST_CONCEPT_SUPPORT_DETAIL_GUARD_TRAITS_HPP
#define VAST_CONCEPT_SUPPORT_DETAIL_GUARD_TRAITS_HPP

#include <type_traits>

#include <caf/detail/type_traits.hpp>

namespace vast {
namespace detail {

template <class Guard>
struct guard_traits {
  using traits = caf::detail::get_callable_trait<Guard>;
  using result_type = typename traits::result_type;

  static constexpr auto arity = traits::num_args;
  static constexpr bool returns_bool = std::is_same<result_type, bool>::value;
  static constexpr bool no_args_returns_bool = arity == 0 && returns_bool;
  static constexpr bool one_arg_returns_bool = arity == 1 && returns_bool;
};

} // namespace detail
} // namespace vast

#endif

