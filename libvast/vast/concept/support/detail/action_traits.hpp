#ifndef VAST_CONCEPT_SUPPORT_DETAIL_ACTION_TRAITS_HPP
#define VAST_CONCEPT_SUPPORT_DETAIL_ACTION_TRAITS_HPP

#include <type_traits>

#include <caf/detail/type_traits.hpp>

#include "vast/detail/type_list.hpp"

namespace vast {
namespace detail {

template <class Action>
struct action_traits {
  using traits = caf::detail::get_callable_trait<Action>;

  using first_arg_type =
    std::remove_reference_t<tl_head_t<typename traits::arg_types>>;

  using result_type = typename traits::result_type;

  static constexpr auto arity = traits::num_args;
  static constexpr bool returns_void = std::is_void<result_type>::value;
  static constexpr bool no_args_returns_void = arity == 0 && returns_void;
  static constexpr bool one_arg_returns_void = arity == 1 && returns_void;
  static constexpr bool no_args_returns_non_void = arity == 0 && !returns_void;
  static constexpr bool one_arg_returns_non_void = arity == 1 && !returns_void;
};

} // namespace detail
} // namespace vast

#endif

