#ifndef VAST_CONCEPT_PRINTABLE_CORE_ACTION_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_ACTION_HPP

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/action_traits.hpp"

namespace vast {

/// Executes a function after successfully parsing the inner attribute.
template <typename Printer, typename Action>
class action_printer : public printer<action_printer<Printer, Action>> {
public:
  using inner_attribute = typename Printer::attribute;
  using action_traits = detail::action_traits<Action>;
  using action_arg_type = typename action_traits::first_arg_type;

  using attribute =
    std::conditional_t<
      action_traits::returns_void,
      unused_type,
      typename action_traits::result_type
    >;

  action_printer(Printer p, Action fun) : printer_{std::move(p)}, action_(fun) {
  }

  // No argument, void return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto print(Iterator& out, Attribute const& attr) const
  -> std::enable_if_t<detail::action_traits<A>::no_args_returns_void, bool> {
    action_();
    return printer_.print(out, attr);
  }

  // One argument, void return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto print(Iterator& out, Attribute const& attr) const
  -> std::enable_if_t<detail::action_traits<A>::one_arg_returns_void, bool> {
    action_(attr);
    return printer_.print(out, attr);
  }

  // No argument, non-void return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto print(Iterator& out, Attribute const&) const
  -> std::enable_if_t<
    detail::action_traits<A>::no_args_returns_non_void, bool
  > {
    auto x = action_();
    return printer_.print(out, x);
  }

  // One argument, non-void return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto print(Iterator& out, Attribute const& attr) const
  -> std::enable_if_t<
    detail::action_traits<A>::one_arg_returns_non_void, bool
  > {
    auto x = action_(attr);
    return printer_.print(out, x);
  }

private:
  Printer printer_;
  Action action_;
};

} // namespace vast

#endif
