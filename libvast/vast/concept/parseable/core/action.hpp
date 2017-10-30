#ifndef VAST_CONCEPT_PARSEABLE_CORE_ACTION_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_ACTION_HPP

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/support/detail/action_traits.hpp"

namespace vast {

/// Executes a function after successfully parsing the inner attribute.
template <typename Parser, typename Action>
class action_parser : public parser<action_parser<Parser, Action>> {
public:
  using inner_attribute = typename Parser::attribute;
  using action_traits = detail::action_traits<Action>;
  using action_arg_type = typename action_traits::first_arg_type;

  using attribute =
    std::conditional_t<
      action_traits::returns_void,
      inner_attribute,
      typename action_traits::result_type
    >;

  action_parser(Parser p, Action fun) : parser_{std::move(p)}, action_(fun) {
  }

  // No argument, void return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto parse(Iterator& f, Iterator const& l, Attribute& a) const
  -> std::enable_if_t<detail::action_traits<A>::no_args_returns_void, bool> {
    if (!parser_(f, l, a))
      return false;
    action_();
    return true;
  }

  // One argument, void return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto parse(Iterator& f, Iterator const& l, Attribute& a) const
  -> std::enable_if_t<detail::action_traits<A>::one_arg_returns_void, bool> {
    if (!parser_(f, l, a))
      return false;
    action_(a);
    return true;
  }

  // No argument, non-void return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto parse(Iterator& f, Iterator const& l, Attribute& a) const
  -> std::enable_if_t<
    detail::action_traits<A>::no_args_returns_non_void, bool
  > {
    inner_attribute x;
    if (!parser_(f, l, x))
      return false;
    a = action_();
    return true;
  }

  // One argument, non-void return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto parse(Iterator& f, Iterator const& l, Attribute& a) const
  -> std::enable_if_t<
    detail::action_traits<A>::one_arg_returns_non_void, bool
  > {
    action_arg_type x;
    if (!parser_(f, l, x))
      return false;
    a = action_(std::move(x));
    return true;
  }

private:
  Parser parser_;
  Action action_;
};

} // namespace vast

#endif
