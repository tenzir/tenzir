#ifndef VAST_CONCEPT_PARSEABLE_CORE_ACTION_H
#define VAST_CONCEPT_PARSEABLE_CORE_ACTION_H

#include <caf/detail/type_traits.hpp>

#include "vast/concept/parseable/core/parser.h"

namespace vast {

/// Transform a parser's inner attribute after a successful parse.
/// @tparam Parser The parser to augment with an action.
/// @tparam Action A function taking the synthesized attribute and returning
///                a new type.
template <typename Parser, typename Action>
class action_parser : public parser<action_parser<Parser, Action>>
{
public:
  using inner_attribute = typename Parser::attribute;
  using action_traits = caf::detail::get_callable_trait<Action>;
  using action_result_type = typename action_traits::result_type;
  static constexpr size_t action_arity = action_traits::num_args;
  using attribute =
    std::conditional_t<
      std::is_same<action_result_type, void>{},
      unused_type,
      action_result_type
    >;

  action_parser(Parser const& p, Action fun)
    : parser_{p},
      action_(fun)
  {
  }

  // No argument, no return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto parse(Iterator& f, Iterator const& l, Attribute&) const
    -> std::enable_if_t<
         caf::detail::get_callable_trait<A>::num_args == 0
           && std::is_same<
                typename caf::detail::get_callable_trait<A>::result_type,
                void
              >{},
         bool
       >
  {
    inner_attribute x;
    if (! parser_.parse(f, l, x))
      return false;
    action_();
    return true;
  }

  // One argument, no return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto parse(Iterator& f, Iterator const& l, Attribute&) const
    -> std::enable_if_t<
         caf::detail::get_callable_trait<A>::num_args == 1
           && std::is_same<
                typename caf::detail::get_callable_trait<A>::result_type,
                void
              >{},
         bool
       >
  {
    inner_attribute x;
    if (! parser_.parse(f, l, x))
      return false;
    action_(std::move(x));
    return true;
  }

  // No argument, with return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto parse(Iterator& f, Iterator const& l, Attribute& a) const
    -> std::enable_if_t<
         caf::detail::get_callable_trait<A>::num_args == 0
           && ! std::is_same<
                typename caf::detail::get_callable_trait<A>::result_type,
                void
              >{},
         bool
       >
  {
    inner_attribute x;
    if (! parser_.parse(f, l, x))
      return false;
    a = action_();
    return true;
  }

  // One argument, with return type.
  template <typename Iterator, typename Attribute, typename A = Action>
  auto parse(Iterator& f, Iterator const& l, Attribute& a) const
    -> std::enable_if_t<
         caf::detail::get_callable_trait<A>::num_args == 1
           && ! std::is_same<
                typename caf::detail::get_callable_trait<A>::result_type,
                void
              >{},
         bool
       >
  {
    inner_attribute x;
    if (! parser_.parse(f, l, x))
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
