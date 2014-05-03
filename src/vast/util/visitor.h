#ifndef VAST_UTIL_VISITOR_H
#define VAST_UTIL_VISITOR_H

namespace vast {
namespace util {

//
// Visitors
//

/// The base class for mutable visitors.
template <typename... Visitables>
struct visitor;

template <typename First, typename... Visitables>
struct visitor<First, Visitables...> : visitor<Visitables...>
{
  using visitor<Visitables...>::visit;
  virtual void visit(First&) = 0;
};

template <typename First>
struct visitor<First>
{
  virtual void visit(First&) = 0;
};


/// The base class for constant visitors.
template <typename... Visitables>
struct const_visitor;

template <typename First, typename... Visitables>
struct const_visitor<First, Visitables...> : const_visitor<Visitables...>
{
  using const_visitor<Visitables...>::visit;
  virtual void visit(First const&) = 0;
};

template <typename First>
struct const_visitor<First>
{
  virtual void visit(First const&) = 0;
};


/// The mixin for base classes of a visitable hierarchy.
/// @tparam Visitor The concrete visitor to support.
template <typename... Visitors>
struct visitable_with;

template <typename First, typename... Visitors>
struct visitable_with<First, Visitors...> : visitable_with<Visitors...>
{
  using visitable_with<Visitors...>::accept;
  virtual void accept(First& v) = 0;
  virtual void accept(First& v) const = 0;
};

template <typename Last>
struct visitable_with<Last>
{
  virtual void accept(Last& v) = 0;
  virtual void accept(Last& v) const = 0;
};


/// The mixin for child classes of a visitable hierarchy.
/// @tparam Base The common base class of this class.
/// @tparam Derived This class.
/// @tparam Visitor The concrete visitor to support.
template <typename Base, typename Derived, typename... Visitors>
struct visitable;

template <typename Base, typename Derived, typename First, typename... Visitors>
struct visitable<Base, Derived, First, Visitors...>
  : visitable<Base, Derived, Visitors...>
{
  using visitable<Base, Derived, Visitors...>::accept;

  virtual void accept(First& v)
  {
    v.visit(static_cast<Derived&>(*this));
  }
  
  virtual void accept(First& v) const
  {
    v.visit(static_cast<Derived const&>(*this));
  }
};

template <typename Base, typename Derived, typename Last>
struct visitable<Base, Derived, Last> : public Base
{
  virtual void accept(Last& v)
  {
    v.visit(static_cast<Derived&>(*this));
  }
  
  virtual void accept(Last& v) const
  {
    v.visit(static_cast<Derived const&>(*this));
  }
};


/// A mixin for abstract child classes of a visitable hierarchy.
/// @tparam Base The common base class of this class.
/// @tparam Visitors The concrete visitors to support.
template <typename Base, typename... Visitors>
struct abstract_visitable;

template <typename Base, typename First, typename... Visitors>
struct abstract_visitable<Base, First, Visitors...>
  : abstract_visitable<Base, Visitors...>
{
  using abstract_visitable<Base, Visitors...>::accept;
  virtual void accept(First& v) = 0;
  virtual void accept(First& v) const = 0;
};

template <typename Base, typename Last>
struct abstract_visitable<Base, Last> : public Base
{
  virtual void accept(Last& v) = 0;
  virtual void accept(Last& v) const = 0;
};

} // namespace util
} // namespace vast

#endif
