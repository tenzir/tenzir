#ifndef VAST_UTIL_VISITOR_H
#define VAST_UTIL_VISITOR_H

namespace vast {
namespace util {

//
// Visitors
//

template <typename... Visitables>
struct visitor;

template <typename First, typename... Visitables>
struct visitor<First, Visitables...> : visitor<Visitables...>
{
  using visitor<Visitables...>::visit;
  virtual void visit(First const&) = 0;
};

template <typename First>
struct visitor<First>
{
  virtual void visit(First&) = 0;
};


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

//
// Helper macros
//

#define VAST_ACCEPT_DECLARE(visitor)              \
  virtual void accept(visitor& v);

#define VAST_ACCEPT_CONST_DECLARE(visitor)        \
  virtual void accept(visitor& v) const;
    
#define VAST_ACCEPT_CONST_DEFINE(klass, visitor)  \
  void klass::accept(visitor& v) const            \
  {                                               \
    v.visit(*this);                               \
  }
    
#define VAST_ACCEPT__DEFINE(klass, visitor)       \
  void klass::accept(visitor& v)                  \
  {                                               \
    v.visit(*this);                               \
  }
    
#define VAST_ACCEPT_CONST(visitor)        \
  virtual void accept(visitor& v) const   \
  {                                       \
    v.visit(*this);                       \
  }
    
#define VAST_ACCEPT(visitor)        \
  virtual void accept(visitor& v)   \
  {                                 \
    v.visit(*this);                 \
  }
    

} // namespace util
} // namespace vast

#endif
