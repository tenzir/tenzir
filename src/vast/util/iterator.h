#ifndef VAST_UTIL_ITERATOR_H
#define VAST_UTIL_ITERATOR_H

#include "vast/traits.h"
#include "vast/util/operators.h"

namespace vast {
namespace util {

template <typename, typename, typename, typename, typename>
class iterator_facade;

// Provides clean access to iterator internals. Similar to vast::access.
class iterator_access
{
  template <typename, typename, typename, typename, typename>
  friend class iterator_facade;

public:
  template <typename Facade>
  static typename Facade::reference dereference(Facade const& f)
  {
    return f.dereference();
  }

  template <typename Facade>
  static void increment(Facade& f)
  {
    f.increment();
  }

  template <typename Facade>
  static void decrement(Facade& f)
  {
    f.decrement();
  }

  template <typename Facade>
  static void advance(Facade& f, typename Facade::difference_type n)
  {
    f.advance(n);
  }

  template <typename Facade1, typename Facade2>
  static bool equals(Facade1 const& f1, Facade2 const& f2)
  {
    return f1.equals(f2);
  }

  template <typename Facade1, typename Facade2>
  static auto distance_from(Facade1 const& f1, Facade2 const& f2)
    -> typename Facade2::difference_type
  {
    return f2.distance_to(f1);
  }

private:
  template <typename I, typename C, typename V, typename R, typename D>
  static I& I(iterator_facade<I, C, V, R, D>& facade)
  {
    return *static_cast<I*>(&facade);
  }

  template <typename I, typename C, typename V, typename R, typename D>
  static I const& derived(iterator_facade<I, C, V, R, D> const& facade)
  {
    return *static_cast<I const*>(&facade);
  }

  iterator_access() = default;
};

/// A simple reimplementation of `boost::iterator_facade`.
template <
  typename Derived,
  typename Category,
  typename Value,
  typename Reference  = Value&,
  typename Difference = std::ptrdiff_t
>
class iterator_facade : totally_ordered<
                          iterator_facade<
                            Derived, Category, Value, Reference, Difference
                          >
                        >
{
private:
  using is_immutable = typename std::is_const<Value>::type;

  template <typename R, typename P>
  struct operator_arrow_dispatch
  {
    struct proxy
    {
      explicit proxy(R const& x)
        : ref(x)
      {
      }

      R* operator->()
      {
        return std::addressof(ref);
      }

      R ref;
    };

    using result_type = proxy;

    static result_type apply(R const& x)
    {
      return result_type{x};
    }
  };

  template <typename T, typename P>
  struct operator_arrow_dispatch<T&, P>
  {
    using result_type = P;

    static result_type apply(T& x)
    {
      return std::addressof(x);
    }
  };

  Derived& derived()
  {
    return *static_cast<Derived*>(this);
  }

  Derived const& derived() const
  {
    return *static_cast<Derived const*>(this);
  }


public:
  using iterator_category = Category;
  using value_type = typename std::remove_cv<Value>::type;
  using reference = Reference;
  using difference_type = Difference;
  using arrow_dispatcher = operator_arrow_dispatch<
    reference,
    typename std::add_pointer<
      Conditional<is_immutable, value_type const, value_type>
    >::type
  >;

  using pointer = typename arrow_dispatcher::result_type;

  // TODO: operator[]

  reference operator*() const
  {
    return iterator_access::dereference(derived());
  }

  pointer operator->() const
  {
    return arrow_dispatcher::apply(*derived());
  }

  Derived& operator++()
  {
    iterator_access::increment(derived());
    return derived();
  }

  Derived operator++(int)
  {
    Derived tmp{derived()};
    ++*this;
    return tmp;
  }

  Derived& operator--()
  {
    iterator_access::decrement(derived());
    return derived();
  }

  Derived operator--(int)
  {
    Derived tmp{derived()};
    --*this;
    return tmp;
  }

  Derived& operator+=(difference_type n)
  {
    iterator_access::advance(derived(), n);
    return derived();
  }

  Derived& operator-=(difference_type n)
  {
    iterator_access::advance(derived(), -n);
    return derived();
  }


  Derived operator-(difference_type x) const
  {
    Derived result(derived());
    return result -= x;
  }

  friend bool operator==(iterator_facade const& x, iterator_facade const& y)
  {
    return iterator_access::equals(static_cast<Derived const&>(x),
                                   static_cast<Derived const&>(y));
  }

  friend bool operator<(iterator_facade const& x, iterator_facade const& y)
  {
    return 0 > iterator_access::distance_from(static_cast<Derived const&>(x),
                                              static_cast<Derived const&>(y));
  }

  friend Derived operator+(iterator_facade const& x, difference_type n)
  {
    Derived tmp{static_cast<Derived const&>(x)};
    return tmp += n;
  }

  friend Derived operator+(difference_type n, iterator_facade const& x)
  {
    Derived tmp{static_cast<Derived const&>(x)};
    return tmp += n;
  }

  friend auto operator-(iterator_facade const& x, iterator_facade const& y)
    -> decltype(iterator_access::distance_from(x, y))
  {
    return iterator_access::distance_from(static_cast<Derived const&>(x),
                                          static_cast<Derived const&>(y));
  }

protected:
  using iterator_facade_type =
    iterator_facade<Derived, Category, Value, Reference, Difference>;
};

} // namspace util
} // namespace vast

#endif
