#ifndef VAST_DETAIL_ITERATOR_HPP
#define VAST_DETAIL_ITERATOR_HPP

#include <memory>

#include "vast/detail/operators.hpp"

namespace vast {
namespace detail {

template <typename, typename, typename, typename, typename>
class iterator_facade;

// Provides clean access to iterator internals.
class iterator_access {
  template <typename, typename, typename, typename, typename>
  friend class iterator_facade;

public:
  template <typename Facade>
  static typename Facade::reference dereference(Facade const& f) {
    return f.dereference();
  }

  template <typename Facade>
  static void increment(Facade& f) {
    f.increment();
  }

  template <typename Facade>
  static void decrement(Facade& f) {
    f.decrement();
  }

  template <typename Facade>
  static void advance(Facade& f, typename Facade::difference_type n) {
    f.advance(n);
  }

  template <typename Facade1, typename Facade2>
  static bool equals(Facade1 const& f1, Facade2 const& f2) {
    return f1.equals(f2);
  }

  template <typename Facade1, typename Facade2>
  static auto distance_from(Facade1 const& f1, Facade2 const& f2) ->
    typename Facade2::difference_type {
    return f2.distance_to(f1);
  }

private:
  iterator_access() = default;
};

/// A simple version of `boost::iterator_facade`.
template <
  typename Derived,
  typename Value,
  typename Category,
  typename Reference  = Value&,
  typename Difference = std::ptrdiff_t
>
class iterator_facade : totally_ordered<
                          iterator_facade<
                            Derived, Value, Category, Reference, Difference
                          >
                        > {
private:
  template <typename R, typename P>
  struct operator_arrow_dispatch {
    struct proxy {
      explicit proxy(R const& x) : ref(x) {
      }

      R* operator->() {
        return std::addressof(ref);
      }

      R ref;
    };

    using result_type = proxy;

    static result_type apply(R const& x) {
      return result_type{x};
    }
  };

  template <typename T, typename P>
  struct operator_arrow_dispatch<T&, P> {
    using result_type = P;

    static result_type apply(T& x) {
      return std::addressof(x);
    }
  };

  Derived& derived() {
    return *static_cast<Derived*>(this);
  }

  Derived const& derived() const {
    return *static_cast<Derived const*>(this);
  }

public:
  using iterator_category = Category;
  using value_type = std::remove_cv_t<Value>;
  using reference = Reference;
  using difference_type = Difference;
  using arrow_dispatcher =
    operator_arrow_dispatch<
      reference,
      std::add_pointer_t<
        std::conditional_t<
          std::is_const<Value>::value,
          value_type const,
          value_type
        >
      >
    >;

  using pointer = typename arrow_dispatcher::result_type;

  // TODO: operator[]

  reference operator*() const {
    return iterator_access::dereference(derived());
  }

  pointer operator->() const {
    return arrow_dispatcher::apply(*derived());
  }

  Derived& operator++() {
    iterator_access::increment(derived());
    return derived();
  }

  Derived operator++(int) {
    Derived tmp{derived()};
    ++*this;
    return tmp;
  }

  Derived& operator--() {
    iterator_access::decrement(derived());
    return derived();
  }

  Derived operator--(int) {
    Derived tmp{derived()};
    --*this;
    return tmp;
  }

  Derived& operator+=(difference_type n) {
    iterator_access::advance(derived(), n);
    return derived();
  }

  Derived& operator-=(difference_type n) {
    iterator_access::advance(derived(), -n);
    return derived();
  }

  Derived operator-(difference_type x) const {
    Derived result(derived());
    return result -= x;
  }

  friend bool operator==(iterator_facade const& x, iterator_facade const& y) {
    return iterator_access::equals(static_cast<Derived const&>(x),
                                   static_cast<Derived const&>(y));
  }

  friend bool operator<(iterator_facade const& x, iterator_facade const& y) {
    return 0 > iterator_access::distance_from(static_cast<Derived const&>(x),
                                              static_cast<Derived const&>(y));
  }

  friend Derived operator+(iterator_facade const& x, difference_type n) {
    Derived tmp{static_cast<Derived const&>(x)};
    return tmp += n;
  }

  friend Derived operator+(difference_type n, iterator_facade const& x) {
    Derived tmp{static_cast<Derived const&>(x)};
    return tmp += n;
  }

  friend auto operator-(iterator_facade const& x, iterator_facade const& y)
    -> decltype(iterator_access::distance_from(x, y)) {
    return iterator_access::distance_from(static_cast<Derived const&>(x),
                                          static_cast<Derived const&>(y));
  }

protected:
  using iterator_facade_type =
    iterator_facade<Derived, Value, Category, Reference, Difference>;
};

/// A simple version of `boost::iterator_adaptor`.
template <
  typename Derived,
  typename Base,
  typename Value,
  typename Category,
  typename Reference = Value&,
  typename Difference = std::ptrdiff_t
>
class iterator_adaptor
  : public iterator_facade<
             Derived, Value, Category, Reference, Difference
           > {
 public:
    using base_iterator = Base;
    using super = iterator_adaptor<
      Derived, Base, Value, Category, Reference, Difference
    >;

    iterator_adaptor() = default;

    explicit iterator_adaptor(Base const& b)
      : iterator_{b} {
    }

    Base const& base() const {
      return iterator_;
    }

 protected:
    Base& base() {
      return iterator_;
    }

 private:
    friend iterator_access;

    Reference dereference() const {
      return *iterator_;
    }

    bool equals(iterator_adaptor const& other) const {
      return iterator_ == other.base();
    }

    void advance(Difference n) {
      iterator_ += n;
    }

    void increment() {
      ++iterator_;
    }

    void decrement() {
      --iterator_;
    }

    Difference distance_to(iterator_adaptor const& other) const {
      return other.base() - iterator_;
    }

 private:
    Base iterator_;
};

} // namspace detail
} // namespace vast

#endif
