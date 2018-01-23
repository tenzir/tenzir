/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_DETAIL_ITERATOR_HPP
#define VAST_DETAIL_ITERATOR_HPP

#include <memory>

#include "vast/detail/operators.hpp"

namespace vast::detail {

template <typename, typename, typename, typename, typename>
class iterator_facade;

// Provides clean access to iterator internals.
class iterator_access {
  template <typename, typename, typename, typename, typename>
  friend class iterator_facade;

public:
  template <typename Facade>
  static decltype(auto) dereference(const Facade& f) {
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

  template <typename Facade, typename Distance>
  static void advance(Facade& f, Distance n) {
    f.advance(n);
  }

  template <typename Facade1, typename Facade2>
  static bool equals(const Facade1& f1, const Facade2& f2) {
    return f1.equals(f2);
  }

  template <typename Facade1, typename Facade2>
  static auto distance_from(const Facade1& f1, const Facade2& f2) {
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
      explicit proxy(const R& x) : ref(x) {
      }

      R* operator->() {
        return std::addressof(ref);
      }

      R ref;
    };

    using result_type = proxy;

    static result_type apply(const R& x) {
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

  const Derived& derived() const {
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

  struct postfix_increment_proxy {
    explicit postfix_increment_proxy(const Derived& x) : value(*x) {
    }

    value_type& operator*() const {
      return value;
    }

    mutable value_type value;
  };

  using postfix_increment_result =
    std::conditional_t<
      std::is_convertible<
        reference,
        std::add_lvalue_reference_t<Value const>
      >::value,
      postfix_increment_proxy,
      Derived
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

  postfix_increment_result operator++(int) {
    postfix_increment_result tmp{derived()};
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

  friend bool operator==(const iterator_facade& x, const iterator_facade& y) {
    return iterator_access::equals(static_cast<const Derived&>(x),
                                   static_cast<const Derived&>(y));
  }

  friend bool operator<(const iterator_facade& x, const iterator_facade& y) {
    return 0 > iterator_access::distance_from(static_cast<const Derived&>(x),
                                              static_cast<const Derived&>(y));
  }

  friend Derived operator+(const iterator_facade& x, difference_type n) {
    Derived tmp{static_cast<const Derived&>(x)};
    return tmp += n;
  }

  friend Derived operator+(difference_type n, const iterator_facade& x) {
    Derived tmp{static_cast<const Derived&>(x)};
    return tmp += n;
  }

  friend auto operator-(const iterator_facade& x, const iterator_facade& y) {
    return iterator_access::distance_from(static_cast<const Derived&>(x),
                                          static_cast<const Derived&>(y));
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

    explicit iterator_adaptor(const Base& b)
      : iterator_{b} {
    }

    const Base& base() const {
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

    bool equals(const iterator_adaptor& other) const {
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

    Difference distance_to(const iterator_adaptor& other) const {
      return other.base() - iterator_;
    }

 private:
    Base iterator_;
};

} // namespace vast::detail

#endif
