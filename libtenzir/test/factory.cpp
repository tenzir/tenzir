//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/factory.hpp"

#include "tenzir/test/test.hpp"

#include <memory>
#include <typeindex>

using namespace tenzir;

namespace {

// The polymorphic abstract class.
class abstract {
public:
  abstract(int x, int y) : x_{x}, y_{y} {
    // nop
  }

  virtual ~abstract() = default;

  virtual int f() const {
    return x_ + y_;
  }

protected:
  int x_;
  int y_;
};

// A concrete class
class concrete : public abstract {
public:
  concrete(int x, int y) : abstract{x, y} {
    // nop
  }

  int f() const override {
    return x_ * y_;
  }
};

std::unique_ptr<abstract> double_make(int x, int y) {
  return std::make_unique<concrete>(x * 2, y * 2);
}

} // namespace

namespace tenzir {

template <>
struct factory_traits<abstract> {
  // Mandatory types
  using result_type = std::unique_ptr<abstract>;
  using key_type = size_t;
  using signature = result_type (*)(int, int);

  // Convenience functions

  // Enables type-based retrieval of factory functions in factory<Type>.
  template <class T>
  static key_type key() {
    return std::type_index{typeid(T)}.hash_code() % 42;
  }

  // Enables type-based registraction via factory<Type>::add<T>.
  template <class T>
  static result_type make(int x, int y) {
    return std::make_unique<T>(x, y);
  }
};

// Define the factory.
using F = factory<abstract>;

struct fixture {
  fixture() {
    F::clear();
  }
};

} // namespace tenzir

WITH_FIXTURE(fixture) {
  TEST("convenient interface for conrete type registration") {
    CHECK_EQUAL(F::get<concrete>(), nullptr);     // not yet registered
    CHECK(F::add<concrete>());                    // register first
    CHECK(! F::add<concrete>());                  // works only once per key
    CHECK_NOT_EQUAL(F::get<concrete>(), nullptr); // now we have function
  }

  TEST("type-based factory retrieval and construction") {
    REQUIRE(F::add<concrete>());
    auto f = F::get<concrete>();
    REQUIRE_NOT_EQUAL(f, nullptr);
    auto x = f(1, 2);
    REQUIRE_NOT_EQUAL(x, nullptr);
    CHECK_EQUAL(x->f(), 1 * 2);
  }

  TEST("key-based registration and construction") {
    auto k = F::traits::key<concrete>() + 1;
    CHECK(F::add(k, double_make));
    auto f = F::get(k);
    REQUIRE_NOT_EQUAL(f, nullptr);
    auto x = f(3, 7);
    CHECK_EQUAL(x->f(), (2 * 3) * (2 * 7));
    auto y = F::make(k, 2, 3);
    REQUIRE_NOT_EQUAL(y, nullptr);
    CHECK_EQUAL(y->f(), (2 * 2) * (2 * 3));
  }

  TEST("concstruction with a priori known type") {
    REQUIRE(F::add<concrete>());
    auto x = F::make<concrete>(2, 3);
    REQUIRE_NOT_EQUAL(x, nullptr);
    CHECK_EQUAL(x->f(), 2 * 3);
  }
}
