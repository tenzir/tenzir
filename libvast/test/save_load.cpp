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

#define SUITE serialization

#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system.hpp"

#include "vast/save.hpp"
#include "vast/load.hpp"

using namespace std::string_literals;
using namespace vast;

namespace {

// A type that models the Serializable concept.
struct foo {
  int i = 0;
};

template <class Inspector>
auto inspect(Inspector& f, foo& x) {
  return f(x.i);
}

// A type that models the State concept.
class bar {
public:
  void set(int i) {
    f_.i = i;
  }

  int get() const {
    return f_.i;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, bar& b) {
    return f(b.f_);
  }

private:
  foo f_;
};

} // namespace <anonymous>

FIXTURE_SCOPE(serialization_tests, fixtures::deterministic_actor_system)

TEST(variadic) {
  std::string buf;
  CHECK_EQUAL(save<compression::lz4>(sys, buf, 42, 4.2, 1337u, "foo"s),
              caf::none);
  int i;
  double d;
  unsigned u;
  std::string s;
  CHECK_EQUAL(load<compression::lz4>(sys, buf, i, d, u, s), caf::none);
  CHECK_EQUAL(i, 42);
  CHECK_EQUAL(d, 4.2);
  CHECK_EQUAL(u, 1337u);
  CHECK_EQUAL(s, "foo");
}

TEST(custom type modeling serializable) {
  std::vector<char> buf;
  foo x;
  x.i = 42;
  CHECK_EQUAL(save(sys, buf, x), caf::none);
  foo y;
  CHECK_EQUAL(load(sys, buf, y), caf::none);
  CHECK_EQUAL(x.i, y.i);
}

TEST(custom type modeling state) {
  std::vector<char> buf;
  bar x;
  x.set(42);
  CHECK_EQUAL(save(sys, buf, x), caf::none);
  bar y;
  CHECK_EQUAL(load(sys, buf, y), caf::none);
  CHECK_EQUAL(x.get(), y.get());
}

FIXTURE_SCOPE_END()
