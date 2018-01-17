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

#include "vast/save.hpp"
#include "vast/load.hpp"

#define SUITE serialization
#include "test.hpp"

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

TEST(variadic) {
  std::string buf;
  auto m = save<compression::lz4>(buf, 42, 4.2, 1337u, "foo"s);
  CHECK(!m.error());
  int i;
  double d;
  unsigned u;
  std::string s;
  m = load<compression::lz4>(buf, i, d, u, s);
  CHECK(!m.error());
  CHECK_EQUAL(i, 42);
  CHECK_EQUAL(d, 4.2);
  CHECK_EQUAL(u, 1337u);
  CHECK_EQUAL(s, "foo");
}

TEST(custom type modeling serializable) {
  std::vector<char> buf;
  foo x;
  x.i = 42;
  auto m = save(buf, x);
  foo y;
  m = load(buf, y);
  CHECK_EQUAL(x.i, y.i);
}

TEST(custom type modeling state) {
  std::vector<char> buf;
  bar x;
  x.set(42);
  auto m = save(buf, x);
  bar y;
  m = load(buf, y);
  CHECK_EQUAL(x.get(), y.get());
}
