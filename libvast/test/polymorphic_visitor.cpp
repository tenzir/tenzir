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

#include <cmath>

#include "vast/polymorphic_visitor.hpp"

#define SUITE polymorphic_visitor
#include "test.hpp"

using namespace vast;

namespace {

struct shape {
  virtual ~shape() {
    // nop
  }
};

struct rectangle : shape {
  rectangle(double x, double y) : x{x}, y{y} {
    // nop
  }

  const double x = 0;
  const double y = 0;
};

struct square : rectangle {
  square(double x) : rectangle{x, x} {
    // nop
  }
};

struct circle : shape {
  circle(double r) : r{r} {
    // nop
  }

  const double r = 0;
};

struct no_default_ctor {
  explicit no_default_ctor(int x) : data{x} { }
  int data = 0;
};

template <class T>
const shape& as_shape(const T& x) {
  return x;
}

} // namespace <anonymous>

TEST(lambda visitation) {
  auto compute_area = make_visitor<rectangle, square, circle>(
    [&](const rectangle& x) { return x.x * x.y; },
    [&](const square& x) { return std::pow(x.x, 2); },
    [&](const circle& x) { return std::pow(x.r, 2) * 3.14; }
  );
  auto x = rectangle{3, 4};
  auto y = square{5};
  auto z = circle{7};
  CHECK_EQUAL(compute_area(as_shape(x)), 12.0);
  CHECK_EQUAL(compute_area(as_shape(y)), 25.0);
  CHECK_EQUAL(compute_area(as_shape(z)), 153.86);
}

TEST(default constructability not required) {
  auto f = make_visitor<rectangle, square>(
    [&](const rectangle&) { return no_default_ctor{1}; },
    [&](const square&) { return no_default_ctor{2}; }
  );
  auto x = square{5};
  auto result = f(as_shape(x));
  REQUIRE(result);
  CHECK_EQUAL(result->data, 2);
  auto y = circle{7};
  result = f(as_shape(y));
  CHECK(!result);
}
