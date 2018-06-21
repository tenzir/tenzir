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

struct square final : rectangle {
  square(double x) : rectangle{x, x} {
    // nop
  }
};

struct circle final : shape {
  circle(double r) : r{r} {
    // nop
  }

  const double r = 0;
};

struct no_default_ctor {
  explicit no_default_ctor(int x) : data{x} { }
  int data = 0;
};

inline const shape& as_shape(const shape& x) {
  return x;
}

} // namespace <anonymous>

TEST(leaf visitation) {
  auto compute_area = make_polymorphic_visitor<shape>(
    [&](const square& x) { return std::pow(x.x, 2); },
    [&](const rectangle& x) { return x.x * x.y; },
    [&](const circle& x) { return std::pow(x.r, 2) * 3.14; }
  );
  auto x = rectangle{3, 4};
  auto y = square{5};
  auto z = circle{7};
  CHECK_EQUAL(compute_area(as_shape(x)), 12.0);
  CHECK_EQUAL(compute_area(as_shape(y)), 25.0);
  CHECK_EQUAL(compute_area(as_shape(z)), 153.86);
}

TEST(ordering) {
  using std::literals::operator""s;
  auto get_name_1 = make_polymorphic_visitor<shape>(
    [&](const rectangle&) { return "rectangle"s; },
    // Unreachable, never called because rectangle matches first.
    [&](const square&) { return "square"s; },
    [&](const circle&) { return "circle"s; }
  );
  auto get_name_2 = make_polymorphic_visitor<shape>(
    // OK, matches before rectable does.
    [&](const square&) { return "square"s; },
    [&](const rectangle&) { return "rectangle"s; },
    [&](const circle&) { return "circle"s; }
  );
  auto x = rectangle{3, 4};
  auto y = square{5};
  auto z = circle{7};
  CHECK_EQUAL(*get_name_1(as_shape(x)), "rectangle");
  CHECK_EQUAL(*get_name_1(as_shape(y)), "rectangle");
  CHECK_EQUAL(*get_name_1(as_shape(z)), "circle");
  CHECK_EQUAL(*get_name_2(as_shape(x)), "rectangle");
  CHECK_EQUAL(*get_name_2(as_shape(y)), "square");
  CHECK_EQUAL(*get_name_2(as_shape(z)), "circle");
}

TEST(default constructability not required) {
  auto f = make_polymorphic_visitor<shape>(
    [&](const square&) { return no_default_ctor{2}; },
    [&](const rectangle&) { return no_default_ctor{1}; }
  );
  auto x = square{5};
  auto result = f(as_shape(x));
  REQUIRE(result);
  CHECK_EQUAL(result->data, 2);
  auto y = circle{7};
  result = f(as_shape(y));
  CHECK(!result);
}
