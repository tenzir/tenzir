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

#include "vast/visitor.hpp"

#define SUITE visitor
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

} // namespace <anonymous>

TEST(lambda visitation) {
  auto area = 0.0;
  auto compute_area = make_visitor<rectangle, square, circle>(
    [&](const rectangle& x) { area = x.x * x.y; },
    [&](const square& x) { area = std::pow(x.x, 2); },
    [&](const circle& x) { area = std::pow(x.r, 2) * 3.14; }
  );
  auto x = rectangle{3, 4};
  auto y = square{5};
  auto z = circle{7};
  shape& sx = x;
  shape& sy = y;
  shape& sz = z;
  compute_area(sx);
  CHECK_EQUAL(area, 12.0);
  compute_area(sy);
  CHECK_EQUAL(area, 25.0);
  compute_area(sz);
  CHECK_EQUAL(area, 153.86);
}
