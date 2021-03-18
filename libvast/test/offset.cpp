// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/offset.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/offset.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/offset.hpp"

#include "vast/test/test.hpp"

using namespace vast;

TEST(offset printing) {
  auto o = offset{0, 10, 8};
  CHECK(to_string(o) == "0,10,8");
}

TEST(offset parsing) {
  auto o = to<offset>("0,4,8,12");
  CHECK(o);
  CHECK(*o == offset({0, 4, 8, 12}));
}
