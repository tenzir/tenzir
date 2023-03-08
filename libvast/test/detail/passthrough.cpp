//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/passthrough.hpp"

#include "vast/test/test.hpp"
#include "vast/type.hpp"

namespace vast {

TEST(passthrough) {
  {
    MESSAGE("non-visitable types can be passed through");
    int i = 42;
    auto pi = detail::passthrough(i);
    CHECK_EQUAL(caf::get<int>(pi), i);
    auto f = [&](int& fi) {
      CHECK_EQUAL(fi, i);
      CHECK_EQUAL(&fi, &i);
    };
    caf::visit(f, pi);
  }
  {
    MESSAGE("visitable types can be passed through");
    auto t = type{bool_type{}};
    auto pt = detail::passthrough(t);
    CHECK_EQUAL(caf::get<bool_type>(t), bool_type{});
    CHECK_EQUAL(caf::get<type>(pt), t);
    auto f = [&](const type& ft, const concrete_type auto& fct) {
      CHECK_EQUAL(ft, fct);
      CHECK_EQUAL(ft, t);
      CHECK_EQUAL(&ft, &t);
    };
    caf::visit(f, pt, t);
  }
}

} // namespace vast
