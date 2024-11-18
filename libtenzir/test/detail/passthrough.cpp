//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/passthrough.hpp"

#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

namespace tenzir {

TEST(passthrough) {
  {
    MESSAGE("non-visitable types can be passed through");
    int i = 42;
    auto pi = detail::passthrough(i);
    CHECK_EQUAL(as<int>(pi), i);
    auto f = [&](int& fi) {
      CHECK_EQUAL(fi, i);
      CHECK_EQUAL(&fi, &i);
    };
    tenzir::match(pi, f);
  }
  {
    MESSAGE("visitable types can be passed through");
    auto t = type{bool_type{}};
    auto pt = detail::passthrough(t);
    CHECK_EQUAL(as<bool_type>(t), bool_type{});
    CHECK_EQUAL(as<type>(pt), t);
    auto f = [&](const type& ft, const concrete_type auto& fct) {
      CHECK_EQUAL(ft, fct);
      CHECK_EQUAL(ft, t);
      CHECK_EQUAL(&ft, &t);
    };
    tenzir::match(std::tie(pt, t), f);
  }
}

} // namespace tenzir
