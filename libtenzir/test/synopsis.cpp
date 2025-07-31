//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/synopsis.hpp"

#include "tenzir/bool_synopsis.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/synopsis.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/time_synopsis.hpp"

#include <caf/binary_serializer.hpp>

using namespace std::chrono_literals;
using namespace tenzir;
using namespace tenzir::test;

namespace {

const tenzir::time epoch;

} // namespace

TEST("min - max synopsis") {
  using tenzir::time;
  using namespace nft;
  factory<synopsis>::initialize();
  auto x = factory<synopsis>::make(type{time_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(x, nullptr);
  x->add(time{epoch + 4s});
  x->add(time{epoch + 7s});
  auto verify = verifier{x.get()};
  MESSAGE("[4,7] op 0");
  time zero = epoch + 0s;
  verify(zero, {N, N, N, N, F, T, F, F, T, T});
  MESSAGE("[4,7] op 4");
  time four = epoch + 4s;
  verify(four, {N, N, N, N, T, N, F, T, T, T});
  MESSAGE("[4,7] op 6");
  time six = epoch + 6s;
  verify(six, {N, N, N, N, N, N, T, T, T, T});
  MESSAGE("[4,7] op 7");
  time seven = epoch + 7s;
  verify(seven, {N, N, N, N, T, N, T, T, F, T});
  MESSAGE("[4,7] op 9");
  time nine = epoch + 9s;
  verify(nine, {N, N, N, N, F, T, T, T, F, F});
  MESSAGE("[4,7] op [0, 4]");
  auto zero_four = data{list{zero, four}};
  auto zero_four_view = make_view(zero_four);
  verify(zero_four_view, {T, F, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op [7, 9]");
  auto seven_nine = data{list{seven, nine}};
  auto seven_nine_view = make_view(seven_nine);
  verify(seven_nine_view, {T, F, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op [0, 9]");
  auto zero_nine = data{list{zero, nine}};
  auto zero_nine_view = make_view(zero_nine);
  verify(zero_nine_view, {F, T, N, N, N, N, N, N, N, N});
  // Check that we don't do any implicit conversions.
  MESSAGE("[4,7] op count{{5}}");
  uint64_t c = 5;
  verify(c, {N, N, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op [count{{5}}, 7]");
  auto heterogeneous = data{list{c, seven}};
  auto heterogeneous_view = make_view(heterogeneous);
  verify(heterogeneous_view, {T, F, N, N, N, N, N, N, N, N});
}
