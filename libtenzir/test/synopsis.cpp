//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/synopsis.hpp"

#include "tenzir/bool_synopsis.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/synopsis.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/time_synopsis.hpp"

#include <arrow/builder.h>
#include <caf/binary_serializer.hpp>

using namespace std::chrono_literals;
using namespace tenzir;
using namespace tenzir::test;

namespace {

const tenzir::time epoch;

// Helper to create a series from multiple time values
auto make_time_series(std::vector<tenzir::time> values) -> series {
  auto builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::NANO), arrow::default_memory_pool()};
  for (auto value : values) {
    auto status = builder.Append(value.time_since_epoch().count());
    TENZIR_ASSERT(status.ok());
  }
  auto result = builder.Finish();
  TENZIR_ASSERT(result.ok());
  return series{type{time_type{}}, std::move(*result)};
}

} // namespace

TEST("min - max synopsis") {
  using tenzir::time;
  using namespace nft;
  factory<synopsis>::initialize();
  auto x = factory<synopsis>::make(type{time_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(x, nullptr);
  x->add(make_time_series({time{epoch + 4s}, time{epoch + 7s}}));
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
  // `in` matches because `4 == min`; `not_in` matches because `7` is a stored
  // value not in the list.
  verify(zero_four_view, {T, T, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op [7, 9]");
  auto seven_nine = data{list{seven, nine}};
  auto seven_nine_view = make_view(seven_nine);
  // Symmetric to the previous case: `in` matches via `max`, `not_in` via `min`.
  verify(seven_nine_view, {T, T, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op [0, 9]");
  auto zero_nine = data{list{zero, nine}};
  auto zero_nine_view = make_view(zero_nine);
  verify(zero_nine_view, {F, T, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op [5, 6]");
  time five = epoch + 5s;
  auto five_six = data{list{five, six}};
  auto five_six_view = make_view(five_six);
  // Both elements lie strictly inside `(min, max)`. `in` is unknown
  // (regression for the catalog bug); `not_in` is true because neither
  // `4` nor `7` is in the list.
  verify(five_six_view, {N, T, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op [4, 7]");
  auto four_seven = data{list{four, seven}};
  auto four_seven_view = make_view(four_seven);
  // Both endpoints in the list, but interior values are unknown — `not_in`
  // cannot be ruled out.
  verify(four_seven_view, {T, N, N, N, N, N, N, N, N, N});
  // Lists with `null` are treated conservatively. `null` cannot be compared
  // against a stored value, so it neither confirms membership nor rules it
  // out. `[4, null]` keeps `in` true via the `4 == min` match but forces
  // `not_in` to stay unknown.
  MESSAGE("[4,7] op [4, null]");
  auto four_null = data{list{four, data{caf::none}}};
  auto four_null_view = make_view(four_null);
  verify(four_null_view, {T, N, N, N, N, N, N, N, N, N});
  // An all-null list has no comparable elements at all, so both `in` and
  // `not_in` must be unknown — pruning either direction would be unsafe.
  MESSAGE("[4,7] op [null]");
  auto just_null = data{list{data{caf::none}}};
  auto just_null_view = make_view(just_null);
  verify(just_null_view, {N, N, N, N, N, N, N, N, N, N});
  // Check that we don't do any implicit conversions.
  MESSAGE("[4,7] op count{{5}}");
  uint64_t c = 5;
  verify(c, {N, N, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op [count{{5}}, 7]");
  auto heterogeneous = data{list{c, seven}};
  auto heterogeneous_view = make_view(heterogeneous);
  // The type-mismatched element makes `min`'s membership uncertain, so
  // `not_in` must stay nullopt rather than incorrectly inverting `in`.
  verify(heterogeneous_view, {T, N, N, N, N, N, N, N, N, N});
}
