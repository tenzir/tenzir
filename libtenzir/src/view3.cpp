//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/view3.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/series_builder.hpp"

namespace tenzir {

auto values3(const table_slice& x) -> generator<record_view3> {
  auto array = check(to_record_batch(x)->ToStructArray());
  for (auto row : values3(*array)) {
    TENZIR_ASSERT(row);
    co_yield *row;
  }
}

auto make_view_wrapper(data_view2 x) -> view_wrapper {
  auto b = series_builder{};
  b.data(std::move(x));
  return view_wrapper{b.finish_assert_one_array().array};
}

} // namespace tenzir

namespace fmt {

#define X(view)                                                                \
  auto formatter<tenzir::view>::format(const tenzir::view& value,              \
                                       format_context& ctx) const              \
    -> format_context::iterator {                                              \
    auto printer = tenzir::json_printer{{                                      \
      .tql = true,                                                             \
      .oneline = true,                                                         \
    }};                                                                        \
    auto it = ctx.out();                                                       \
    const auto ok = printer.print(it, value);                                  \
    TENZIR_ASSERT(ok);                                                         \
    return it;                                                                 \
  }                                                                            \
  static_assert(true)

X(data_view3);
X(record_view3);
X(list_view3);

#undef X

} // namespace fmt
