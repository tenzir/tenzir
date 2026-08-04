//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/projection.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/test/test.hpp>

#include <arrow/builder.h>

#include <initializer_list>
#include <optional>

namespace tenzir::plugins::iceberg {

namespace {

auto make_booleans(std::initializer_list<std::optional<bool>> values)
  -> std::shared_ptr<arrow::BooleanArray> {
  auto builder = arrow::BooleanBuilder{};
  for (auto value : values) {
    if (value) {
      check(builder.Append(*value));
    } else {
      check(builder.AppendNull());
    }
  }
  return std::static_pointer_cast<arrow::BooleanArray>(check(builder.Finish()));
}

TEST("conversion failures beneath null parents are hidden") {
  auto convertible = make_booleans({false, true});
  auto parent = make_booleans({std::nullopt, true});
  CHECK(projection::count_visible_conversion_failures(convertible, parent)
        == 0);
}

TEST("conversion failures beneath valid parents remain visible") {
  auto convertible = make_booleans({false, false, std::nullopt});
  auto parent = make_booleans({std::nullopt, true, true});
  CHECK(projection::count_visible_conversion_failures(convertible, parent)
        == 1);
  CHECK(projection::count_visible_conversion_failures(convertible, nullptr)
        == 2);
}

} // namespace

} // namespace tenzir::plugins::iceberg
