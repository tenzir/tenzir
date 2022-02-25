//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/offset.hpp"
#include "vast/sketch/builder.hpp"
#include "vast/sketch/sketch.hpp"
#include "vast/table_slice.hpp"

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <type_traits>

namespace vast::sketch {

/// An accumulator for a table slice column.
template <class T>
concept accumulator = requires(T& x, const std::shared_ptr<arrow::Array>& xs) {
  { x.accumulate(xs) } -> std::same_as<caf::error>;
  { x.finish() } -> std::same_as<caf::expected<sketch>>;
};

/// Wraps an accumulator into a builder interface.
template <accumulator Accumulator>
class accumulator_builder : public builder {
public:
  explicit accumulator_builder(Accumulator acc = {})
    : accumulator_{std::move(acc)} {
  }

  caf::error add(const std::shared_ptr<arrow::Array>& xs) final {
    return accumulator_.accumulate(xs);
  }

  caf::expected<sketch> finish() final {
    auto result = accumulator_.finish();
    if (result)
      accumulator_ = {};
    return result;
  }

private:
  Accumulator accumulator_;
};

/// Explicit deduction guide for overload (not needed as of C++20).
template <accumulator Accumulator>
accumulator_builder(Accumulator) -> accumulator_builder<Accumulator>;

} // namespace vast::sketch
