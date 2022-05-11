//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/fbs/sketch.hpp"
#include "vast/flatbuffer.hpp"
#include "vast/sketch/sketch.hpp"
#include "vast/type.hpp"

#include <arrow/array.h>
#include <arrow/array/util.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <caf/error.hpp>
#include <flatbuffers/flatbuffers.h>
#include <fmt/format.h>

#include <memory>
#include <type_traits>

namespace vast::sketch {

template <typename T>
struct accumulator_traits;

template <>
struct accumulator_traits<vast::bool_type> {
  using accumulator_type = uint64_t;
  using flatbuffer_type = fbs::sketch::MinMaxU64;
  static fbs::sketch::Sketch flatbuffer_union_variant() {
    return fbs::sketch::Sketch::min_max_u64;
  }
};

template <>
struct accumulator_traits<vast::integer_type> {
  using accumulator_type = int64_t;
  using flatbuffer_type = fbs::sketch::MinMaxI64;
  static fbs::sketch::Sketch flatbuffer_union_variant() {
    return fbs::sketch::Sketch::min_max_i64;
  }
};

template <>
struct accumulator_traits<vast::count_type> {
  using accumulator_type = uint64_t;
  using flatbuffer_type = fbs::sketch::MinMaxU64;
  static fbs::sketch::Sketch flatbuffer_union_variant() {
    return fbs::sketch::Sketch::min_max_u64;
  }
};

template <>
struct accumulator_traits<vast::real_type> {
  using accumulator_type = double;
  using flatbuffer_type = fbs::sketch::MinMaxF64;
  static fbs::sketch::Sketch flatbuffer_union_variant() {
    return fbs::sketch::Sketch::min_max_f64;
  }
};

template <>
struct accumulator_traits<vast::duration_type> {
  using accumulator_type = int64_t;
  using flatbuffer_type = fbs::sketch::MinMaxI64;
  static fbs::sketch::Sketch flatbuffer_union_variant() {
    return fbs::sketch::Sketch::min_max_i64;
  }
};

template <>
struct accumulator_traits<vast::time_type> {
  using accumulator_type = int64_t;
  using flatbuffer_type = fbs::sketch::MinMaxI64;
  static fbs::sketch::Sketch flatbuffer_union_variant() {
    return fbs::sketch::Sketch::min_max_i64;
  }
};

template <typename T>
// requires: T is a vast::type
class min_max_accumulator {
public:
  using arrow_array_type = vast::type_to_arrow_array_t<T>;
  using traits = accumulator_traits<T>;
  using accumulator_type = typename traits::accumulator_type;
  using flatbuffer_type = typename traits::flatbuffer_type;

  caf::error accumulate(const std::shared_ptr<arrow::Array>& xs) {
    auto specific_array = std::static_pointer_cast<arrow_array_type>(xs);
    for (auto const& x : *specific_array) {
      if (!x)
        continue;
      min_ = std::min<accumulator_type>(min_, *x);
      max_ = std::max<accumulator_type>(max_, *x);
    }
    return caf::none;
  }

  [[nodiscard]] caf::expected<vast::sketch::sketch> finish() const {
    constexpr auto flatbuffer_size = 42; // FIXME: compute size manually
    flatbuffers::FlatBufferBuilder builder{flatbuffer_size};
    auto type = traits::flatbuffer_union_variant();
    auto minmax_offset = builder.CreateStruct(flatbuffer_type{min_, max_});
    auto union_offset = minmax_offset.Union();
    auto sketch_offset = fbs::CreateSketch(builder, type, union_offset);
    builder.Finish(sketch_offset);
    // VAST_ASSERT(builder.GetSize() == flatbuffer_size);
    auto fb = flatbuffer<fbs::Sketch>::make(builder.Release());
    if (!fb)
      return fb.error();
    return sketch{std::move(*fb)};
  }

private:
  accumulator_type min_ = {};
  accumulator_type max_ = {};
};

} // namespace vast::sketch
