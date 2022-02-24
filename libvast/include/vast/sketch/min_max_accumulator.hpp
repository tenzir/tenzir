//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fbs/sketch.hpp"

#include <arrow/array/util.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <caf/error.hpp>
#include <flatbuffers/flatbuffers.h>

#include <memory>
#include <type_traits>

namespace vast::sketch {

class min_max_accumulator {
public:
  caf::error accumulate(const std::shared_ptr<arrow::Array>& xs) {
    auto result = arrow::Result<arrow::Datum>{};
    if (!min_) {
      VAST_ASSERT(!max_);
      result = arrow::compute::MinMax(xs);
    } else {
      // Concatenate with existing min and max for convenient calling of Arrow
      // API. If we had a function that operates on scalars (arrow::Min(Scalar,
      // Scalar)), this would not be needed.
      auto ys = arrow::ChunkedArray::Make(
        {xs, arrow::MakeArrayFromScalar(*min_, 1).MoveValueUnsafe(),
         arrow::MakeArrayFromScalar(*max_, 1).MoveValueUnsafe()});
      result = arrow::compute::MinMax(ys.MoveValueUnsafe());
    }
    if (!result.ok())
      return caf::make_error(ec::unimplemented,
                             fmt::format("MinMax kernel failed to execute: {}",
                                         result.status().ToString()));
    auto min_max = result.MoveValueUnsafe().scalar_as<arrow::StructScalar>();
    min_ = min_max.value[0];
    max_ = min_max.value[1];
    return caf::none;
  }

  caf::expected<sketch> finish() const {
    constexpr auto flatbuffer_size = 42; // FIXME: compute size manually
    flatbuffers::FlatBufferBuilder builder{flatbuffer_size};
    // FIXME: convert min/max to data.
    // auto min_offset =
    // auto max_offset =
    // auto min_max_offset
    //  = fbs::sketch::CreateMinMax(builder, min_offset, max_offset);
    // auto sketch_offset = fbs::CreateSketch(
    //  builder, fbs::sketch::Sketch::min_max, min_max_offset.Union());
    // builder.Finish(sketch_offset);
    // FIXME
    // VAST_ASSERT(builder.GetSize() == flatbuffer_size);
    auto fb = flatbuffer<fbs::Sketch>::make(builder.Release());
    if (!fb)
      return fb.error();
    return sketch{std::move(*fb)};
  }

private:
  std::shared_ptr<arrow::Scalar> min_ = {};
  std::shared_ptr<arrow::Scalar> max_ = {};
};

} // namespace vast::sketch
