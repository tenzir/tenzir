//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/offset.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/table_slice.hpp"

#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace vast {

std::strong_ordering
operator<=>(const offset& lhs, const offset& rhs) noexcept {
  if (&lhs == &rhs)
    return std::strong_ordering::equal;
  const auto [lhs_mismatch, rhs_mismatch]
    = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
  const auto lhs_exhausted = lhs_mismatch == lhs.end();
  const auto rhs_exhausted = rhs_mismatch == rhs.end();
  if (lhs_exhausted && rhs_exhausted)
    return std::strong_ordering::equivalent;
  if (lhs_exhausted)
    return std::strong_ordering::less;
  if (rhs_exhausted)
    return std::strong_ordering::greater;
  return *lhs_mismatch < *rhs_mismatch ? std::strong_ordering::less
                                       : std::strong_ordering::greater;
}

auto offset::get(const table_slice& slice) const noexcept
  -> std::pair<type, std::shared_ptr<arrow::Array>> {
  if (slice.rows() == 0)
    return {};
  return {
    caf::get<record_type>(slice.schema()).field(*this).type,
    get(*to_record_batch(slice)),
  };
}

auto offset::get(const arrow::RecordBatch& batch) const noexcept
  -> std::shared_ptr<arrow::Array> {
  return get(*batch.ToStructArray().ValueOrDie());
}

auto offset::get(const arrow::StructArray& struct_array) const noexcept
  -> std::shared_ptr<arrow::Array> {
  auto impl
    = [](auto&& impl, std::span<const offset::value_type> index,
         const arrow::StructArray& array) -> std::shared_ptr<arrow::Array> {
    VAST_ASSERT(not index.empty());
    auto field
      = array.GetFlattenedField(detail::narrow_cast<int>(index.front()))
          .ValueOrDie();
    index = index.subspan(1);
    if (index.empty())
      return field;
    return impl(impl, index, caf::get<arrow::StructArray>(*field));
  };
  return impl(impl, *this, struct_array);
}

} // namespace vast
