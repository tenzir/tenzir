//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/buffered_builder.hpp"

#include "vast/arrow_extension_types.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"
#include "vast/hash/hash.hpp"
#include "vast/sketch/sketch.hpp"
#include "vast/table_slice.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <cstdint>
#include <unordered_set>

namespace vast::sketch {

namespace {

/// Applies a function over all non-null hash values. Since a null value
/// effectively adds an extra value to every domain, the function return value
/// is the number of null values that have been skipped.
///
/// We may consider rewriting this visitor as an Arrow Compute function at some
/// point.
template <class Function>
struct array_hasher {
public:
  explicit array_hasher(Function& f) : f_{f} {
  }

  caf::expected<size_t> operator()(const arrow::NullArray&) {
    return caf::make_error(ec::unimplemented, "null type not supported");
  }

  caf::expected<size_t> operator()(const arrow::BooleanArray& xs) {
    if (xs.false_count() > 0)
      f_(0);
    if (xs.true_count() > 0)
      f_(1);
    return xs.null_count();
  }

  // Overload that handles all stateless hash computations that only dependend
  // on the array value.
  template <class Array>
    requires detail::is_any_v<
      Array, arrow::Int8Array, arrow::Int16Array, arrow::Int32Array,
      arrow::Int64Array, arrow::UInt8Array, arrow::UInt16Array,
      arrow::UInt32Array, arrow::UInt64Array, arrow::HalfFloatArray,
      arrow::FloatArray, arrow::DoubleArray, arrow::StringArray>
      caf::expected<size_t>
  operator()(const Array& xs) {
    for (auto i = 0; i < xs.length(); ++i) {
      if (!xs.IsNull(i)) {
        if constexpr (detail::is_any_v<Array, arrow::Int8Array,
                                       arrow::Int16Array, arrow::Int32Array,
                                       arrow::Int64Array>)
          f_(hash(static_cast<int64_t>(xs.Value(i))));
        else if constexpr (detail::is_any_v<
                             Array, arrow::UInt8Array, arrow::UInt16Array,
                             arrow::UInt32Array, arrow::UInt64Array>)
          f_(hash(static_cast<uint64_t>(xs.Value(i))));
        else if constexpr (detail::is_any_v<Array, arrow::HalfFloatArray,
                                            arrow::FloatArray,
                                            arrow::DoubleArray>)
          f_(hash(static_cast<double>(xs.Value(i))));
        else if constexpr (std::is_same_v<Array, arrow::StringArray>)
          f_(hash(as_bytes(xs.GetView(i))));
        else if constexpr (std::is_same_v<Array, arrow::StringArray>)
          f_(hash(as_bytes(xs.GetView(i))));
        else
          static_assert(detail::always_false_v<Array>, "missing array type");
      }
    }
    return xs.null_count();
  }

  // Overload that handles types that have a pair form (x,y) where x in X and
  // y in Y. We can use a seeded hash to cover the cross product of X and Y but
  // treating x as seed.
  template <class Array>
    requires detail::is_any_v<Array, arrow::DurationArray, arrow::TimestampArray>
      caf::expected<size_t>
  operator()(const Array& xs) {
    auto unit = arrow::TimeUnit::type{};
    if constexpr (std::is_same_v<Array, arrow::DurationArray>)
      unit = static_cast<const arrow::DurationType&>(*xs.type()).unit();
    else if constexpr (std::is_same_v<Array, arrow::TimestampArray>)
      unit = static_cast<const arrow::TimestampType&>(*xs.type()).unit();
    else
      static_assert(detail::always_false_v<Array>, "missing array type");
    auto seed = static_cast<default_hash::seed_type>(unit);
    for (auto i = 0; i < xs.length(); ++i)
      if (!xs.IsNull(i))
        f_(seeded_hash<default_hash>{seed}(xs.Value(i)));
    return xs.null_count();
  }

  caf::expected<size_t> operator()(const pattern_array& xs) {
    const auto& ys = static_cast<const arrow::StringArray&>(*xs.storage());
    return (*this)(ys);
  }

  caf::expected<size_t> operator()(const address_array& xs) {
    const auto& ys
      = static_cast<const arrow::FixedSizeBinaryArray&>(*xs.storage());
    for (auto i = 0; i < xs.length(); ++i) {
      if (!xs.IsNull(i)) {
        auto bytes = as_bytes(ys.GetView(i));
        VAST_ASSERT(bytes.size() == 16);
        // Hash a fixed-size span because we'd otherwise hash the size as well.
        auto span = bytes.first<16>();
        f_(hash(address{span}));
      }
    }
    return xs.null_count();
  }

  caf::expected<size_t> operator()(const subnet_array& xs) {
    // We treat the subnet length as seed to compute a one-pass hash.
    const auto& structs = static_cast<const arrow::StructArray&>(*xs.storage());
    const auto& lengths
      = static_cast<const arrow::UInt8Array&>(*structs.field(0));
    const auto& addrs = static_cast<const address_array&>(*structs.field(1));
    const auto& ys
      = static_cast<const arrow::FixedSizeBinaryArray&>(*addrs.storage());
    VAST_ASSERT(lengths.length() == ys.length());
    VAST_ASSERT(lengths.null_count() == ys.null_count());
    for (auto i = 0; i < ys.length(); ++i) {
      if (!ys.IsNull(i)) {
        auto seed = static_cast<default_hash::seed_type>(lengths.Value(i));
        auto bytes = as_bytes(ys.GetView(i));
        VAST_ASSERT(bytes.size() == 16);
        auto span = bytes.first<16>();
        auto digest = seeded_hash<default_hash>{seed}(span);
        f_(digest);
      }
    }
    return xs.null_count();
  }

  caf::expected<size_t> operator()(const enum_array& xs) {
    // Only hash the unique (string) values that we have encountered.
    const auto& ys = static_cast<const arrow::DictionaryArray&>(*xs.storage());
    return caf::visit(*this, *ys.dictionary());
  }

  caf::expected<size_t> operator()(const arrow::ListArray& xs) {
    // Lists are transparent for hashing.
    return caf::visit(*this, *xs.values());
  }

  caf::expected<size_t> operator()(const arrow::MapArray& xs) {
    const auto& base = static_cast<const arrow::ListArray&>(xs);
    const auto& kvps = static_cast<const arrow::StructArray&>(*base.values());
    // Treat keys and values independently.
    auto result = size_t{0};
    for (const auto& field : kvps.fields())
      if (auto x = caf::visit(*this, *field))
        result += *x;
      else
        return x.error();
    return result;
  }

  caf::expected<size_t> operator()(const arrow::StructArray&) {
    // In case there will be structs as first-class values at some point, we
    // will need to hash the cross product row-wise.
    return caf::make_error(ec::logic_error, "invalid flat index field access");
  }

  Function& f_;
};

} // namespace

caf::error buffered_builder::add(table_slice x, offset off) {
  auto record_batch = to_record_batch(x);
  const auto& layout = caf::get<record_type>(x.layout());
  auto idx = layout.flat_index(off);
  auto array = record_batch->column(idx);
  auto add = [this](uint64_t digest) {
    digests_.insert(digest);
  };
  auto nulls = caf::visit(array_hasher{add}, *array);
  if (!nulls)
    return caf::make_error(ec::unspecified, //
                           fmt::format("failed to hash table slice column {}: "
                                       "{}",
                                       idx, nulls.error()));
  // Treat null value as additional value in every domain.
  if (*nulls > 0)
    ; // FIXME: do something meaningful
  return caf::none;
}

caf::expected<sketch> buffered_builder::finish() {
  auto result = build(digests_);
  if (result)
    digests_.clear();
  return result;
}

} // namespace vast::sketch
