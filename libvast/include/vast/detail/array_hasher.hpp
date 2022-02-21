//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/address.hpp"
#include "vast/arrow_extension_types.hpp"
#include "vast/detail/generator.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/die.hpp"
#include "vast/hash/hash.hpp"

#include <arrow/array.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace vast::detail {

/// Applies a function over all non-null hash values. Since a null value
/// effectively adds an extra value to every domain, the function return value
/// is the number of null values that have been skipped.
///
/// We may consider rewriting this visitor as an Arrow Compute function at some
/// point.
struct array_hasher {
public:
  generator<uint64_t> operator()(const arrow::BooleanArray& xs) const {
    static const uint64_t false_digest = hash(0);
    static const uint64_t true_digest = hash(1);
    if (xs.false_count() > 0)
      co_yield uint64_t{false_digest};
    if (xs.true_count() > 0)
      co_yield uint64_t{true_digest};
  }

  // Overload that handles all stateless hash computations that only dependend
  // on the array value.
  template <class Array>
    requires is_any_v<Array, arrow::Int8Array, arrow::Int16Array,
                      arrow::Int32Array, arrow::Int64Array, arrow::UInt8Array,
                      arrow::UInt16Array, arrow::UInt32Array,
                      arrow::UInt64Array, arrow::HalfFloatArray,
                      arrow::FloatArray, arrow::DoubleArray, arrow::StringArray>
      generator<uint64_t>
  operator()(const Array& xs) const {
    for (auto i = 0; i < xs.length(); ++i) {
      if (!xs.IsNull(i)) {
        if constexpr (is_any_v<Array, arrow::Int8Array, arrow::Int16Array,
                               arrow::Int32Array, arrow::Int64Array>)
          co_yield hash(static_cast<int64_t>(xs.Value(i)));
        else if constexpr (is_any_v<Array, arrow::UInt8Array, arrow::UInt16Array,
                                    arrow::UInt32Array, arrow::UInt64Array>)
          co_yield hash(static_cast<uint64_t>(xs.Value(i)));
        else if constexpr (is_any_v<Array, arrow::HalfFloatArray,
                                    arrow::FloatArray, arrow::DoubleArray>)
          co_yield hash(static_cast<double>(xs.Value(i)));
        else if constexpr (std::is_same_v<Array, arrow::StringArray>)
          co_yield hash(as_bytes(xs.GetView(i)));
        else
          static_assert(always_false_v<Array>, "missing array type");
      }
    }
  }

  // Overload that handles types that have a pair form (x,y) where x in X and
  // y in Y. We can use a seeded hash to cover the cross product of X and Y but
  // treating x as seed.
  template <class Array>
    requires is_any_v<Array, arrow::DurationArray, arrow::TimestampArray>
      generator<uint64_t>
  operator()(const Array& xs) const {
    auto unit = arrow::TimeUnit::type{};
    if constexpr (std::is_same_v<Array, arrow::DurationArray>)
      unit = static_cast<const arrow::DurationType&>(*xs.type()).unit();
    else if constexpr (std::is_same_v<Array, arrow::TimestampArray>)
      unit = static_cast<const arrow::TimestampType&>(*xs.type()).unit();
    else
      static_assert(always_false_v<Array>, "missing array type");
    auto seed = static_cast<default_hash::seed_type>(unit);
    for (auto i = 0; i < xs.length(); ++i)
      if (!xs.IsNull(i))
        co_yield seeded_hash<default_hash>{seed}(xs.Value(i));
  }

  generator<uint64_t> operator()(const pattern_array& xs) const {
    const auto& ys = static_cast<const arrow::StringArray&>(*xs.storage());
    return (*this)(ys);
  }

  generator<uint64_t> operator()(const address_array& xs) const {
    const auto& ys
      = static_cast<const arrow::FixedSizeBinaryArray&>(*xs.storage());
    for (auto i = 0; i < xs.length(); ++i) {
      if (!xs.IsNull(i)) {
        auto bytes = as_bytes(ys.GetView(i));
        VAST_ASSERT(bytes.size() == 16);
        // Hash a fixed-size span because we'd otherwise hash the size as well.
        auto span = bytes.first<16>();
        co_yield hash(address{span});
      }
    }
  }

  generator<uint64_t> operator()(const subnet_array& xs) const {
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
        co_yield digest;
      }
    }
  }

  generator<uint64_t> operator()(const enum_array& xs) const {
    // Only hash the unique (string) values that we have encountered.
    const auto& ys = static_cast<const arrow::DictionaryArray&>(*xs.storage());
    return caf::visit(*this, *ys.dictionary());
  }

  generator<uint64_t> operator()(const arrow::ListArray& xs) const {
    // Lists are transparent for hashing.
    return caf::visit(*this, *xs.values());
  }

  generator<uint64_t> operator()(const arrow::MapArray& xs) const {
    const auto& base = static_cast<const arrow::ListArray&>(xs);
    const auto& kvps = static_cast<const arrow::StructArray&>(*base.values());
    // Treat keys and values independently.
    for (const auto& field : kvps.fields())
      for (auto x : caf::visit(*this, *field))
        co_yield x;
  }

  generator<uint64_t> operator()(const arrow::StructArray&) const {
    // In case there will be structs as first-class values at some point, we
    // will need to hash the cross product row-wise.
    die("structs cannot be accessed as top-level column");
  }

  generator<uint64_t> operator()(const arrow::Array& xs) const {
    return caf::visit(*this, xs);
  }
};

/// Convenience instantitation for easier use at call site.
constexpr auto hash_array = array_hasher{};

} // namespace vast::detail
