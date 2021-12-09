//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/buffered_builder.hpp"

#include "vast/error.hpp"
#include "vast/hash/hash.hpp"
#include "vast/sketch/sketch.hpp"
#include "vast/table_slice.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/visitor.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <cstdint>
#include <unordered_set>

namespace vast::sketch {

namespace {

/// Hashes all non-null array elements into a container.
template <class Container>
struct hash_visitor : public arrow::ArrayVisitor {
public:
  hash_visitor(Container& container) : digests_{container} {
  }

  arrow::Status Visit(const arrow::NullArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::BooleanArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::Int8Array& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::Int16Array& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::Int32Array& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::Int64Array& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::UInt8Array& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::UInt16Array& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::UInt32Array& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::UInt64Array& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::HalfFloatArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::FloatArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::DoubleArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::StringArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::BinaryArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::LargeStringArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::LargeBinaryArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::FixedSizeBinaryArray& array) final {
    return visit(array);
  }

  // arrow::Status Visit(const arrow::Date32Array& array) final;
  // arrow::Status Visit(const arrow::Date64Array& array) final;
  // arrow::Status Visit(const arrow::Time32Array& array) final;
  // arrow::Status Visit(const arrow::Time64Array& array) final;

  arrow::Status Visit(const arrow::TimestampArray& array) final {
    return visit(array);
  }

  // arrow::Status Visit(const arrow::DayTimeIntervalArray& array) final;
  // arrow::Status Visit(const arrow::MonthDayNanoIntervalArray& array) final;
  // arrow::Status Visit(const arrow::MonthIntervalArray& array) final;
  // arrow::Status Visit(const arrow::DurationArray& array) final;
  // arrow::Status Visit(const arrow::Decimal128Array& array) final;
  // arrow::Status Visit(const arrow::Decimal256Array& array) final;

  arrow::Status Visit(const arrow::ListArray& array) final {
    return visit(array);
  }

  arrow::Status Visit(const arrow::LargeListArray& array) final {
    return visit(array);
  }

  // arrow::Status Visit(const arrow::MapArray& array) final;
  // arrow::Status Visit(const arrow::FixedSizeListArray& array) final;

  arrow::Status Visit(const arrow::StructArray& array) final {
    return visit(array);
  }

  // arrow::Status Visit(const arrow::SparseUnionArray& array) final;
  // arrow::Status Visit(const arrow::DenseUnionArray& array) final;
  // arrow::Status Visit(const arrow::DictionaryArray& array) final;
  // arrow::Status Visit(const arrow::ExtensionArray& array) final;

private:
  template <class T>
  arrow::Status visit(T& array) {
    using type_class = typename T::TypeClass;
    if (array.null_count() > 0)
      // TODO: treat NULL as a first-class value in all array types.
      ;
    if constexpr (std::is_same_v<type_class, arrow::NullType>) {
      return arrow::Status::TypeError("null type not supported");
    } else if constexpr (std::is_same_v<type_class, arrow::BooleanType>) {
      // Map false to 0 and true to 1.
      if (array.false_count() > 0)
        digests_.insert(0);
      if (array.true_count() > 0)
        digests_.insert(1);
    } else {
      for (auto i = 0; i < array.length(); ++i) {
        if (!array.IsNull(i)) {
          auto digest = uint64_t{0};
          if constexpr (detail::is_any_v<type_class, arrow::Int8Type,
                                         arrow::Int16Type, arrow::Int32Type,
                                         arrow::Int64Type>) {
            digest = hash(int64_t{array.Value(i)});
          } else if constexpr (detail::is_any_v<
                                 type_class, arrow::UInt8Type, arrow::UInt16Type,
                                 arrow::UInt32Type, arrow::UInt64Type>) {
            digest = hash(uint64_t{array.Value(i)});
          } else if constexpr (detail::is_any_v<type_class, arrow::HalfFloatType,
                                                arrow::FloatType,
                                                arrow::DoubleType>) {
            digest = hash(static_cast<double>(array.Value(i)));
          } else if constexpr (std::is_same_v<type_class, arrow::TimestampType>) {
            const auto& ts
              = static_cast<const arrow::TimestampType&>(*array.type());
            auto seed = static_cast<default_hash::seed_type>(ts.unit());
            digest = seeded_hash<default_hash>{seed}(array.Value(i));
          } else if constexpr (std::is_same_v<type_class,
                                              arrow::FixedSizeBinaryType>) {
            // We cover IP addresses and subnets using this type currently.
            // (These should be extension types in the future.)
            auto bytes = as_bytes(array.GetView(i));
            if (bytes.size() == 16) {
              // An IP address is uniquely represented and therefore hashable as
              // a single block of 16 bytes. A fixed-size span gets also hashed
              // as single block, which we use here.
              auto addr = address::v6(bytes.template first<16>());
              digest = hash(addr);
            } else if (bytes.size() == 17) {
              auto addr = address::v6(bytes.template first<16>());
              auto length = static_cast<uint8_t>(bytes[16]);
              digest = hash(subnet{addr, length});
            } else {
              return arrow::Status::TypeError("unsupported type", array.type());
            }
          } else if constexpr (detail::is_any_v<type_class, arrow::ListType,
                                                arrow::LargeListType>) {
            // We are considering a flattened version of the array and recurse
            // into the individual lists.
            auto status = array.values()->Accept(this);
            if (!status.ok())
              return status;
          } else if constexpr (std::is_same_v<type_class, arrow::StructType>) {
            // TODO: implement
            return arrow::Status::TypeError("nested records not yet supported");
          } else {
            // Handle an arrow::util::string_view.
            digest = hash(as_bytes(array.GetView(i)));
          }
          digests_.insert(digest);
        }
      }
    }
    return arrow::Status::OK();
  }

  Container& digests_;
};

} // namespace

caf::error buffered_builder::add(table_slice x, offset off) {
  auto record_batch = to_record_batch(x); // FIXME: do not assume Arrow.
  const auto& layout = caf::get<record_type>(x.layout());
  auto idx = layout.flat_index(off);
  auto array = record_batch->column(idx);
  hash_visitor visitor{digests_};
  if (auto status = array->Accept(&visitor); !status.ok())
    caf::make_error(ec::unspecified, //
                    fmt::format("failed to hash table slice column {}: {}", idx,
                                status.ToString()));
  return {};
}

caf::expected<sketch> buffered_builder::finish() {
  auto result = build(digests_);
  if (result)
    digests_.clear();
  return result;
}

} // namespace vast::sketch
