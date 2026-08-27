//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/try.hpp"

#include <arrow/api.h>
#include <arrow/util/bit_run_reader.h>
#include <arrow/util/bitmap_ops.h>

namespace tenzir {

namespace {

auto contains_extension_type(data_view2 x) -> bool {
  return match(
    x,
    [](const view<record>& x) {
      for (auto y : x) {
        if (contains_extension_type(y.second)) {
          return true;
        }
      }
      return false;
    },
    [](const view<list>& x) {
      for (auto y : x) {
        if (contains_extension_type(y)) {
          return true;
        }
      }
      return false;
    },
    []<class T>(const T&) -> bool {
      using data_type = materialize_t<T>;
      if constexpr (concepts::one_of<data_type, map, pattern>) {
        TENZIR_UNREACHABLE();
      } else {
        return extension_type<data_to_type_t<data_type>>;
      }
    });
}

} // namespace

auto data_to_series(data_view2 value, int64_t length) -> series {
  TENZIR_ASSERT(length >= 0);
  if (is<caf::none_t>(value)) {
    return series::null(null_type{}, length);
  }
  auto b = series_builder{};
  if (contains_extension_type(value)) {
    // We currently cannot convert extension types to scalars, so we append one
    // row at a time. An empty series still needs one row to infer the type.
    for (auto i = int64_t{0}; i < std::max(length, int64_t{1}); ++i) {
      b.data(value);
    }
    auto s = b.finish_assert_one_array();
    return length == 0 ? s.slice(0, 0) : s;
  }
  // Build the value once and replicate the resulting scalar.
  b.data(value);
  auto s = b.finish_assert_one_array();
  return series{
    std::move(s.type),
    check(arrow::MakeArrayFromScalar(*check(s.array->GetScalar(0)), length,
                                     tenzir::arrow_memory_pool())),
  };
}

arrow::Status
append_builder(const null_type&, type_to_arrow_builder_t<null_type>& builder,
               const view<type_to_data_t<null_type>>& view) noexcept {
  (void)view;
  return builder.AppendNull();
}

arrow::Status
append_builder(const bool_type&, type_to_arrow_builder_t<bool_type>& builder,
               const view<type_to_data_t<bool_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const int64_type&, type_to_arrow_builder_t<int64_type>& builder,
               const view<type_to_data_t<int64_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const uint64_type&,
               type_to_arrow_builder_t<uint64_type>& builder,
               const view<type_to_data_t<uint64_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const double_type&,
               type_to_arrow_builder_t<double_type>& builder,
               const view<type_to_data_t<double_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const duration_type&,
               type_to_arrow_builder_t<duration_type>& builder,
               const view<type_to_data_t<duration_type>>& view) noexcept {
  return builder.Append(view.count());
}

arrow::Status
append_builder(const time_type&, type_to_arrow_builder_t<time_type>& builder,
               const view<type_to_data_t<time_type>>& view) noexcept {
  return builder.Append(view.time_since_epoch().count());
}

arrow::Status
append_builder(const string_type&,
               type_to_arrow_builder_t<string_type>& builder,
               const view<type_to_data_t<string_type>>& view) noexcept {
  return builder.Append(std::string_view{view.data(), view.size()});
}

arrow::Status
append_builder(const blob_type&, type_to_arrow_builder_t<blob_type>& builder,
               const view<type_to_data_t<blob_type>>& view) noexcept {
  return builder.Append(
    std::string_view{reinterpret_cast<const char*>(view.data()), view.size()});
}

arrow::Status
append_builder(const ip_type&, type_to_arrow_builder_t<ip_type>& builder,
               const view<type_to_data_t<ip_type>>& view) noexcept {
  const auto bytes = as_bytes(view);
  TENZIR_ASSERT_EXPENSIVE(bytes.size() == 16);
  return builder.Append(std::string_view{
    reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

arrow::Status
append_builder(const subnet_type&,
               type_to_arrow_builder_t<subnet_type>& builder,
               const view<type_to_data_t<subnet_type>>& view) noexcept {
  if (auto status = builder.Append(); not status.ok()) {
    return status;
  }
  if (auto status
      = append_builder(ip_type{}, builder.ip_builder(), view.network());
      not status.ok()) {
    return status;
  }
  return builder.length_builder().Append(view.length());
}

arrow::Status
append_builder(const secret_type&,
               type_to_arrow_builder_t<secret_type>& builder,
               const view<type_to_data_t<secret_type>>& view) noexcept {
  TRY(builder.Append());
  TENZIR_ASSERT(view.buffer.chunk());
  TRY(builder.buffer_builder().Append(
    reinterpret_cast<const char*>(view.buffer.chunk()->data()),
    view.buffer.chunk()->size()));
  return arrow::Status::OK();
}

arrow::Status
append_builder(const enumeration_type&,
               type_to_arrow_builder_t<enumeration_type>& builder,
               const view<type_to_data_t<enumeration_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const list_type& hint,
               type_to_arrow_builder_t<list_type>& builder,
               const view<type_to_data_t<list_type>>& view) noexcept {
  if (auto status = builder.Append(); not status.ok()) {
    return status;
  }
  auto append_values = [&](const concrete_type auto& value_type) noexcept {
    auto& value_builder = *builder.value_builder();
    for (const auto& value_view : view) {
      if (auto status = append_builder(value_type, value_builder, value_view);
          not status.ok()) {
        return status;
      }
    }
    return arrow::Status::OK();
  };
  return match(hint.value_type(), append_values);
}

arrow::Status
append_builder(const map_type& hint, type_to_arrow_builder_t<map_type>& builder,
               const view<type_to_data_t<map_type>>& view) noexcept {
  if (auto status = builder.Append(); not status.ok()) {
    return status;
  }
  auto append_values = [&](const concrete_type auto& key_type,
                           const concrete_type auto& item_type) noexcept {
    auto& key_builder = *builder.key_builder();
    auto& item_builder = *builder.item_builder();
    for (const auto& [key_view, item_view] : view) {
      if (auto status = append_builder(key_type, key_builder, key_view);
          not status.ok()) {
        return status;
      }
      if (auto status = append_builder(item_type, item_builder, item_view);
          not status.ok()) {
        return status;
      }
    }
    return arrow::Status::OK();
  };
  return match(std::tuple{hint.key_type(), hint.value_type()}, append_values);
}

arrow::Status
append_builder(const record_type& hint,
               type_to_arrow_builder_t<record_type>& builder,
               const view<type_to_data_t<record_type>>& view) noexcept {
  if (auto status = builder.Append(); not status.ok()) {
    return status;
  }
  for (int index = 0; const auto& [_, field_type] : hint.fields()) {
    if (auto status = append_builder(field_type, *builder.field_builder(index),
                                     view->at(index).second);
        not status.ok()) {
      return status;
    }
    ++index;
  }
  return arrow::Status::OK();
}

arrow::Status append_builder(const type& hint,
                             std::same_as<arrow::ArrayBuilder> auto& builder,
                             const view<type_to_data_t<type>>& value) noexcept {
  if (is<caf::none_t>(value)) {
    return builder.AppendNull();
  }
  auto f = [&]<concrete_type Type>(const Type& hint) {
    return append_builder(hint, as<type_to_arrow_builder_t<Type>>(builder),
                          as<view<type_to_data_t<Type>>>(value));
  };
  return match(hint, f);
}

template <concrete_type Ty>
auto append_array_slice(type_to_arrow_builder_t<Ty>& builder, const Ty& ty,
                        const type_to_arrow_array_t<Ty>& array, int64_t begin,
                        int64_t count) -> arrow::Status {
  TENZIR_ASSERT(0 <= begin);
  auto end = begin + count;
  TENZIR_ASSERT(end <= array.length());
  TRY(builder.Reserve(count));
  if constexpr (arrow::is_extension_type<type_to_arrow_type_t<Ty>>::value) {
    // TODO: `AppendArraySlice(...)` throws a `std::bad_cast` with extension
    // types (Arrow 13.0.0). Hence, we have to use some custom logic here.
    for (auto row = begin; row < end; ++row) {
      if (array.IsNull(row)) {
        TRY(builder.AppendNull());
      } else {
        TRY(append_builder(ty, builder, *view_at<Ty>(array, row)));
      }
    }
  } else if constexpr (std::same_as<Ty, record_type>) {
    TENZIR_ASSERT(detail::narrow<size_t>(builder.num_fields())
                  == ty.num_fields());
    TENZIR_ASSERT(array.num_fields() == builder.num_fields());
    for (auto row = begin; row < end; ++row) {
      TRY(builder.Append(array.IsValid(row)));
    }
    for (auto field = 0; field < builder.num_fields(); ++field) {
      TRY(append_array_slice(*builder.field_builder(field),
                             ty.field(field).type, *array.field(field), begin,
                             count));
    }
  } else if constexpr (std::same_as<Ty, list_type>) {
    for (auto row = begin; row < end; ++row) {
      auto valid = array.IsValid(row);
      TRY(builder.Append(valid));
      if (valid) {
        auto list_begin = array.value_offset(row);
        auto list_end = array.value_offset(row + 1);
        TRY(append_array_slice(*builder.value_builder(), ty.value_type(),
                               *array.values(), list_begin,
                               list_end - list_begin));
      }
    }
  } else if constexpr (std::same_as<Ty, map_type>) {
    TENZIR_UNREACHABLE();
  } else {
    static_assert(basic_type<Ty>);
    TRY(builder.AppendArraySlice(*array.data(), begin, count));
  }
  return arrow::Status::OK();
}

auto append_array_slice(arrow::ArrayBuilder& builder, const type& ty,
                        const arrow::Array& array, int64_t begin, int64_t count)
  -> arrow::Status {
  return match(ty, [&]<class Ty>(const Ty& ty) {
    return append_array_slice(as<type_to_arrow_builder_t<Ty>>(builder), ty,
                              as<type_to_arrow_array_t<Ty>>(array), begin,
                              count);
  });
}

// Make sure that `append_array_slice<...>` is emitted for every type.
template <std::monostate>
struct instantiate_append_array_slice {
  template <class... T>
  struct inner {
    static constexpr auto value = std::tuple{&append_array_slice<T>...};
  };

  static constexpr auto value
    = detail::tl_apply_t<concrete_types, inner>::value;
};

template struct instantiate_append_array_slice<std::monostate{}>;

namespace {

/// A bitmap in which a null of the source mask reads as `false`, so that run
/// detection needs to look at a single bitmap only.
struct EffectiveBitmap {
  const uint8_t* data = nullptr;
  int64_t offset = 0;
  /// Keeps `data` alive when the validity had to be folded in.
  std::shared_ptr<arrow::Buffer> owned = nullptr;
};

/// Folds the validity of `mask` into its values.
///
/// Without nulls this borrows the value bitmap. With nulls it materializes the
/// conjunction once, which costs one bitmap allocation but keeps the scan
/// below word-wise instead of falling back to bit-by-bit.
auto to_effective_bitmap(const arrow::BooleanArray& mask) -> EffectiveBitmap {
  const auto& data = *mask.data();
  // Bitmaps carry their offset in bits, so the pointers must stay unadjusted.
  const auto* values = data.GetValues<uint8_t>(1, 0);
  if (mask.null_count() == 0) {
    return {values, data.offset, nullptr};
  }
  const auto* validity = data.GetValues<uint8_t>(0, 0);
  auto buffer = check(arrow::internal::BitmapAnd(arrow_memory_pool(), validity,
                                                 data.offset, values,
                                                 data.offset, data.length, 0));
  return {buffer->data(), 0, std::move(buffer)};
}

/// Iterates contiguous runs in a boolean mask, calling fn(begin, end, value)
/// for each run. A null counts as `false`.
///
/// Arrow's `BitRunReader` scans a machine word at a time instead of polling
/// every single bit, which matters because callers use the runs to replace
/// per-row appends with bulk slice appends.
template <typename F>
void for_each_run(const arrow::BooleanArray& mask, F&& fn) {
  auto len = mask.length();
  if (len == 0) {
    return;
  }
  auto bitmap = to_effective_bitmap(mask);
  auto reader = arrow::internal::BitRunReader{bitmap.data, bitmap.offset, len};
  auto begin = int64_t{0};
  while (begin < len) {
    auto run = reader.NextRun();
    TENZIR_ASSERT(run.length > 0);
    auto end = begin + run.length;
    fn(begin, end, run.set);
    begin = end;
  }
}

} // namespace

auto mask_runs(const arrow::BooleanArray& mask, size_t limit)
  -> Option<std::vector<MaskRun>> {
  auto len = mask.length();
  auto result = std::vector<MaskRun>{};
  if (len == 0) {
    return result;
  }
  result.reserve(std::min(limit, static_cast<size_t>(len)));
  auto bitmap = to_effective_bitmap(mask);
  auto reader = arrow::internal::BitRunReader{bitmap.data, bitmap.offset, len};
  auto begin = int64_t{0};
  while (begin < len) {
    if (result.size() == limit) {
      return None{};
    }
    auto run = reader.NextRun();
    TENZIR_ASSERT(run.length > 0);
    auto end = begin + run.length;
    result.push_back({begin, end, run.set});
    begin = end;
  }
  return result;
}

auto partition_array(arrow::ArrayBuilder& true_builder,
                     arrow::ArrayBuilder& false_builder, const type& ty,
                     const arrow::Array& array, const arrow::BooleanArray& mask)
  -> void {
  TENZIR_ASSERT(array.length() == mask.length());
  auto true_count = mask.true_count();
  auto false_count = mask.length() - true_count;
  match(ty, [&]<class Ty>(const Ty& ty) {
    auto* typed_array_ptr
      = dynamic_cast<const type_to_arrow_array_t<Ty>*>(&array);
    TENZIR_ASSERT(typed_array_ptr);
    auto& typed_array = *typed_array_ptr;
    if constexpr (std::same_as<Ty, record_type>) {
      auto* tbp = dynamic_cast<arrow::StructBuilder*>(&true_builder);
      auto* fbp = dynamic_cast<arrow::StructBuilder*>(&false_builder);
      TENZIR_ASSERT(tbp);
      TENZIR_ASSERT(fbp);
      auto& tb = *tbp;
      auto& fb = *fbp;
      check(tb.Reserve(true_count));
      check(fb.Reserve(false_count));
      for (auto i = int64_t{0}; i < mask.length(); ++i) {
        auto& b = (mask.IsValid(i) and mask.Value(i)) ? tb : fb;
        check(b.Append(typed_array.IsValid(i)));
      }
      for (auto field = 0; field < tb.num_fields(); ++field) {
        partition_array(*tb.field_builder(field), *fb.field_builder(field),
                        ty.field(field).type, *typed_array.field(field), mask);
      }
    } else if constexpr (std::same_as<Ty, list_type>) {
      auto* tbp = dynamic_cast<arrow::ListBuilder*>(&true_builder);
      auto* fbp = dynamic_cast<arrow::ListBuilder*>(&false_builder);
      TENZIR_ASSERT(tbp);
      TENZIR_ASSERT(fbp);
      auto& tb = *tbp;
      auto& fb = *fbp;
      check(tb.Reserve(true_count));
      check(fb.Reserve(false_count));
      for (auto i = int64_t{0}; i < mask.length(); ++i) {
        auto& b = (mask.IsValid(i) and mask.Value(i)) ? tb : fb;
        auto valid = typed_array.IsValid(i);
        check(b.Append(valid));
        if (valid) {
          auto list_begin = typed_array.value_offset(i);
          auto list_end = typed_array.value_offset(i + 1);
          check(append_array_slice(*b.value_builder(), type{ty.value_type()},
                                   *typed_array.values(), list_begin,
                                   list_end - list_begin));
        }
      }
    } else if constexpr (std::same_as<Ty, map_type>) {
      TENZIR_UNREACHABLE();
    } else {
      auto* tbp = dynamic_cast<type_to_arrow_builder_t<Ty>*>(&true_builder);
      auto* fbp = dynamic_cast<type_to_arrow_builder_t<Ty>*>(&false_builder);
      TENZIR_ASSERT(tbp);
      TENZIR_ASSERT(fbp);
      auto& tb = *tbp;
      auto& fb = *fbp;
      check(tb.Reserve(true_count));
      check(fb.Reserve(false_count));
      if constexpr (std::same_as<Ty, string_type>
                    or std::same_as<Ty, blob_type>) {
        auto true_bytes = int64_t{0};
        auto false_bytes = int64_t{0};
        for (auto i = int64_t{0}; i < mask.length(); ++i) {
          auto bytes = typed_array.value_length(i);
          if (mask.IsValid(i) and mask.Value(i)) {
            true_bytes += bytes;
          } else {
            false_bytes += bytes;
          }
        }
        check(tb.ReserveData(true_bytes));
        check(fb.ReserveData(false_bytes));
      }
      for_each_run(mask, [&](int64_t begin, int64_t end, bool value) {
        auto& b = value ? tb : fb;
        check(append_array_slice(b, ty, typed_array, begin, end - begin));
      });
    }
  });
}

} // namespace tenzir
