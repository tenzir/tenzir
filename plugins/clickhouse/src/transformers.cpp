//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "transformers.hpp"

#include "clickhouse/columns/tuple.h"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/view3.hpp"

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

using namespace clickhouse;
using namespace std::string_view_literals;

namespace tenzir::plugins::clickhouse {

transformer_record::transformer_record(std::string clickhouse_typename,
                                       schema_transformations transformations)
  : transformer{std::move(clickhouse_typename), true},
    transformations{std::move(transformations)} {
  for (const auto& [_, t] : transformations) {
    if (not t->clickhouse_nullable) {
      clickhouse_nullable = false;
      break;
    }
  }
  found_column.resize(this->transformations.size());
}

auto transformer_record::update_dropmask(
  const tenzir::type& type, const arrow::Array& array, dropmask_ref dropmask,
  tenzir::diagnostic_handler& dh) -> drop {
  if (clickhouse_nullable) {
    return drop::none;
  }
  if (not array.null_bitmap()) {
    return drop::none;
  }
  if (array.null_count() == 0) {
    return drop::none;
  }
  /// Update the dropmask based of the record itself. If we are here, we know
  /// that we cannot null every subcolumn, so a "top level" null requires us
  /// to drop the event.
  for (int64_t i = 0; i < array.length(); ++i) {
    if (array.IsNull(i)) {
      dropmask[i] = true;
    }
  }
  const auto& rt = as<record_type>(type);
  const auto& struct_array = as<arrow::StructArray>(array);
  auto updated = drop::none;
  /// Update the dropmasks from all nested columns
  for (auto [k, t, arr] : columns_of(rt, struct_array)) {
    const auto [trafo, out_idx] = transfrom_and_index_for(k);
    if (not trafo) {
      diagnostic::warning(
        "nested column `{}` does not exist in ClickHouse table ", k)
        .note("column will be dropped")
        .emit(dh);
      continue;
    }
    found_column[out_idx] = true;
    // TODO we can potentially discover a "drop::all" here
    updated = updated | trafo->update_dropmask(t, arr, dropmask, dh);
  }
  /// Detect missing columns
  for (const auto& [i, kvp] : detail::enumerate(transformations)) {
    if (found_column[i]) {
      continue;
    }
    if (kvp.second->clickhouse_nullable) {
      continue;
    }
    diagnostic::warning(
      "required column missing in input, event will be dropped")
      .note("column `{}` is missing", kvp.first)
      .emit(dh);
    std::ranges::fill(dropmask, true);
    updated = drop::all;
    break;
  }
  return updated;
}

auto transformer_record::create_null_column(size_t n) const
  -> ::clickhouse::ColumnRef {
  if (not clickhouse_nullable) {
    return nullptr;
  }
  auto columns = std::vector<ColumnRef>(transformations.size());
  for (const auto& [_, t] : transformations) {
    auto c = t->create_null_column(n);
    if (not c) {
      return nullptr;
    }
    columns.push_back(std::move(c));
  }
  return std::make_shared<ColumnTuple>(std::move(columns));
}

auto transformer_record::create_column(
  const tenzir::type& type, const arrow::Array& array, dropmask_cref dropmask,
  tenzir::diagnostic_handler& dh) const -> ::clickhouse::ColumnRef {
  auto columns = std::vector<ColumnRef>(transformations.size());
  const auto& rt = as<record_type>(type);
  const auto& struct_array = as<arrow::StructArray>(array);
  for (auto [k, t, arr] : columns_of(rt, struct_array)) {
    const auto [trafo, out_idx] = transfrom_and_index_for(k);
    if (not trafo) {
      continue;
    }
    auto this_column = trafo->create_column(t, arr, dropmask, dh);
    // TODO: re-evaluate this
    TENZIR_ASSERT(this_column);
    columns[out_idx] = std::move(this_column);
  }
  return std::make_shared<ColumnTuple>(std::move(columns));
}

auto transformer_record::transfrom_and_index_for(std::string_view name) const
  -> find_result {
  const auto it = transformations.find(name);
  if (it == transformations.end()) {
    return {nullptr, 0};
  }
  return {&*it->second, std::distance(transformations.begin(), it)};
}

auto remove_non_significant_whitespace(std::string_view str) -> std::string {
  std::string ret;
  ret.reserve(str.size());
  auto can_skip = false;
  constexpr static auto syntax_characters = "(),"sv;
  for (size_t i = 0; i < str.size(); ++i) {
    const auto is_space = std::isspace(str[i]);
    if (can_skip and std::isspace(str[i])) {
      continue;
    }
    ret += str[i];
    const auto is_syntax
      = syntax_characters.find(str[i]) != std::string_view::npos;
    can_skip = is_space or is_syntax;
    // Remove space *before* the current syntax token. Handles e.g. `text )`
    if (is_syntax and i > 0 and std::isspace(str[i - 1])) {
      ret.pop_back();
    }
  }
  return ret;
}

namespace {

auto value_transform(auto v) {
  return v;
}

auto value_transform(tenzir::time v) -> int64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
           v.time_since_epoch())
    .count();
}

auto value_transform(tenzir::duration v) -> int64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(v).count();
}

auto value_transform(tenzir::ip v) -> in6_addr {
  auto res = in6_addr{};
  // TODO: I feel dirty. Please clean this.
  std::memcpy(&res, &v, sizeof(v));
  return res;
}

auto value_transform(tenzir::subnet v) -> std::tuple<in6_addr, uint8_t> {
  auto res = std::tuple<in6_addr, uint8_t>{};
  std::memcpy(&std::get<0>(res), &v.network(), sizeof(tenzir::ip));
  std::get<1>(res) = v.length();
  return res;
}

template <typename>
struct tenzir_to_clickhouse_trait;

#define X(TENZIR_TYPENAME, CLICKHOUSE_COLUMN, CLICKHOUSE_NAME)                 \
  template <>                                                                  \
  struct tenzir_to_clickhouse_trait<TENZIR_TYPENAME> {                         \
    constexpr static std::string_view name = CLICKHOUSE_NAME;                  \
    using column_type = CLICKHOUSE_COLUMN;                                     \
                                                                               \
    constexpr static auto null_value = std::nullopt;                           \
                                                                               \
    static auto clickhouse_typename(bool nullable) -> std::string {            \
      if (nullable) {                                                          \
        return std::string{"Nullable("}.append(name).append(1, ')');           \
      }                                                                        \
      return std::string{name};                                                \
    }                                                                          \
                                                                               \
    template <bool nullable>                                                   \
    static auto allocate(size_t n) {                                           \
      using Column_Type                                                        \
        = std::conditional_t<nullable, ColumnNullableT<column_type>,           \
                             column_type>;                                     \
      auto res = std::make_shared<Column_Type>();                              \
      res->Reserve(n);                                                         \
      return res;                                                              \
    }                                                                          \
  }

X(int64_type, ColumnInt64, "Int64");
X(uint64_type, ColumnUInt64, "UInt64");
X(double_type, ColumnFloat64, "Float64");
X(string_type, ColumnString, "String");
X(duration_type, ColumnInt64, "Int64");
X(ip_type, ColumnIPv6, "IPv6");
#undef X

template <>
struct tenzir_to_clickhouse_trait<time_type> {
  constexpr static std::string_view name = "DateTime64(9)";
  using column_type = ColumnDateTime64;

  constexpr static auto null_value = std::nullopt;

  static auto clickhouse_typename(bool nullable) -> std::string {
    if (nullable) {
      return std::string{"Nullable("}.append(name).append(")");
    }
    return std::string{name};
  }

  template <bool nullable>
  static auto allocate(size_t n) {
    using Column_Type
      = std::conditional_t<nullable, ColumnNullableT<column_type>, column_type>;
    auto res = std::make_shared<Column_Type>(9);
    res->Reserve(n);
    return res;
  }
};

template <>
struct tenzir_to_clickhouse_trait<subnet_type> {
  static auto clickhouse_typename(bool nullable) -> std::string {
    if (nullable) {
      return "Tuple(ip Nullable(IPv6),length Nullable(UInt8))";
    }
    return "Tuple(ip IPv6,length UInt8)";
  }

  constexpr static auto null_value = std::tuple{std::nullopt, std::nullopt};

  template <bool nullable>
  static auto allocate(size_t n) {
    using ip_t
      = std::conditional_t<nullable, ColumnNullableT<ColumnIPv6>, ColumnIPv6>;
    using length_t
      = std::conditional_t<nullable, ColumnNullableT<ColumnUInt8>, ColumnUInt8>;
    using Column_Type = ColumnTupleT<ip_t, length_t>;
    auto ip_c = std::make_shared<ip_t>();
    ip_c->Reserve(n);
    auto length_c = std::make_shared<length_t>();
    length_c->Reserve(n);
    auto res = std::make_shared<Column_Type>(
      std::tuple{std::move(ip_c), std::move(length_c)});
    return res;
  }
};

template <typename Expected, typename Actual>
concept convertible_hack = std::same_as<Expected, Actual>
                           or (std::same_as<Expected, int64_type>
                               and std::same_as<Actual, duration_type>);

template <typename T, bool Nullable>
  requires requires { tenzir_to_clickhouse_trait<T>{}; }
struct transformer_from_trait : transformer {
  using traits = tenzir_to_clickhouse_trait<T>;

  transformer_from_trait()
    : transformer{traits::clickhouse_typename(Nullable), Nullable} {
  }

  virtual auto update_dropmask(const tenzir::type&, const arrow::Array& array,
                               dropmask_ref dropmask,
                               tenzir::diagnostic_handler&) -> drop override {
    if constexpr (Nullable) {
      return drop::none;
    }
    if (not array.null_bitmap()) {
      return drop::none;
    }
    if (array.null_count() == 0) {
      return drop::none;
    }
    // TODO we can potentially discover a "drop::all" here
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        dropmask[i] = true;
      }
    }
    return drop::some;
  }

  virtual auto
  create_null_column(size_t n) const -> ::clickhouse::ColumnRef override {
    if constexpr (Nullable) {
      auto columns = traits::template allocate<Nullable>(n);
      for (size_t i = 0; i < n; ++n) {
        columns->Append(traits::null_value);
      }
    }
    return nullptr;
  }

  virtual auto create_column(
    const tenzir::type& type, const arrow::Array& array, dropmask_cref dropmask,
    tenzir::diagnostic_handler& dh) const -> ::clickhouse::ColumnRef override {
    const auto f = detail::overload{
      [&](const null_type&) -> std::shared_ptr<Column> {
        if constexpr (Nullable) {
          auto column = traits::template allocate<Nullable>(array.length());
          for (int64_t i = 0; i < array.length(); ++i) {
            column->Append(traits::null_value);
          }
          return column;
        }
        return nullptr;
      },
      [&]<typename U>(const U&) -> std::shared_ptr<Column> {
        // error case. Potentially do more conversions?
        diagnostic::warning("incompatible data")
          .note("expected `{}`, got `{}`\n", tenzir::type_kind{tag_v<T>},
                tenzir::type_kind{tag_v<U>})
          .emit(dh);
        return nullptr;
      },
      [&]<typename U>(const U&) -> std::shared_ptr<Column>
        requires convertible_hack<T, U>
      {
        auto column = traits::template allocate<Nullable>(array.length());
        auto cast_array = dynamic_cast<const type_to_arrow_array_t<U>*>(&array);
        TENZIR_ASSERT(cast_array);
        for (int64_t i = 0; i < cast_array->length(); ++i) {
          if (dropmask[i]) {
            continue;
          }
          auto v = view_at(*cast_array, i);
          if constexpr (Nullable) {
            if (not v) {
              column->Append(traits::null_value);
              continue;
            }
          }
          column->Append(value_transform(*v));
        }
        return column;
      },
      };
    return match(type, f);
  }
};

template <typename T>
auto make_transformer_impl(bool nullable) -> std::unique_ptr<transformer> {
  if (nullable) {
    return std::make_unique<transformer_from_trait<T, true>>();
  } else {
    return std::make_unique<transformer_from_trait<T, false>>();
  }
}

struct transformer_array : transformer {
  std::unique_ptr<transformer> data_transform;
  dropmask_type my_mask;
  const arrow::Array* my_value_array;

  transformer_array(std::string clickhouse_typename,
                    std::unique_ptr<transformer> data_transform)
    : transformer{std::move(clickhouse_typename),
                  data_transform->clickhouse_nullable},
      data_transform{std::move(data_transform)} {
  }

  virtual auto
  update_dropmask(const tenzir::type& type, const arrow::Array& array,
                  dropmask_ref dropmask,
                  tenzir::diagnostic_handler& dh) -> drop override {
    const auto value_type = as<list_type>(type).value_type();
    const auto& list_array = as<arrow::ListArray>(array);
    const auto& value_array = *list_array.values();
    const auto& offsets
      = static_cast<arrow::Int32Array&>(*list_array.offsets());
    my_mask.clear();
    my_mask.resize(value_array.length(), false);
    my_value_array = &value_array;
    // These "early returns MUST happen after we updated `my_mask`"
    if (clickhouse_nullable) {
      return drop::none;
    }
    if (not array.null_bitmap()) {
      return drop::none;
    }
    if (array.null_count() == 0) {
      return drop::none;
    }
    auto updated
      = data_transform->update_dropmask(value_type, value_array, my_mask, dh);
    if (updated == drop::none) {
      return drop::none;
    }
    /// TODO: we may be able to discover a `drop::all` here
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        dropmask[i] = true;
        updated = drop::some;
        continue;
      }
      const auto start = offsets.Value(i);
      const auto end = offsets.Value(i + 1);
      auto event_has_null = false;
      for (int64_t j = start; j < end; ++j) {
        event_has_null |= my_mask[j];
      }
      dropmask[i] |= event_has_null;
      if (event_has_null) {
        updated = drop::some;
      }
    }
    return updated;
  }

  static auto
  make_offsets(const arrow::Int32Array& input) -> std::shared_ptr<ColumnUInt64> {
    auto res = std::make_shared<ColumnUInt64>();
    auto& output = res->GetWritableData();
    output.resize(input.length() - 1);
    // arrow offsets are [ start1 , end1/start2, ... ]
    // clickhouse offsets are [end1, end2, ...]
    // See e.g. `::clickhouse::ColumnArray::GetSize`
    for (int64_t i = 0; i < input.length() - 1; ++i) {
      auto start = input.GetView(i);
      auto end = input.GetView(i + 1);
      auto size = end - start;
      output[i] = start + size;
    }
    return res;
  }

  virtual auto
  create_null_column(size_t n) const -> ::clickhouse::ColumnRef override {
    if (not clickhouse_nullable) {
      return nullptr;
    }
    auto column = data_transform->create_null_column(n);
    if (not column) {
      return nullptr;
    }
    auto column_offsets = std::make_shared<ColumnUInt64>();
    auto& output = column_offsets->GetWritableData();
    output.resize(n);
    for (size_t i = 0; i < n; ++i) {
      output[i] = i;
    }
    return std::make_shared<ColumnArray>(std::move(column),
                                         std::move(column_offsets));
  }

  virtual auto create_column(
    const tenzir::type& type, const arrow::Array& array, dropmask_cref dropmask,
    tenzir::diagnostic_handler& dh) const -> ::clickhouse::ColumnRef override {
    TENZIR_UNUSED(dropmask);
    const auto value_type = as<list_type>(type).value_type();
    const auto& list_array = as<arrow::ListArray>(array);
    const auto& value_array = *list_array.values();
    const auto& offsets
      = static_cast<arrow::Int32Array&>(*list_array.offsets());
    TENZIR_ASSERT(my_value_array == &value_array);
    TENZIR_ASSERT(my_mask.size() == static_cast<size_t>(value_array.length()),
                  fmt::format("{} == {}", my_mask.size(),
                              value_array.length()));
    auto clickhouse_columns
      = data_transform->create_column(value_type, value_array, my_mask, dh);
    auto clickhouse_offsets = make_offsets(offsets);
    return std::make_shared<ColumnArray>(std::move(clickhouse_columns),
                                         std::move(clickhouse_offsets));
  }
};

auto make_record_functions_from_clickhouse(std::string_view clickhouse_typename)
  -> std::unique_ptr<transformer> {
  clickhouse_typename.remove_prefix("Tuple("sv.size());
  clickhouse_typename.remove_suffix(1);
  auto fields = std::vector<std::pair<std::string_view, std::string_view>>{};
  auto open = 0;
  size_t last = 0;
  for (size_t i = 0; i < clickhouse_typename.size(); ++i) {
    const auto c = clickhouse_typename[i];
    if (c == ')') {
      --open;
      continue;
    }
    if (c == '(') {
      ++open;
      continue;
    }
    if (c == ',' and open == 0) {
      auto part = detail::trim(clickhouse_typename.substr(last, i));
      auto split = part.find(' ');
      fields.emplace_back(part.substr(0, split), part.substr(split + 1));
      last = i + 1;
      continue;
    }
  }
  auto part = detail::trim(clickhouse_typename.substr(last));
  auto split = part.find(' ');
  fields.emplace_back(part.substr(0, split), part.substr(split + 1));

  auto transformations = transformer_record::schema_transformations{};

  for (const auto& [k, t] : fields) {
    auto functions = make_functions_from_clickhouse(t);
    if (not functions) {
      return nullptr;
    }
    auto [_, success]
      = transformations.try_emplace(std::string{k}, std::move(functions));
    TENZIR_ASSERT(success);
  }
  return std::make_unique<transformer_record>(std::string{clickhouse_typename},
                                              std::move(transformations));
}

auto make_array_functions_from_clickhouse(std::string_view clickhouse_typename)
  -> std::unique_ptr<transformer> {
  auto value_typename = clickhouse_typename;
  value_typename.remove_prefix("Array("sv.size());
  value_typename.remove_suffix(1);
  auto data_transform = make_functions_from_clickhouse(value_typename);
  if (not data_transform) {
    return nullptr;
  }
  return std::make_unique<transformer_array>(std::string{clickhouse_typename},
                                             std::move(data_transform));
}

} // namespace

auto type_to_clickhouse_typename(tenzir::type t, bool nullable) -> std::string {
  const auto f = detail::overload{
    [&]<typename T>(const T&)
      requires requires {
        tenzir_to_clickhouse_trait<T>::clickhouse_typename(true);
      }
    {
      return tenzir_to_clickhouse_trait<T>::clickhouse_typename(nullable);
    },
    [](const record_type& r) {
      auto tup = plain_clickhouse_tuple_elements(r);
      if (tup.empty()) {
        return std::string{};
      }
      return "Tuple" + tup;
    },
    [&](const list_type& l) {
      auto vt = type_to_clickhouse_typename(l.value_type(), nullable);
      if (vt.empty()) {
        return std::string{};
      }
      return "Array(" + vt + ")";
    },
    [](const auto& t) {
      TENZIR_TRACE("unsupported type `{}`", t);
      return std::string{};
    },
  };
  return match(t, f);
}

auto plain_clickhouse_tuple_elements(const record_type& record,
                                     std::string_view primary) -> std::string {
  auto res = std::string{"("};
  auto first = true;
  for (auto [k, t] : record.fields()) {
    if (not first) {
      res += ", ";
    } else {
      first = false;
    }
    auto nested = type_to_clickhouse_typename(t, k != primary);
    if (nested.empty()) {
      return {};
    }
    fmt::format_to(std::back_inserter(res), "{} {}", k, nested);
  }
  res += ")";
  return res;
}

auto make_functions_from_clickhouse(const std::string_view clickhouse_typename)
  -> std::unique_ptr<transformer> {
  // Array(T)
  const bool is_nullable = clickhouse_typename.starts_with("Nullable(");
  TENZIR_ASSERT(not is_nullable or clickhouse_typename.ends_with(')'));
#define X(TENZIR_TYPE)                                                         \
  if (clickhouse_typename                                                      \
      == tenzir_to_clickhouse_trait<TENZIR_TYPE>::clickhouse_typename(         \
        false)) {                                                              \
    return make_transformer_impl<TENZIR_TYPE>(false);                          \
  }                                                                            \
  if (clickhouse_typename                                                      \
      == tenzir_to_clickhouse_trait<TENZIR_TYPE>::clickhouse_typename(true)) { \
    return make_transformer_impl<TENZIR_TYPE>(true);                           \
  }
  X(int64_type);
  X(uint64_type);
  X(double_type);
  X(string_type);
  X(time_type);
  X(duration_type);
  X(ip_type);
  X(subnet_type);
#undef X
  if (clickhouse_typename.starts_with("Tuple(")) {
    return make_record_functions_from_clickhouse(clickhouse_typename);
  }
  if (clickhouse_typename.starts_with("Array(")) {
    return make_array_functions_from_clickhouse(clickhouse_typename);
  }
  return nullptr;
}

} // namespace tenzir::plugins::clickhouse
