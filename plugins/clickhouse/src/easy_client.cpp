//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "easy_client.hpp"

#include "clickhouse/client.h"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/view3.hpp"

#include <boost/regex.hpp>

#include <ranges>

using namespace clickhouse;
using namespace std::string_view_literals;

namespace tenzir::plugins::clickhouse {

namespace {

std::string remove_excessive_spaces(std::string_view str) {
  std::string ret;
  ret.reserve(str.size());
  for (size_t i = 0; i < str.size() - 1; ++i) {
    if (std::isspace(str[i]) and std::isspace(str[i + 1])) {
      continue;
    }
    ret += str[i];
  }
  ret += str.back();
  return ret;
}

auto value_transform(auto v) {
  return v;
}

auto value_transform(tenzir::time v) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
           v.time_since_epoch())
    .count();
}

auto value_transform(tenzir::duration v) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(v).count();
}

template <typename>
struct tenzir_to_clickhouse_trait;

#define X(TENZIR_TYPENAME, CLICKHOUSE_COLUMN, CLICKHOUSE_NAME)                 \
  template <>                                                                  \
  struct tenzir_to_clickhouse_trait<TENZIR_TYPENAME> {                         \
    constexpr static std::string_view clickhouse_typename = CLICKHOUSE_NAME;   \
    using column_type = CLICKHOUSE_COLUMN;                                     \
                                                                               \
    static auto full_typename() -> std::string {                               \
      return std::string{"Nullable("}                                          \
        .append(clickhouse_typename)                                           \
        .append(1, ')');                                                       \
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
X(uint64_type, ColumnUInt64, "Uint64");
X(double_type, ColumnFloat64, "Float64");
X(string_type, ColumnString, "String");
X(duration_type, ColumnInt64, "Int64");
#undef X

template <>
struct tenzir_to_clickhouse_trait<time_type> {
  constexpr static std::string_view clickhouse_typename = "DateTime64";
  using column_type = ColumnDateTime64;

  static auto full_typename() -> std::string {
    return std::string{"Nullable("}.append(clickhouse_typename).append("(8))");
  }

  template <bool nullable>
  static auto allocate(size_t n) {
    using Column_Type
      = std::conditional_t<nullable, ColumnNullableT<column_type>, column_type>;
    auto res = std::make_shared<Column_Type>(8);
    res->Reserve(n);
    return res;
  }
};

/// TODO
// Array

template <typename T, bool nullable>
  requires requires { tenzir_to_clickhouse_trait<T>{}; }
struct transformer_from_trait : transformer {
  using traits = tenzir_to_clickhouse_trait<T>;

  transformer_from_trait() : transformer{traits::full_typename()} {
  }

  virtual auto update_dropmask(const tenzir::type&, const arrow::Array& array,
                               dropmask_ref dropmask,
                               tenzir::diagnostic_handler&) -> bool override {
    if constexpr (nullable) {
      return false;
    }
    if (not array.null_bitmap()) {
      return false;
    }
    if (array.null_count() == 0) {
      return false;
    }
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        dropmask[i] = true;
      }
    }
    return true;
  }

  virtual auto create_columns(
    const tenzir::type& type, const arrow::Array& array, dropmask_ref dropmask,
    tenzir::diagnostic_handler& dh) -> ::clickhouse::ColumnRef override {
    const auto f = detail::overload{
      [&](const auto&) -> std::shared_ptr<Column> {
        // error case. Potentially do conversions?
        diagnostic::warning("incompatible data").emit(dh);
        return nullptr;
      },
      [&](const T&) -> std::shared_ptr<Column> {
        auto column = traits::template allocate<nullable>(array.length());
        auto cast_array = dynamic_cast<const type_to_arrow_array_t<T>*>(&array);
        TENZIR_ASSERT(cast_array);
        for (int64_t i = 0; i < cast_array->length(); ++i) {
          if (dropmask[i]) {
            continue;
          }
          auto v = view_at(*cast_array, i);
          if constexpr (nullable) {
            if (not v) {
              column->Append(std::nullopt);
              continue;
            }
          }
          column->Append(value_transform(*v));
        }
        return column;
      }};
    return match(type, f);
  }
};

struct transformer_record : transformer {
  schema_transformations transformations;

  transformer_record(std::string clickhouse_typename,
                     schema_transformations transformations)
    : transformer{std::move(clickhouse_typename)},
      transformations{std::move(transformations)} {
  }

  virtual auto
  update_dropmask(const tenzir::type& type, const arrow::Array& array,
                  dropmask_ref dropmask,
                  tenzir::diagnostic_handler& dh) -> bool override {
    if (not array.null_bitmap()) {
      return false;
    }
    if (array.null_count() == 0) {
      return false;
    }
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        dropmask[i] = true;
      }
    }
    const auto& rt = as<record_type>(type);
    const auto& struct_array = as<arrow::StructArray>(array);
    for (auto [i, kt] : detail::enumerate(rt.fields())) {
      const auto& [k, t] = kt;
      const auto it = transformations.find(k);
      if (it == transformations.end()) {
        diagnostic::warning(
          "nested column `{}` does not exist in ClickHouse table ", k)
          .emit(dh);
        continue;
      }
      auto offset = tenzir::offset{};
      offset.push_back(i);
      auto arr = offset.get(struct_array);
      auto& trafo = it->second;
      trafo->update_dropmask(t, *arr, dropmask, dh);
    }
    return true;
  }

  virtual auto create_columns(
    const tenzir::type& type, const arrow::Array& array, dropmask_ref dropmask,
    tenzir::diagnostic_handler& dh) -> ::clickhouse::ColumnRef override {
    auto columns = std::vector<ColumnRef>{};
    const auto& rt = as<record_type>(type);
    const auto& struct_array = as<arrow::StructArray>(array);
    for (auto [i, kt] : detail::enumerate(rt.fields())) {
      const auto& [k, t] = kt;
      const auto it = transformations.find(k);
      if (it == transformations.end()) {
        continue;
      }
      auto offset = tenzir::offset{};
      offset.push_back(i);
      auto arr = offset.get(struct_array);
      auto& trafo = it->second;
      columns.push_back(trafo->create_columns(t, *arr, dropmask, dh));
    }
    return std::make_shared<ColumnTuple>(std::move(columns));
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

auto make_record_functions_from_clickhouse(std::string_view clickhouse_typelist)
  -> std::unique_ptr<transformer>;

auto make_functions_from_clickhouse(const std::string_view clickhouse_typename)
  -> std::unique_ptr<transformer> {
  // Array(T)
  // IPv4, IPv6
  const bool is_nullable = clickhouse_typename.starts_with("Nullable(");
  TENZIR_ASSERT(not is_nullable or clickhouse_typename.ends_with(')'));
  auto stripped_clickhouse_name = clickhouse_typename;
  if (is_nullable) {
    stripped_clickhouse_name.remove_prefix("Nullable("sv.size());
    stripped_clickhouse_name.remove_suffix(1);
  }
#define X(TENZIR_TYPE)                                                         \
  if (stripped_clickhouse_name                                                 \
      == tenzir_to_clickhouse_trait<TENZIR_TYPE>::clickhouse_typename) {       \
    return make_transformer_impl<TENZIR_TYPE>(is_nullable);                    \
  }
  X(int64_type);
  X(uint64_type);
  X(double_type);
  X(string_type);
  X(time_type);
  X(duration_type);
#undef X
  if (stripped_clickhouse_name.starts_with("Tuple(")) {
    return make_record_functions_from_clickhouse(stripped_clickhouse_name);
  }
  return nullptr;
}

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

  auto transformations = schema_transformations{};

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

auto type_to_clickhouse_typename(tenzir::type t) -> std::string;

auto plain_clickhouse_tuple_elements(const record_type& record) -> std::string {
  auto res = std::string{"("};
  auto first = true;
  for (auto [k, t] : record.fields()) {
    if (not first) {
      res += ", ";
    } else {
      first = false;
    }
    auto nested = type_to_clickhouse_typename(t);
    if (nested.empty()) {
      return {};
    }
    fmt::format_to(std::back_inserter(res), "{} {}", k, nested);
  }
  res += ")";
  return res;
}

auto type_to_clickhouse_typename(tenzir::type t) -> std::string {
#define X(CLICKHOUSE_TYPE_STRING, TENZIR_TYPE)                                 \
  [](const TENZIR_TYPE&) -> std::string {                                      \
    return "Nullable(" CLICKHOUSE_TYPE_STRING ")";                             \
  }
  constexpr static auto f = detail::overload{
    []<typename T>(const T&)
      requires requires { tenzir_to_clickhouse_trait<T>::clickhouse_typename; }
    {
      return tenzir_to_clickhouse_trait<T>::full_typename();
    },
    [](const record_type& r) {
      return "Tuple" + plain_clickhouse_tuple_elements(r);
    },
    [](const auto&) -> std::string {
      return {};
    },
  };
#undef X
  return match(t, f);
}

} // namespace

auto Easy_Client::table_exists(std::string_view table) -> bool {
  // // This does not work for some reason
  // auto query = Query{fmt::format("EXISTS TABLE {}", table)};
  // auto exists = false;
  // auto cb = [&](const Block& block) {
  //   TENZIR_ASSERT(block.GetColumnCount() == 1);
  //   auto cast = block[0]->As<ColumnUInt8>();
  //   TENZIR_ASSERT(cast);
  //   exists = cast->At(0) == 1;
  // };
  // query.OnData(cb);
  // client.Execute(query);
  // return exists;
  auto query = Query{fmt::format("SHOW TABLES LIKE '{}'", table)};
  auto exists = false;
  auto cb = [&](const Block& block) {
    for (size_t i = 0; i < block.GetRowCount(); ++i) {
      auto name = block[0]->As<ColumnString>()->At(i);
      if (name == table) {
        exists = true;
        break;
      }
    }
  };
  query.OnData(cb);
  client.Execute(query);
  return exists;
};

auto Easy_Client::get_schema_transformations(std::string_view table)
  -> std::optional<schema_transformations> {
  auto query = Query{fmt::format("DESCRIBE TABLE {} "
                                 "SETTINGS describe_compact_output=1",
                                 table)};
  auto error = false;
  auto result = schema_transformations{};
  auto cb = [&](const Block& block) {
    for (size_t i = 0; i < block.GetRowCount(); ++i) {
      auto name = block[0]->As<ColumnString>()->At(i);
      auto type_str
        = remove_excessive_spaces(block[1]->As<ColumnString>()->At(i));
      auto functions = make_functions_from_clickhouse(type_str);
      if (not functions) {
        error = true;
        auto diag
          = diagnostic::error(
              "unsupported column type in pre-existing table `{}`", table)
              .primary(operator_location)
              .note("column `{}` has unsupported type `{}`", name, type_str);
        // A few helpful suggestions for the types that we do support
        if (name.starts_with("Nested(")) {
          diag = std::move(diag).note("use `Tuple(T...)` instead");
        } else if (name.starts_with("Date")) {
          diag = std::move(diag).note("use `DateTime64(8)` instead");
        } else if (name.starts_with("UInt")) {
          diag = std::move(diag).note("use `UInt64` instead");
        } else if (name.starts_with("Int")) {
          diag = std::move(diag).note("use `Int64` instead");
        } else if (name.starts_with("Float")) {
          diag = std::move(diag).note("use `Float64` instead");
        }
        std::move(diag).emit(dh);
      }
      result.try_emplace(std::string{name}, std::move(functions));
    }
  };
  query.OnData(cb);
  client.Execute(query);
  if (error) {
    return std::nullopt;
  }
  return result;
}

auto Easy_Client::create_table(std::string_view table_name,
                               const tenzir::record_type& schema)

  -> std::optional<schema_transformations> {
  auto result = schema_transformations{};
  auto columns = std::string{};
  for (auto [k, t] : schema.fields()) {
    auto clickhouse_type = type_to_clickhouse_typename(t);
    auto functions = make_functions_from_clickhouse(clickhouse_type);
    if (not functions) {
      diagnostic::error("unsupported column type in input")
        .primary(operator_location)
        .note("type `{}` is not supported", t.kind())
        .emit(dh);
      return std::nullopt;
    }
    result.try_emplace(std::string{k}, std::move(functions));
  }
  const std::string_view engine = "MergeTree";
  const std::string_view order = schema.field(0).name;
  auto query_text
    = fmt::format("CREATE TABLE {}"
                  " {}"
                  " ENGINE = {}"
                  " ORDER BY {} SETTINGS allow_nullable_key=1 ;",
                  table_name, plain_clickhouse_tuple_elements(schema), engine,
                  order);
  fmt::print("`{}`\n", query_text);
  auto query = Query{query_text};
  client.Execute(query);
  return result;
}
} // namespace tenzir::plugins::clickhouse
