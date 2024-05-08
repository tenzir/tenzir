//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/numeric/bool.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/range_map.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/fbs/data.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>

#include <arrow/array.h>
#include <arrow/array/array_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <caf/error.hpp>
#include <caf/sum_type.hpp>
#include <tsl/robin_map.h>

#include <chrono>
#include <concepts>
#include <memory>
#include <string>

namespace tenzir::plugins::lookup_table {

namespace {

template <typename T>
auto try_lossless_cast(T x) -> std::optional<T> {
  return x;
}

template <typename To, typename From>
auto try_lossless_cast(From from) -> std::optional<To>
  requires std::integral<From> && std::integral<To>
           && (not std::same_as<From, To>)
{
  // Adapted from narrow.hpp
  auto to = detail::narrow_cast<To>(from);
  if (static_cast<From>(to) != from
      && (detail::is_same_signedness<To, From>::value
          || (to < To{}) != (from < From{}))) {
    return std::nullopt;
  }
  return to;
}

template <typename To, typename From>
auto try_lossless_cast(From from) -> std::optional<To>
  requires(std::floating_point<From> || std::floating_point<To>)
          && (not std::same_as<From, To>)
{
  if constexpr (std::integral<To>) {
    if (not std::is_signed_v<To> && from < From{}) {
      return std::nullopt;
    }
  }
  auto to = static_cast<To>(from);
  if (static_cast<From>(to) != from) {
    return std::nullopt;
  }
  return to;
}

// Used as the key in the lookup table.
//
// Wraps a `data`, which is modified in such a way so that
// `key_data{int64_t{42}} == key_data{uint64_t{42}} == key_data{double{42.0}}`,
// and their hashes also compare equal.
// This is achieved by casting the incoming data to `int64` or `uint64` if the
// conversion is lossless. The original type information is retained, to be used
// with context dumps and binary serialization.
class key_data {
  static constexpr size_t i64_index = static_cast<size_t>(
    caf::detail::tl_index_of<data::types, int64_t>::value);
  static constexpr size_t u64_index = static_cast<size_t>(
    caf::detail::tl_index_of<data::types, uint64_t>::value);
  static constexpr size_t double_index
    = static_cast<size_t>(caf::detail::tl_index_of<data::types, double>::value);

public:
  key_data() = default;

  explicit(false) key_data(data d)
    : original_type_index_(d.get_data().index()),
      data_(from_data(std::move(d))) {
  }

  friend bool operator==(const key_data& a, const key_data& b) {
    return a.data_ == b.data_;
  }

  /// Returns `data` that's used as the key in the lookup table.
  auto to_lookup_data() const -> const data& {
    return data_;
  }

  /// Returns `data` that has the same type as the `data` originally given to
  /// construct `*this`.
  auto to_original_data() const -> data {
    if (original_type_index_ == data_.get_data().index()) {
      return data_;
    }
    auto visitor = [&]<typename T>(T&& x) -> data {
      if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>
                    || std::is_same_v<T, double>) {
        return to_original_data_impl<T>();
      } else {
        return x;
      }
    };
    return caf::visit(visitor, data_);
  }

  /// Calls `out.emplace_back(x)` for every `x` that the value contained in
  /// `*this` can be losslessly converted to.
  void populate_snapshot_data(auto& out) const {
    auto visitor = [&]<typename T>(const T& x) {
      if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>
                    || std::is_same_v<T, double>) {
        if (auto y = try_lossless_cast<int64_t>(x)) {
          out.emplace_back(data{*y});
        }
        if (auto y = try_lossless_cast<uint64_t>(x)) {
          out.emplace_back(data{*y});
        }
        if (auto y = try_lossless_cast<double>(x)) {
          out.emplace_back(data{*y});
        }
      } else {
        out.emplace_back(data{x});
      }
    };
    caf::visit(visitor, data_);
  }

  auto original_type_index() const -> size_t {
    return original_type_index_;
  }

private:
  static auto from_data(data d) -> data {
    auto visitor = []<typename T>(T x) -> data {
      if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>
                    || std::is_same_v<T, double>) {
        // First try int64, then uint64, fall back to double
        if (auto y = try_lossless_cast<int64_t>(x)) {
          return data{*y};
        }
        if (auto y = try_lossless_cast<uint64_t>(x)) {
          return data{*y};
        }
        return data{x};
      } else {
        return x;
      }
    };
    return caf::visit(visitor, std::move(d));
  }

  template <typename StoredType>
  auto to_original_data_impl() const -> data {
    switch (original_type_index_) {
      case i64_index:
        return data{static_cast<int64_t>(caf::get<StoredType>(data_))};
      case u64_index:
        return data{static_cast<uint64_t>(caf::get<StoredType>(data_))};
      case double_index:
        return data{static_cast<double>(caf::get<StoredType>(data_))};
      default:
        TENZIR_UNREACHABLE();
    }
  }

  size_t original_type_index_{0};
  data data_{caf::none};
};

} // namespace

} // namespace tenzir::plugins::lookup_table

namespace std {

template <>
struct hash<tenzir::plugins::lookup_table::key_data> {
  auto operator()(const tenzir::plugins::lookup_table::key_data& x) const {
    return tenzir::hash(x.to_lookup_data());
  }
};

} // namespace std

namespace tenzir::plugins::lookup_table {

namespace {

using map_type = tsl::robin_map<key_data, data>;

class ctx final : public virtual context {
public:
  ctx() noexcept = default;

  explicit ctx(map_type context_entries) noexcept
    : context_entries{std::move(context_entries)} {
    // nop
  }

  auto context_type() const -> std::string override {
    return "lookup-table";
  }

  auto apply(series array, bool replace) const
    -> caf::expected<std::vector<series>> override {
    auto builder = series_builder{};
    for (const auto& value : array.values()) {
      if (auto it = context_entries.find(materialize(value));
          it != context_entries.end()) {
        builder.data(it->second);
      } else if (replace and not caf::holds_alternative<caf::none_t>(value)) {
        builder.data(value);
      } else {
        builder.null();
      }
    }
    return builder.finish();
  }

  auto snapshot(parameter_map, const std::vector<std::string>& fields) const
    -> caf::expected<expression> override {
    auto keys = list{};
    keys.reserve(context_entries.size());
    auto first_index = std::optional<size_t>{};
    for (const auto& [k, _] : context_entries) {
      k.populate_snapshot_data(keys);
      auto current_index = k.original_type_index();
      if (not first_index) [[unlikely]] {
        first_index = current_index;
      } else if (*first_index != current_index) [[unlikely]] {
        // TODO: With the language revamp, we should get heterogeneous lookups
        // for free.
        return caf::make_error(ec::unimplemented,
                               "lookup-table does not support snapshots for "
                               "heterogeneous keys");
      }
    }
    auto result = disjunction{};
    result.reserve(fields.size());
    for (const auto& field : fields) {
      auto lhs = to<operand>(field);
      TENZIR_ASSERT(lhs);
      result.emplace_back(predicate{
        *lhs,
        relational_operator::in,
        data{keys},
      });
    }
    return result;
  }

  /// Inspects the context.
  auto show() const -> record override {
    return record{{"num_entries", context_entries.size()}};
  }

  auto dump() -> generator<table_slice> override {
    auto entry_builder = series_builder{};
    for (const auto& [key, value] : context_entries) {
      auto row = entry_builder.record();
      row.field("key", key.to_original_data());
      row.field("value", value);
      if (entry_builder.length() >= context::dump_batch_size_limit) {
        co_yield entry_builder.finish_assert_one_slice(
          fmt::format("tenzir.{}.info", context_type()));
      }
    }
    // Dump all remaining entries that did not reach the size limit.
    co_yield entry_builder.finish_assert_one_slice(
      fmt::format("tenzir.{}.info", context_type()));
  }

  /// Updates the context.
  auto update(table_slice slice, context::parameter_map parameters)
    -> caf::expected<update_result> override {
    // context does stuff on its own with slice & parameters
    TENZIR_ASSERT(slice.rows() != 0);
    if (not parameters.contains("key")) {
      return caf::make_error(ec::invalid_argument, "missing 'key' parameter");
    }
    auto key_field = parameters["key"];
    if (not key_field) {
      return caf::make_error(ec::invalid_argument,
                             "invalid 'key' parameter; 'key' must be a string");
    }
    auto key_column = slice.schema().resolve_key_or_concept_once(*key_field);
    if (not key_column) {
      // If there's no key column then we cannot do much.
      return update_result{record{}};
    }
    auto [key_type, key_array] = key_column->get(slice);
    auto context_array = std::static_pointer_cast<arrow::Array>(
      to_record_batch(slice)->ToStructArray().ValueOrDie());
    auto key_values = values(key_type, *key_array);
    auto key_values_list = list{};
    auto context_values = values(slice.schema(), *context_array);
    auto key_it = key_values.begin();
    auto context_it = context_values.begin();
    while (key_it != key_values.end()) {
      TENZIR_ASSERT(context_it != context_values.end());
      auto materialized_key = materialize(*key_it);
      context_entries.insert_or_assign(materialized_key,
                                       materialize(*context_it));
      key_values_list.emplace_back(materialized_key);
      ++key_it;
      ++context_it;
    }
    TENZIR_ASSERT(context_it == context_values.end());
    auto query_f
      = [key_values_list = std::move(key_values_list)](
          parameter_map,
          const std::vector<std::string>& fields) -> caf::expected<expression> {
      auto result = disjunction{};
      result.reserve(fields.size());
      for (const auto& field : fields) {
        auto lhs = to<operand>(field);
        TENZIR_ASSERT(lhs);
        result.emplace_back(predicate{
          *lhs,
          relational_operator::in,
          data{key_values_list},
        });
      }
      return result;
    };
    return update_result{.update_info = show(),
                         .make_query = std::move(query_f)};
  }

  auto make_query() -> make_query_type override {
    auto key_values_list = list{};
    key_values_list.reserve(context_entries.size());
    for (const auto& entry : context_entries) {
      entry.first.populate_snapshot_data(key_values_list);
    }
    return
      [key_values_list = std::move(key_values_list)](
        parameter_map,
        const std::vector<std::string>& fields) -> caf::expected<expression> {
        auto result = disjunction{};
        result.reserve(fields.size());
        for (const auto& field : fields) {
          auto lhs = to<operand>(field);
          TENZIR_ASSERT(lhs);
          result.emplace_back(predicate{
            *lhs,
            relational_operator::in,
            data{key_values_list},
          });
        }
        return result;
      };
  }

  auto reset() -> caf::expected<void> override {
    context_entries.clear();
    return {};
  }

  auto save() const -> caf::expected<save_result> override {
    // We save the context by formatting into a record of this format:
    //   [{key: key, value: value}, ...]
    auto builder = flatbuffers::FlatBufferBuilder{};
    auto value_offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
    value_offsets.reserve(context_entries.size());
    for (const auto& [key, value] : context_entries) {
      auto field_offsets
        = std::vector<flatbuffers::Offset<fbs::data::RecordField>>{};
      field_offsets.reserve(2);
      const auto key_key_offset = builder.CreateSharedString("key");
      const auto key_value_offset = pack(builder, key.to_original_data());
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, key_key_offset, key_value_offset));
      const auto value_key_offset = builder.CreateSharedString("value");
      const auto value_value_offset = pack(builder, value);
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, value_key_offset, value_value_offset));
      const auto record_offset
        = fbs::data::CreateRecordDirect(builder, &field_offsets);
      value_offsets.emplace_back(fbs::CreateData(
        builder, fbs::data::Data::record, record_offset.Union()));
    }
    const auto list_offset
      = fbs::data::CreateListDirect(builder, &value_offsets);
    const auto data_offset
      = fbs::CreateData(builder, fbs::data::Data::list, list_offset.Union());
    fbs::FinishDataBuffer(builder, data_offset);
    return save_result{.data = chunk::make(builder.Release()), .version = 1};
  }

private:
  map_type context_entries;
};

struct v1_loader : public context_loader {
  auto version() const -> int {
    return 1;
  }

  auto load(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> {
    auto fb = flatbuffer<fbs::Data>::make(std::move(serialized));
    if (not fb) {
      return caf::make_error(ec::serialization_error,
                             fmt::format("failed to deserialize lookup table "
                                         "context: {}",
                                         fb.error()));
    }
    auto context_entries = map_type{};
    const auto* list = fb.value()->data_as_list();
    if (not list) {
      return caf::make_error(ec::serialization_error,
                             "failed to deserialize lookup table "
                             "context: no valid list value for "
                             "serialized context entry list");
    }
    if (not list->values()) {
      return caf::make_error(ec::serialization_error,
                             "failed to deserialize lookup table "
                             "context: missing or invalid values for "
                             "context entry in serialized entry list");
    }
    for (const auto* list_value : *list->values()) {
      const auto* record = list_value->data_as_record();
      if (not record) {
        return caf::make_error(ec::serialization_error,
                               "failed to deserialize lookup table "
                               "context: invalid type for "
                               "context entry in serialized entry list, "
                               "entry must be a record");
      }
      if (not record->fields() or record->fields()->size() != 2) {
        return caf::make_error(ec::serialization_error,
                               "failed to deserialize lookup table "
                               "context: invalid or missing value for "
                               "context entry in serialized entry list, "
                               "entry must be a record {key, value}");
      }
      data key;
      data value;
      auto err = unpack(*record->fields()->Get(0)->data(), key);
      if (err) {
        return caf::make_error(ec::serialization_error,
                               fmt::format("failed to deserialize lookup "
                                           "table "
                                           "context: invalid key: {}",
                                           err));
      }
      err = unpack(*record->fields()->Get(1)->data(), value);
      if (err) {
        return caf::make_error(ec::serialization_error,
                               fmt::format("failed to deserialize lookup "
                                           "table "
                                           "context: invalid value: {}",
                                           err));
      }
      context_entries.emplace(std::move(key), std::move(value));
    }

    return std::make_unique<ctx>(std::move(context_entries));
  }
};

class plugin : public virtual context_plugin {
  auto initialize(const record&, const record&) -> caf::error override {
    register_loader(std::make_unique<v1_loader>());
    return caf::none;
  }

  auto name() const -> std::string override {
    return "lookup-table";
  }

  auto make_context(context::parameter_map) const
    -> caf::expected<std::unique_ptr<context>> override {
    return std::make_unique<ctx>();
  }
};

} // namespace

} // namespace tenzir::plugins::lookup_table

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lookup_table::plugin)
