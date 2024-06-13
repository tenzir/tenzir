//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/numeric/bool.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/range_map.hpp>
#include <tenzir/detail/subnet_tree.hpp>
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

#include <memory>
#include <optional>
#include <string>
#include <utility>

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

  friend auto operator==(const key_data& a, const key_data& b) -> bool {
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

// TODO; encapsulate some enum so can distinguish between
struct value_data {
  data data;
  std::optional<time> update_timeout;
  std::optional<time> create_timeout;
  std::optional<duration> update_duration;
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

using map_type = tsl::robin_map<key_data, value_data>;
class ctx final : public virtual context {
public:
  ctx() noexcept = default;
  explicit ctx(map_type context_entries,
               detail::subnet_tree subnet_entries) noexcept
    : context_entries{std::move(context_entries)},
      subnet_entries{std::move(subnet_entries)} {
    // nop
  }

  auto context_type() const -> std::string override {
    return "lookup-table";
  }

  auto apply(series array, bool replace)
    -> caf::expected<std::vector<series>> override {
    auto builder = series_builder{};
    auto subnet_lookup = [&](const auto& value) -> std::optional<view<data>> {
      auto match = detail::overload{
        [&](const auto&) -> const data* {
          return nullptr;
        },
        [&](view<ip> addr) {
          return subnet_entries.match(materialize(addr));
        },
        [&](view<subnet> sn) {
          return subnet_entries.match(materialize(sn));
        },
      };
      if (auto x = caf::visit(match, value)) {
        return make_view(*x);
      }
      return std::nullopt;
    };
    auto now = time::clock::now();
    for (auto value : array.values()) {
      if (auto it = context_entries.find(materialize(value));
          it != context_entries.end()) {
        if ((it->second.create_timeout && it->second.create_timeout < now)
            || (it->second.update_timeout && it->second.update_timeout < now)) {
          builder.null();
          continue;
        }
        if (it->second.update_timeout) {
          TENZIR_ASSERT(!it->second.update_duration);
          *it.value().update_timeout = now + *it->second.update_duration;
        }
      } else if (auto x = subnet_lookup(value)) {
        builder.data(*x);
      } else if (replace and not caf::holds_alternative<caf::none_t>(value)) {
        builder.data(value);
      } else {
        builder.null();
      }
    }
    return builder.finish();
  }

  // TODO: maybe have to skip fields
  auto snapshot(parameter_map, const std::vector<std::string>& fields) const
    -> caf::expected<expression> override {
    auto keys = list{};
    keys.reserve(context_entries.size());
    auto now = time::clock::now();
    auto first_index = std::optional<size_t>{};
    for (const auto& [k, v] : context_entries) {
      if ((v.create_timeout && v.create_timeout < now)
          || (v.update_timeout && v.update_timeout < now)) {
        continue;
      }
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
    for (const auto& [k, _] : subnet_entries.nodes()) {
      keys.emplace_back(k);
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

  // TODO: skip incorrect times on count
  /// Inspects the context.
  auto show() const -> record override {
    // There's no size() function for the PATRICIA trie, so we walk the tree
    // nodes here once in O(n).
    auto num_subnet_entries = size_t{0};
    auto num_context_entries = size_t{0};
    auto now = time::clock::now();
    // TENZIR_WARN("now time {}", now);
    for (auto _ : subnet_entries.nodes()) {
      ++num_subnet_entries;
      (void)_;
      // skip over past time
    }
    for (const auto& entry : context_entries) {
      if ((entry.second.update_timeout && entry.second.update_timeout < now)
          || (entry.second.create_timeout
              && entry.second.create_timeout < now)) {
        continue;
      }
      ++num_context_entries;
    }
    // TENZIR_WARN("num_entries {}", num_context_entries + num_subnet_entries);
    return record{
      {"num_entries", num_context_entries + num_subnet_entries},
    };
  }

  auto dump() -> generator<table_slice> override {
    auto entry_builder = series_builder{};
    auto now = time::clock::now();
    for (const auto& [key, value] : subnet_entries.nodes()) {
      auto row = entry_builder.record();
      row.field("key", data{key});
      row.field("value", value ? *value : data{});
      if (entry_builder.length() >= context::dump_batch_size_limit) {
        co_yield entry_builder.finish_assert_one_slice(
          fmt::format("tenzir.{}.info", context_type()));
      }
    }
    for (const auto& [key, value] : context_entries) {
      if ((value.update_timeout && value.update_timeout < now)
          || (value.create_timeout && value.create_timeout < now)) {
        continue;
      }
      auto row = entry_builder.record();
      row.field("key", key.to_original_data());
      row.field("value",
                value.data); // should we also get timeout info in dump?
      if (entry_builder.length() >= context::dump_batch_size_limit) {
        for (auto&& slice : entry_builder.finish_as_table_slice(
               fmt::format("tenzir.{}.info", context_type()))) {
          co_yield std::move(slice);
        }
      }
    }
    // Dump all remaining entries that did not reach the size limit.
    for (auto&& slice : entry_builder.finish_as_table_slice(
           fmt::format("tenzir.{}.info", context_type()))) {
      co_yield std::move(slice);
    }
  }
  // TODO: simplify update logic: create struct at beg
  /// Updates the context.
  auto update(table_slice slice, context::parameter_map parameters)
    -> caf::expected<update_result> override {
    // context does stuff on its own with slice & parameters
    TENZIR_ASSERT(slice.rows() != 0);
    if (caf::get<record_type>(slice.schema()).num_fields() == 0) {
      return caf::make_error(ec::invalid_argument,
                             "context update cannot handle empty input events");
    }
    const auto now = time::clock::now();
    auto context_value = value_data{};
    if (parameters.contains("create-timeout")) {
      auto create_timeout_str = parameters["create-timeout"];
      if (not create_timeout_str) {
        return caf::make_error(ec::invalid_argument,
                               "'create-timeout' option must have a value");
      }
      auto create_timeout = to<duration>(*create_timeout_str);
      if (not create_timeout) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("'create-timeout' option must be a "
                                           "valid duration: {}",
                                           create_timeout.error()));
      }
      context_value.create_timeout = now + *create_timeout;
    }
    if (parameters.contains("update-timeout")) {
      auto update_timeout_str = parameters["update-timeout"];
      if (not update_timeout_str) {
        return caf::make_error(ec::invalid_argument,
                               "'update-timeout' option must have a value");
      }
      auto update_timeout = to<duration>(*update_timeout_str);
      if (not update_timeout) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("'update-timeout' option must be a "
                                           "valid duration: {}",
                                           update_timeout.error()));
      }
      context_value.update_timeout = now + *update_timeout;
      context_value.update_duration = *update_timeout;
    }
    auto erase = parameters.contains("erase");
    if (erase and parameters["erase"].has_value()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("'erase' option must not have a "
                                         "value; found '{}'",
                                         *parameters["erase"]));
    }
    auto key_column = [&]() -> caf::expected<offset> {
      if (not parameters.contains("key")) {
        return offset{0};
      }
      auto key_field = parameters["key"];
      if (not key_field) {
        return caf::make_error(ec::invalid_argument, "invalid 'key' parameter; "
                                                     "'key' must be a string");
      }
      auto key_column = slice.schema().resolve_key_or_concept_once(*key_field);
      if (not key_column) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("key '{}' does not exist in schema "
                                           "'{}'",
                                           *key_field, slice.schema()));
      }
      return std::move(*key_column);
    }();
    if (not key_column) {
      return std::move(key_column.error());
    }
    auto [key_type, key_array] = key_column->get(slice);
    // TENZIR_WARN("key_array: {}", key_array->ToString());
    auto key_values = values(key_type, *key_array);
    auto key_values_list = list{};
    if (erase) {
      // Subnets never make it into the regular map of entries.
      if (caf::holds_alternative<subnet_type>(key_type)) {
        for (const auto& key :
             values(subnet_type{},
                    caf::get<type_to_arrow_array_t<subnet_type>>(*key_array))) {
          if (not key) {
            continue;
          }
          subnet_entries.erase(*key);
        }
      } else {
        for (const auto& key : values(key_type, *key_array)) {
          context_entries.erase(materialize(key));
        }
      }
      return update_result{
        .update_info = show(),
        .make_query = {},
      };
    }
    auto context_array = std::static_pointer_cast<arrow::Array>(
      to_record_batch(slice)->ToStructArray().ValueOrDie());
    auto context_values = values(slice.schema(), *context_array);
    auto key_it = key_values.begin();
    auto context_it = context_values.begin();
    while (key_it != key_values.end()) {
      value_data value_val = context_value;
      TENZIR_ASSERT(context_it != context_values.end());
      value_val.data = materialize(*context_it);
      auto materialized_key = materialize(*key_it);
      // Subnets never make it into the regular map of entries.
      if (caf::holds_alternative<subnet_type>(key_type)) {
        const auto& key = caf::get<subnet>(materialized_key);
        subnet_entries.insert(key, materialize(*context_it));
      } else {
        // TENZIR_WARN("Trying to update with cr time {} and up time {}",
        //             caf::timestamp_to_string(*value_val.create_timeout),
        //             caf::timestamp_to_string(*value_val.update_timeout));
        context_entries.insert_or_assign(materialized_key, value_val);
      }
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
    return update_result{
      .update_info = show(),
      .make_query = std::move(query_f),
    };
  }

  // TODO:: only query ones with correct time out
  auto make_query() -> make_query_type override {
    auto key_values_list = list{};
    key_values_list.reserve(
      context_entries.size()); // TODO: have another variable here?
    auto now = time::clock::now();
    for (const auto& entry : context_entries) {
      if ((entry.second.update_timeout && entry.second.update_timeout < now)
          || (entry.second.create_timeout
              && entry.second.create_timeout < now)) {
        continue;
      }
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
    subnet_entries.clear();
    return {};
  }

  auto save() const -> caf::expected<save_result> override {
    // We save the context by formatting into a record of this format:
    //   [{key: key, value: value}, ...]
    auto builder = flatbuffers::FlatBufferBuilder{};
    auto value_offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
    value_offsets.reserve(context_entries.size()); // TODO: store as variable
    auto now = time::clock::now();
    for (const auto& [key, value] : context_entries) {
      if ((value.update_timeout && value.update_timeout < now)
          || (value.create_timeout && value.create_timeout < now)) {
        continue;
      }
      auto field_offsets
        = std::vector<flatbuffers::Offset<fbs::data::RecordField>>{};
      field_offsets.reserve(4);
      const auto key_key_offset = builder.CreateSharedString("key");
      const auto key_value_offset = pack(builder, key.to_original_data());
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, key_key_offset, key_value_offset));
      auto value_key_offset = builder.CreateSharedString("value");
      auto value_value_offset = pack(builder, value.data);
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, value_key_offset, value_value_offset));
      if (value.create_timeout) {
        value_key_offset = builder.CreateSharedString("create-timeout");
        value_value_offset
          = pack(builder, *to<data>(*value.create_timeout)); // better way??
        field_offsets.emplace_back(fbs::data::CreateRecordField(
          builder, value_key_offset, value_value_offset));
      }
      if (value.update_timeout) {
        value_key_offset = builder.CreateSharedString("update-timeout");
        value_value_offset
          = pack(builder, *to<data>(*value.update_timeout)); // better way??
        field_offsets.emplace_back(fbs::data::CreateRecordField(
          builder, value_key_offset, value_value_offset));
        value_key_offset = builder.CreateSharedString("update-duration");
        value_value_offset
          = pack(builder, *to<data>(*value.update_duration)); // better way??
        field_offsets.emplace_back(fbs::data::CreateRecordField(
          builder, value_key_offset, value_value_offset));
      }
      const auto record_offset
        = fbs::data::CreateRecordDirect(builder, &field_offsets);
      value_offsets.emplace_back(fbs::CreateData(
        builder, fbs::data::Data::record, record_offset.Union()));
    }
    for (const auto& [key, value] : subnet_entries.nodes()) {
      auto field_offsets
        = std::vector<flatbuffers::Offset<fbs::data::RecordField>>{};
      field_offsets.reserve(2);
      const auto key_key_offset = builder.CreateSharedString("key");
      const auto key_value_offset = pack(builder, data{key});
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, key_key_offset, key_value_offset));
      const auto value_key_offset = builder.CreateSharedString("value");
      const auto value_value_offset = pack(builder, *value);
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
  detail::subnet_tree subnet_entries;
};

struct v1_loader : public context_loader {
  auto version() const -> int final {
    return 1;
  }

  auto load(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> final {
    auto fb = flatbuffer<fbs::Data>::make(std::move(serialized));
    if (not fb) {
      return caf::make_error(ec::serialization_error,
                             fmt::format("failed to deserialize lookup table "
                                         "context: {}",
                                         fb.error()));
    }
    auto context_entries = map_type{};
    auto subnet_entries = detail::subnet_tree{};
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
    auto now = time::clock::now();
    for (const auto* list_value : *list->values()) {
      const auto* record = list_value->data_as_record();
      if (not record) {
        return caf::make_error(ec::serialization_error,
                               "failed to deserialize lookup table "
                               "context: invalid type for "
                               "context entry in serialized entry list, "
                               "entry must be a record");
      }
      if (not record->fields()) {
        return caf::make_error(ec::serialization_error,
                               "failed to deserialize lookup table "
                               "context: invalid or missing value for "
                               "context entry in serialized entry list, "
                               "entry must be a record {key, value}");
      }
      data key;
      data subnet_value;
      value_data context_value{};
      auto err = unpack(*record->fields()->Get(0)->data(), key);
      // TODO: improve error handling
      if (err) {
        return caf::make_error(ec::serialization_error,
                               fmt::format("failed to deserialize lookup "
                                           "table "
                                           "context: invalid key: {}",
                                           err));
      }
      if (err) {
        return caf::make_error(ec::serialization_error,
                               fmt::format("failed to deserialize lookup "
                                           "table "
                                           "context: invalid value: {}",
                                           err));
      }
      // how to make is easier to not mix up create/update timeout
      if (auto* sn = caf::get_if<subnet>(&key)) {
        err = unpack(*record->fields()->Get(1)->data(), subnet_value);
        subnet_entries.insert(*sn, std::move(subnet_value));
      } else {
        for (const auto* field : *record->fields()) {
          if (GetString(field->name()) == "value") {
            err = unpack(*field->data(), context_value.data);
          }
          if (GetString(field->name()) == "create-timeout") {
            err = unpack(*field->data(),
                         *to<data>(*context_value.update_timeout));
            if (context_value.create_timeout < now) {
              continue;
            }
          }
          if (GetString(field->name()) == "update-timeout") {
            err = unpack(*field->data(),
                         *to<data>(*context_value.create_timeout));
            if (context_value.update_timeout < now) {
              continue;
            }
          }
          if (GetString(field->name()) == "update-duration") {
            err = unpack(*field->data(),
                         *to<data>(*context_value.update_duration));
          }
        }
        if (err) {
          return caf::make_error(ec::serialization_error,
                                 fmt::format("failed to deserialize lookup "
                                             "table "
                                             "context: invalid value: {}",
                                             err));
        }
        // TENZIR_WARN("Emplacing on load, upt: {}, creT: {}",
        //             context_value.update_timeout,
        //             context_value.create_timeout);
        context_entries.emplace(std::move(key), std::move(context_value));
      }
    }
    return std::make_unique<ctx>(std::move(context_entries),
                                 std::move(subnet_entries));
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
