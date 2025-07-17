//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/numeric/bool.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/context.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/range_map.hpp>
#include <tenzir/detail/subnet_tree.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/fbs/data.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/array.h>
#include <arrow/array/array_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <caf/error.hpp>
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
  static constexpr size_t i64_index
    = static_cast<size_t>(detail::tl_index_of<data::types, int64_t>::value);
  static constexpr size_t u64_index
    = static_cast<size_t>(detail::tl_index_of<data::types, uint64_t>::value);
  static constexpr size_t double_index
    = static_cast<size_t>(detail::tl_index_of<data::types, double>::value);

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
    return match(data_, visitor);
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
    tenzir::match(data_, visitor);
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
    return match(std::move(d), visitor);
  }

  template <typename StoredType>
  auto to_original_data_impl() const -> data {
    switch (original_type_index_) {
      case i64_index:
        return data{static_cast<int64_t>(as<StoredType>(data_))};
      case u64_index:
        return data{static_cast<uint64_t>(as<StoredType>(data_))};
      case double_index:
        return data{static_cast<double>(as<StoredType>(data_))};
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

struct value_data {
  data raw_data;

  std::optional<time> create_timeout;
  std::optional<time> write_timeout;

  // TODO: read_timeout and read_timeout_duration can move into the same
  // optional as they cannot be set independently
  std::optional<duration> read_timeout_duration;
  std::optional<time> read_timeout;

  auto is_expired(time now) const -> bool {
    return (read_timeout and *read_timeout < now)
           or (write_timeout and *write_timeout < now)
           or (create_timeout and *create_timeout < now);
  }

  auto refresh_read_timeout(time now) -> void {
    if (read_timeout_duration) {
      read_timeout = now + *read_timeout_duration;
    }
  }
};

using map_type = tsl::robin_map<key_data, value_data>;
using subnet_tree_type = detail::subnet_tree<value_data>;

class lookup_table_context final : public virtual context {
public:
  lookup_table_context() noexcept = default;
  explicit lookup_table_context(map_type context_entries,
                                subnet_tree_type subnet_entries) noexcept
    : context_entries{std::move(context_entries)},
      subnet_entries{std::move(subnet_entries)} {
    // nop
  }

  auto context_type() const -> std::string override {
    return "lookup-table";
  }

  auto subnet_lookup(const auto& value) -> std::pair<subnet, value_data*> {
    auto match = detail::overload{
      [&](const auto&) -> std::pair<subnet, value_data*> {
        return {{}, nullptr};
      },
      [&](view<ip> addr) {
        return subnet_entries.match(materialize(addr));
      },
      [&](view<subnet> sn) {
        return subnet_entries.match(materialize(sn));
      },
    };
    return tenzir::match(value, match);
  };

  auto legacy_apply(series array, bool replace)
    -> caf::expected<std::vector<series>> override {
    auto builder = series_builder{};
    const auto now = time::clock::now();
    for (auto value : array.values()) {
      // TODO: This should really be making use of heterogeneous map lookups
      // instead of materializing, but we're not using `data` and `data_view`
      // directly here, but rather a custom wrapper around them to make mixed
      // number types
      if (auto it = context_entries.find(materialize(value));
          it != context_entries.end()) {
        if (it->second.is_expired(now)) {
          context_entries.erase(it);
          goto retry; // NOLINT(cppcoreguidelines-avoid-goto)
        }
        it.value().refresh_read_timeout(now);
        builder.data(it->second.raw_data);
        continue;
      }
      // We need to retry the lookup if we had an expired hit, as a matched IP
      // address in an expired subnet may very well be part of another subnet.
    retry:
      if (auto [subnet, entry] = subnet_lookup(value); entry) {
        if (entry->is_expired(now)) {
          subnet_entries.erase(subnet);
          goto retry; // NOLINT(cppcoreguidelines-avoid-goto)
        }
        entry->refresh_read_timeout(now);
        builder.data(entry->raw_data);
        continue;
      }
      if (replace and not is<caf::none_t>(value)) {
        builder.data(value);
        continue;
      }
      builder.null();
    }
    return builder.finish();
  }

  auto apply(const series& array, session ctx) -> std::vector<series> override {
    TENZIR_UNUSED(ctx);
    auto builder = series_builder{};
    const auto now = time::clock::now();
    for (auto value : array.values()) {
      // TODO: This should really be making use of heterogeneous map lookups
      // instead of materializing, but we're not using `data` and `data_view`
      // directly here, but rather a custom wrapper around them to make mixed
      // number types
      if (auto it = context_entries.find(materialize(value));
          it != context_entries.end()) {
        if (it->second.is_expired(now)) {
          context_entries.erase(it);
          goto retry; // NOLINT(cppcoreguidelines-avoid-goto)
        }
        it.value().refresh_read_timeout(now);
        builder.data(it->second.raw_data);
        continue;
      }
      // We need to retry the lookup if we had an expired hit, as a matched IP
      // address that was expired may very well be part of another subnet.
    retry:
      if (auto [subnet, entry] = subnet_lookup(value); entry) {
        if (entry->is_expired(now)) {
          subnet_entries.erase(subnet);
          goto retry; // NOLINT(cppcoreguidelines-avoid-goto)
        }
        entry->refresh_read_timeout(now);
        builder.data(entry->raw_data);
        continue;
      }
      builder.null();
    }
    return builder.finish();
  }

  /// Inspects the context.
  auto show() const -> record override {
    // There's no size() function for the PATRICIA trie, so we walk the tree
    // nodes here once in O(n).
    auto num_subnet_entries = size_t{0};
    auto num_context_entries = size_t{0};
    const auto now = time::clock::now();
    for (auto entry : subnet_entries.nodes()) {
      TENZIR_ASSERT(entry.second);
      if (entry.second->is_expired(now)) {
        continue;
      }
      ++num_subnet_entries;
    }
    for (const auto& entry : context_entries) {
      if (entry.second.is_expired(now)) {
        continue;
      }
      ++num_context_entries;
    }
    return record{
      {"num_entries", num_context_entries + num_subnet_entries},
    };
  }

  auto dump() -> generator<table_slice> override {
    auto entry_builder = series_builder{};
    const auto now = time::clock::now();
    for (const auto& [key, value] : subnet_entries.nodes()) {
      if (value->is_expired(now)) {
        continue;
      }
      TENZIR_ASSERT(value);
      auto row = entry_builder.record();
      row.field("key", data{key});
      row.field("value", value->raw_data);
      if (entry_builder.length() >= context::dump_batch_size_limit) {
        for (auto&& slice : entry_builder.finish_as_table_slice(
               fmt::format("tenzir.{}.info", context_type()))) {
          co_yield std::move(slice);
        }
      }
    }
    for (const auto& [key, value] : context_entries) {
      if (value.is_expired(now)) {
        continue;
      }
      auto row = entry_builder.record();
      row.field("key", key.to_original_data());
      row.field("value",
                value.raw_data); // should we also get timeout info in dump?
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

  /// Updates the context.
  auto legacy_update(table_slice slice, context_parameter_map parameters)
    -> caf::expected<context_update_result> override {
    // context does stuff on its own with slice & parameters
    TENZIR_ASSERT(slice.rows() != 0);
    if (as<tenzir::record_type>(slice.schema()).num_fields() == 0) {
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
      context_value.read_timeout = now + *update_timeout;
      context_value.read_timeout_duration = *update_timeout;
    }
    auto erase = parameters.contains("erase");
    if (erase and parameters["erase"].has_value()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("'erase' option must not have a "
                                         "value; found '{}'",
                                         *parameters["erase"]));
    }
    // TODO erase makes it so that we always ignore timeouts, so we should
    // probably not allow it
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
    auto key_values = values(key_type, *key_array);
    auto key_values_list = list{};
    if (erase) {
      // Subnets never make it into the regular map of entries.
      if (is<subnet_type>(key_type)) {
        for (const auto& key :
             values(subnet_type{},
                    as<tenzir::subnet_type::array_type>(*key_array))) {
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
      return context_update_result{
        .make_query = {},
      };
    }
    auto context_array = std::static_pointer_cast<arrow::Array>(
      check(to_record_batch(slice)->ToStructArray()));
    auto context_values = values(slice.schema(), *context_array);
    auto key_it = key_values.begin();
    auto context_it = context_values.begin();
    while (key_it != key_values.end()) {
      value_data value_val = context_value;
      TENZIR_ASSERT(context_it != context_values.end());
      value_val.raw_data = materialize(*context_it);
      auto materialized_key = materialize(*key_it);
      // Subnets never make it into the regular map of entries.
      if (is<subnet_type>(key_type)) {
        const auto& key = as<tenzir::subnet>(materialized_key);
        subnet_entries.insert(key, std::move(value_val));
      } else {
        context_entries.insert_or_assign(materialized_key,
                                         std::move(value_val));
      }
      key_values_list.emplace_back(std::move(materialized_key));
      ++key_it;
      ++context_it;
    }
    TENZIR_ASSERT(context_it == context_values.end());
    auto query_f
      = [key_values_list = std::move(key_values_list)](
          context_parameter_map, const std::vector<std::string>& fields)
      -> caf::expected<std::vector<expression>> {
      auto result = std::vector<expression>{};
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
    return context_update_result{
      .make_query = std::move(query_f),
    };
  }

  auto update(const table_slice& events, const context_update_args& args,
              session ctx) -> failure_or<context_update_result> override {
    auto keys = eval(args.key, events, ctx);
    auto key_values_list = list{};
    const auto update_entry = [&, now = time::clock::now()](
                                bool created, value_data& entry, data context) {
      entry.raw_data = std::move(context);
      if (created and args.create_timeout) {
        entry.create_timeout = now + args.create_timeout->inner;
      }
      if (args.write_timeout) {
        entry.write_timeout = now + args.write_timeout->inner;
      }
      if (args.read_timeout) {
        entry.read_timeout = now + args.read_timeout->inner;
        entry.read_timeout_duration = args.read_timeout->inner;
      }
    };
    auto context
      = eval(args.value.value_or(ast::this_{location::unknown}), events, ctx);
    auto context_gen = context.values();
    for (const auto& key : keys.values()) {
      auto materialized_key = materialize(key);
      auto context = context_gen.next();
      TENZIR_ASSERT(context);
      if (const auto* sn = try_as<tenzir::subnet>(&materialized_key)) {
        const auto created = subnet_entries.insert(*sn, value_data{});
        auto* entry = subnet_entries.lookup(*sn);
        TENZIR_ASSERT(entry);
        update_entry(created, *entry, materialize(*context));
      } else {
        auto [entry, created]
          = context_entries.emplace(materialized_key, value_data{});
        TENZIR_ASSERT(entry != context_entries.end());
        update_entry(created, entry.value(), materialize(*context));
      }
      key_values_list.emplace_back(std::move(materialized_key));
    }
    auto make_query
      = [key_values_list = std::move(key_values_list)](
          context_parameter_map, const std::vector<std::string>& fields)
      -> caf::expected<std::vector<expression>> {
      auto result = std::vector<expression>{};
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
    return context_update_result{
      .make_query = std::move(make_query),
    };
  }

  auto erase(const table_slice& events, const context_erase_args& args,
             session ctx) -> failure_or<void> override {
    auto keys = eval(args.key, events, ctx);
    for (auto&& key : keys) {
      if (is<null_type>(key.type)) {
        continue;
      }
      if (is<subnet_type>(key.type)) {
        for (const auto& key : key.values<subnet_type>()) {
          if (not key) {
            continue;
          }
          subnet_entries.erase(*key);
        }
        return {};
      }
      for (const auto& x : key.values()) {
        if (is<caf::none_t>(x)) {
          continue;
        }
        context_entries.erase(materialize(x));
      }
    }
    return {};
  }

  auto reset() -> caf::expected<void> override {
    context_entries.clear();
    subnet_entries.clear();
    return {};
  }

  auto save() const -> caf::expected<context_save_result> override {
    // We save the context by formatting into a record of this format:
    //   [{key: key, value: value}, ...]
    auto builder = flatbuffers::FlatBufferBuilder{};
    auto value_offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
    value_offsets.reserve(context_entries.size());
    auto now = time::clock::now();
    auto pack_value_data = [&](const data& key, const value_data& value) {
      if (value.is_expired(now)) {
        return;
      }
      auto field_offsets
        = std::vector<flatbuffers::Offset<fbs::data::RecordField>>{};
      field_offsets.reserve(5);
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, builder.CreateSharedString("key"), pack(builder, key)));
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, builder.CreateSharedString("value"),
        pack(builder, value.raw_data)));
      if (value.create_timeout) {
        field_offsets.emplace_back(fbs::data::CreateRecordField(
          builder, builder.CreateSharedString("create-timeout"),
          pack(builder, data{*value.create_timeout})));
      }
      if (value.write_timeout) {
        field_offsets.emplace_back(fbs::data::CreateRecordField(
          builder, builder.CreateSharedString("write-timeout"),
          pack(builder, data{*value.write_timeout})));
      }
      if (value.read_timeout) {
        field_offsets.emplace_back(fbs::data::CreateRecordField(
          builder, builder.CreateSharedString("read-timeout"),
          pack(builder, data{*value.read_timeout})));
        field_offsets.emplace_back(fbs::data::CreateRecordField(
          builder, builder.CreateSharedString("read-timeout-duration"),
          pack(builder, data{*value.read_timeout_duration})));
      }
      const auto record_offset
        = fbs::data::CreateRecordDirect(builder, &field_offsets);
      value_offsets.emplace_back(fbs::CreateData(
        builder, fbs::data::Data::record, record_offset.Union()));
    };
    for (const auto& [key, value] : context_entries) {
      pack_value_data(key.to_original_data(), value);
    }
    for (const auto& [key, value] : subnet_entries.nodes()) {
      TENZIR_ASSERT(value);
      pack_value_data(data{key}, *value);
    }
    const auto list_offset
      = fbs::data::CreateListDirect(builder, &value_offsets);
    const auto data_offset
      = fbs::CreateData(builder, fbs::data::Data::list, list_offset.Union());
    fbs::FinishDataBuffer(builder, data_offset);
    return context_save_result{.data = chunk::make(builder.Release()),
                               .version = 1};
  }

private:
  map_type context_entries;
  subnet_tree_type subnet_entries;
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
    auto subnet_entries = subnet_tree_type{};
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
    const auto now = time::clock::now();
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
      auto key = data{};
      auto value = value_data{};
      for (const auto& field : *record->fields()) {
        TENZIR_ASSERT(field->name());
        TENZIR_ASSERT(field->data());
        if (field->name()->string_view() == "key") {
          if (auto err = unpack(*field->data(), key)) {
            return caf::make_error(ec::serialization_error,
                                   fmt::format("failed to deserialize lookup "
                                               "table context: invalid key: {}",
                                               err));
          }
          continue;
        }
        if (field->name()->string_view() == "value") {
          if (auto err = unpack(*field->data(), value.raw_data)) {
            return caf::make_error(ec::serialization_error,
                                   fmt::format("failed to deserialize lookup "
                                               "table context: invalid value: "
                                               "{}",
                                               err));
          }
          continue;
        }
        if (field->name()->string_view() == "create-timeout") {
          auto create_timeout = data{};
          if (auto err = unpack(*field->data(), create_timeout)) {
            return caf::make_error(ec::serialization_error,
                                   fmt::format("failed to deserialize lookup "
                                               "table context: invalid "
                                               "create-timeout: {}",
                                               err));
          }
          const auto* create_timeout_time
            = try_as<tenzir::time>(&create_timeout);
          if (not create_timeout_time) {
            return caf::make_error(ec::serialization_error,
                                   "failed to deserialize lookup table "
                                   "context: invalid create-timeout "
                                   "must be a time");
          }
          value.create_timeout = *create_timeout_time;
          continue;
        }
        if (field->name()->string_view() == "write-timeout") {
          auto write_timeout = data{};
          if (auto err = unpack(*field->data(), write_timeout)) {
            return caf::make_error(ec::serialization_error,
                                   fmt::format("failed to deserialize lookup "
                                               "table context: invalid "
                                               "write-timeout: {}",
                                               err));
          }
          const auto* write_timeout_time = try_as<tenzir::time>(&write_timeout);
          if (not write_timeout_time) {
            return caf::make_error(ec::serialization_error,
                                   "failed to deserialize lookup table "
                                   "context: invalid write-timeout "
                                   "must be a time");
          }
          value.write_timeout = *write_timeout_time;
          continue;
        }
        if (field->name()->string_view() == "read-timeout"
            or field->name()->string_view() == "update-timeout") {
          auto read_timeout = data{};
          if (auto err = unpack(*field->data(), read_timeout)) {
            return caf::make_error(ec::serialization_error,
                                   fmt::format("failed to deserialize lookup "
                                               "table context: invalid "
                                               "read-timeout: {}",
                                               err));
          }
          const auto* read_timeout_time = try_as<tenzir::time>(&read_timeout);
          if (not read_timeout_time) {
            return caf::make_error(ec::serialization_error,
                                   "failed to deserialize lookup table "
                                   "context: invalid read-timeout "
                                   "must be a time");
          }
          value.read_timeout = *read_timeout_time;
          continue;
        }
        if (field->name()->string_view() == "read-timeout-duration"
            or field->name()->string_view() == "update-duration") {
          auto read_timeout_duration = data{};
          if (auto err = unpack(*field->data(), read_timeout_duration)) {
            return caf::make_error(ec::serialization_error,
                                   fmt::format("failed to deserialize lookup "
                                               "table context: invalid "
                                               "read-timeout-duration: {}",
                                               err));
          }
          const auto* read_timeout_duration_duration
            = try_as<tenzir::duration>(&read_timeout_duration);
          if (not read_timeout_duration_duration) {
            return caf::make_error(ec::serialization_error,
                                   "failed to deserialize lookup table "
                                   "context: invalid read-timeout-duration "
                                   "must be a time");
          }
          value.read_timeout_duration = *read_timeout_duration_duration;
          continue;
        }
        return caf::make_error(ec::serialization_error,
                               fmt::format("failed to deserialize lookup table "
                                           "context: unexpected key {}",
                                           field->name()->string_view()));
      }
      if (value.read_timeout.has_value()
          != value.read_timeout_duration.has_value()) {
        return caf::make_error(ec::serialization_error,
                               "failed to deserialize lookup table context: "
                               "read-timeout and read-timeout-duration must be "
                               "either both set or both unset");
      }
      if (value.is_expired(now)) {
        continue;
      }
      if (const auto* x = try_as<tenzir::subnet>(&key)) {
        subnet_entries.insert(*x, std::move(value));
      } else {
        context_entries.emplace(std::move(key), std::move(value));
      }
    }
    return std::make_unique<lookup_table_context>(std::move(context_entries),
                                                  std::move(subnet_entries));
  }
};

class plugin : public virtual context_factory_plugin<"lookup-table"> {
  auto initialize(const record&, const record&) -> caf::error override {
    register_loader(std::make_unique<v1_loader>());
    return caf::none;
  }

  auto make_context(context_parameter_map) const
    -> caf::expected<std::unique_ptr<context>> override {
    return std::make_unique<lookup_table_context>();
  }

  auto make_context(invocation inv, session ctx) const
    -> failure_or<make_context_result> override {
    auto name = located<std::string>{};
    auto parser = argument_parser2::context("lookup-table");
    parser.positional("name", name);
    TRY(parser.parse(inv, ctx));
    return make_context_result{
      std::move(name),
      std::make_unique<lookup_table_context>(),
    };
  }
};

} // namespace

} // namespace tenzir::plugins::lookup_table

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lookup_table::plugin)
