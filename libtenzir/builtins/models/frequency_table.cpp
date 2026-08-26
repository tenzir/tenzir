//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/blob.hpp>
#include <tenzir/checked_math.hpp>
#include <tenzir/detail/distribution.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/model.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <string_view>
#include <utility>
#include <vector>

#include "value_counts.hpp"

namespace tenzir::plugins::frequency_table {

namespace {

using namespace si_literals;

constexpr auto model_name = std::string_view{"frequency_table"};
constexpr auto model_version = uint64_t{1};
constexpr auto checkpoint_key_type_field
  = std::string_view{"_checkpoint_key_type"};

/// The maximum number of distinct values a frequency table may hold. Unlike
/// the approximate models, an exact table grows with the cardinality of its
/// input, so it needs an explicit bound.
constexpr auto max_values = uint64_t{1_Mi};

using counts_state = detail::value_counts_state<uint64_t>;

template <class Value>
auto contains_non_finite(Value const& value) -> bool {
  return match(value, []<class T>(T const& value) {
    if constexpr (std::same_as<T, double>) {
      return not std::isfinite(value);
    } else if constexpr (std::same_as<T, list> or std::same_as<T, list_view3>) {
      for (auto const& item : value) {
        if (contains_non_finite(item)) {
          return true;
        }
      }
    } else if constexpr (std::same_as<T, map>) {
      for (auto const& [key, item] : value) {
        if (contains_non_finite(key) or contains_non_finite(item)) {
          return true;
        }
      }
    } else if constexpr (std::same_as<T, record>
                         or std::same_as<T, record_view3>) {
      for (auto const& [_, item] : value) {
        if (contains_non_finite(item)) {
          return true;
        }
      }
    }
    return false;
  });
}

struct model {
  uint64_t input_count = 0;
  uint64_t count = 0;
  uint64_t null_count = 0;
  /// The key type derived from the stored values. Empty for an empty table.
  Option<type> key_type;
  counts_state counts;
};

struct entry {
  data value;
  uint64_t count = 0;
};

auto sorted_entries(counts_state const& state) -> std::vector<entry> {
  auto result = std::vector<entry>{};
  result.reserve(state.counts().size());
  for (auto const& [value, count] : state.counts()) {
    result.push_back(entry{value, count});
  }
  std::ranges::sort(result, [](entry const& lhs, entry const& rhs) {
    if (lhs.count != rhs.count) {
      return lhs.count > rhs.count;
    }
    return lhs.value < rhs.value;
  });
  return result;
}

auto make_record(model const& value) -> data {
  auto const value_type
    = value.key_type ? fmt::to_string(*value.key_type) : std::string{"null"};
  auto values = list{};
  values.reserve(value.counts.counts().size());
  for (auto& item : sorted_entries(value.counts)) {
    values.emplace_back(record{
      {"value", std::move(item.value)},
      {"count", item.count},
    });
  }
  return record{
    {"model", std::string{model_name}}, {"version", model_version},
    {"input_count", value.input_count}, {"count", value.count},
    {"null_count", value.null_count},   {"value_type", value_type},
    {"values", std::move(values)},
  };
}

auto result_type(type const& value_type) -> type {
  return model_record_type({
    {"value_type", string_type{}},
    {"values", list_type{record_type{
                 {"value", value_type},
                 {"count", uint64_type{}},
               }}},
  });
}

auto insert_entries(model& result, std::vector<entry> entries)
  -> Result<void, std::string> {
  if (entries.size() > max_values) {
    return Err{fmt::format("a frequency table must not exceed {} distinct "
                           "values",
                           max_values)};
  }
  auto const infer_key_types = not result.key_type;
  result.counts.reserve(entries.size());
  for (auto& item : entries) {
    if (is<caf::none_t>(item.value)) {
      return Err{"frequency-table values must not be null"};
    }
    if (contains_non_finite(item.value)) {
      return Err{"frequency-table values must not contain NaN or infinity"};
    }
    if (infer_key_types) {
      auto item_type = type::infer(item.value);
      if (not item_type) {
        return Err{"frequency-table values must have a known type"};
      }
      auto canonical_type = item_type->prune();
      if (not result.key_type) {
        result.key_type = std::move(canonical_type);
      } else if (*result.key_type != canonical_type) {
        return Err{"frequency-table values must all have the same type"};
      }
    }
    if (not result.counts.insert(std::move(item.value), item.count)) {
      return Err{"frequency-table values must be unique"};
    }
  }
  return {};
}

auto model_field(record_view3 record, std::string_view name)
  -> Option<data_view3> {
  for (auto const& [field_name, value] : record) {
    if (field_name == name) {
      return value;
    }
  }
  return None{};
}

auto model_field(record const& record, std::string_view name)
  -> Option<data const&> {
  auto const it = record.find(name);
  return it == record.end() ? Option<data const&>{None{}}
                            : Option<data const&>{it->second};
}

auto model_list(data_view3 value) -> Option<list_view3> {
  auto const* result = try_as<list_view3>(value);
  return result ? Option{*result} : None{};
}

auto model_list(data const& value) -> Option<list const&> {
  auto const* result = try_as<list>(&value);
  return result ? Option<list const&>{*result} : None{};
}

auto model_record(data_view3 value) -> Option<record_view3> {
  auto const* result = try_as<record_view3>(value);
  return result ? Option{*result} : None{};
}

auto model_record(data const& value) -> Option<record const&> {
  auto const* result = try_as<record>(&value);
  return result ? Option<record const&>{*result} : None{};
}

auto checkpoint_key_type(record const& record)
  -> Result<Option<type>, std::string> {
  auto const field = model_field(record, checkpoint_key_type_field);
  if (not field) {
    return None{};
  }
  auto const* bytes = try_as<blob>(&*field);
  if (not bytes) {
    return Err{"invalid frequency-table checkpoint key type"};
  }
  auto table = flatbuffer<fbs::Type>::make(
    chunk::copy(std::span<const std::byte>{bytes->data(), bytes->size()}));
  if (not table) {
    return Err{"invalid frequency-table checkpoint key type"};
  }
  return Option<type>{type{std::move(*table)}};
}

auto model_value_type(record_view3 record, data_view3) -> Option<type> {
  return record.field_type("value");
}

auto model_value_type(record const&, data const& value) -> Option<type> {
  auto result = type::infer(value);
  return result ? Option{std::move(*result)} : None{};
}

auto materialize_model_value(data_view3 value) -> data {
  return materialize(value);
}

auto materialize_model_value(data const& value) -> data {
  return value;
}

template <class Record>
auto parse_model_impl(Record const& record, Option<type> checkpoint_key_type)
  -> Result<model, std::string> {
  TRY(auto envelope, parse_model_envelope(record));
  if (envelope.model != model_name) {
    return Err{
      fmt::format("expected model `{}`, got `{}`", model_name, envelope.model)};
  }
  if (envelope.version != model_version) {
    return Err{
      fmt::format("unsupported frequency-table model version {}; expected {}",
                  envelope.version, model_version)};
  }
  auto values_field = model_field(record, "values");
  if (not values_field) {
    return Err{"missing field `values`"};
  }
  auto values = model_list(*values_field);
  if (not values) {
    return Err{"`values` must be a list of records"};
  }
  auto result = model{
    .input_count = envelope.input_count,
    .count = envelope.count,
    .null_count = envelope.null_count,
    .key_type = std::move(checkpoint_key_type),
    .counts = {},
  };
  result.counts.reserve(values->size());
  for (auto const& value : *values) {
    auto item = model_record(value);
    if (not item) {
      return Err{"invalid frequency-table model shape"};
    }
    auto item_value = model_field(*item, "value");
    auto item_count_field = model_field(*item, "count");
    if (not item_value or not item_count_field) {
      return Err{"invalid frequency-table model shape"};
    }
    TRY(auto item_count, model_uint64(*item_count_field));
    auto materialized_value = materialize_model_value(*item_value);
    if (not result.key_type) {
      auto item_type = model_value_type(*item, *item_value);
      if (item_type) {
        result.key_type = item_type->prune();
      }
    }
    auto inserted
      = result.counts.insert(std::move(materialized_value), item_count);
    TENZIR_ASSERT(inserted);
  }
  return result;
}

auto parse_model(record_view3 record) -> Result<model, std::string> {
  return parse_model_impl(record, None{});
}

auto parse_model(record const& record) -> Result<model, std::string> {
  TRY(auto key_type, checkpoint_key_type(record));
  return parse_model_impl(record, std::move(key_type));
}

auto jensen_shannon(model const& lhs, model const& rhs) -> double {
  auto result = detail::jensen_shannon_accumulator{
    static_cast<double>(lhs.count), static_cast<double>(rhs.count)};
  for (auto const& [value, count] : lhs.counts.counts()) {
    auto const it = rhs.counts.counts().find(value);
    result.add(count, it == rhs.counts.counts().end() ? 0 : it.value());
  }
  for (auto const& [value, count] : rhs.counts.counts()) {
    if (not lhs.counts.counts().contains(value)) {
      result.add(0, count);
    }
  }
  return result.value();
}

class merge_state final : public model_merge_state {
public:
  explicit merge_state(model state) : state_{std::move(state)} {
  }

  auto merge(record_view3 value) -> Result<void, std::string> override {
    TRY(auto incoming, parse_model(value));
    if (state_.key_type and incoming.key_type
        and *state_.key_type != *incoming.key_type) {
      return Err{fmt::format("cannot merge frequency tables with value types "
                             "`{}` and `{}`",
                             *state_.key_type, *incoming.key_type)};
    }
    auto input_count = checked_add(state_.input_count, incoming.input_count);
    auto count = checked_add(state_.count, incoming.count);
    auto null_count = checked_add(state_.null_count, incoming.null_count);
    if (not input_count or not count or not null_count) {
      return Err{"counter overflow"};
    }
    auto merged = state_.counts;
    for (auto const& [value, value_count] : incoming.counts.counts()) {
      auto it = merged.counts().find(value);
      if (it == merged.counts().end()) {
        if (merged.counts().size() >= max_values) {
          return Err{fmt::format("merged table would exceed {} distinct values",
                                 max_values)};
        }
        if (not merged.insert(value, value_count)) {
          return Err{"failed to insert a new value count"};
        }
        continue;
      }
      auto next = checked_add(it.value(), value_count);
      if (not next) {
        return Err{"value count overflows"};
      }
      it.value() = *next;
    }
    state_.input_count = *input_count;
    state_.count = *count;
    state_.null_count = *null_count;
    if (state_.counts.counts().empty()) {
      state_.key_type = incoming.key_type;
    }
    state_.counts = std::move(merged);
    return {};
  }

  auto get() const -> data override {
    return make_record(state_);
  }

  auto get_for_checkpoint() const -> data override {
    auto result = get();
    if (state_.key_type) {
      as<record>(result).emplace(std::string{checkpoint_key_type_field},
                                 blob{as_bytes(*state_.key_type)});
    }
    return result;
  }

private:
  model state_;
};

class instance final : public aggregation_instance {
public:
  explicit instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(table_slice const& input, session ctx) -> void override {
    if (failed_) {
      return;
    }
    for (auto& arg : eval(expr_, input, ctx)) {
      auto const has_values = arg.array->length() > arg.array->null_count();
      auto const key_type = arg.type.prune();
      if (has_values and state_.key_type and *state_.key_type != key_type) {
        fail(fmt::format("cannot combine values of types `{}` and `{}`",
                         *state_.key_type, key_type),
             ctx);
        return;
      }
      for (auto value : values3(*arg.array)) {
        auto input_count = checked_add(state_.input_count, uint64_t{1});
        if (not input_count) {
          fail("`input_count` overflow", ctx);
          return;
        }
        if (is<caf::none_t>(value)) {
          auto null_count = checked_add(state_.null_count, uint64_t{1});
          if (not null_count) {
            fail("`null_count` overflow", ctx);
            return;
          }
          state_.input_count = *input_count;
          state_.null_count = *null_count;
          continue;
        }
        if (contains_non_finite(value)) {
          fail("values must not contain NaN or infinity", ctx);
          return;
        }
        auto count = checked_add(state_.count, uint64_t{1});
        if (not count) {
          fail("`count` overflow", ctx);
          return;
        }
        if (state_.counts.counts().size() >= max_values
            and not state_.counts.counts().contains(value)) {
          fail(fmt::format("more than {} distinct values", max_values), ctx);
          return;
        }
        if (not state_.counts.add(value)) {
          fail("value count overflow", ctx);
          return;
        }
        if (not state_.key_type) {
          state_.key_type = key_type;
        }
        state_.input_count = *input_count;
        state_.count = *count;
      }
    }
  }

  auto get() const -> data override {
    return failed_ ? data{} : make_record(state_);
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto offsets
      = std::vector<flatbuffers::Offset<fbs::aggregation::FrequencyTableEntry>>{};
    offsets.reserve(state_.counts.counts().size());
    for (auto const& item : sorted_entries(state_.counts)) {
      offsets.push_back(fbs::aggregation::CreateFrequencyTableEntry(
        fbb, pack(fbb, item.value), item.count));
    }
    auto values = fbb.CreateVector(offsets);
    auto key_type = state_.key_type ? *state_.key_type : type{null_type{}};
    auto key_type_bytes = as_bytes(key_type);
    auto fb_key_type = fbb.CreateVector(
      reinterpret_cast<uint8_t const*>(key_type_bytes.data()),
      key_type_bytes.size());
    auto table
      = fbs::aggregation::CreateFrequencyTable(fbb, values, state_.input_count,
                                               state_.count, state_.null_count,
                                               failed_, fb_key_type);
    fbb.Finish(table);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    auto fb
      = flatbuffer<fbs::aggregation::FrequencyTable>::make(std::move(chunk));
    if (not fb or not(*fb)->values() or not(*fb)->key_type()) {
      TENZIR_WARN("failed to restore `frequency_table` aggregation instance: "
                  "invalid FlatBuffer");
      return false;
    }
    auto const* nested_key_type = (*fb)->key_type_nested_root();
    TENZIR_ASSERT(nested_key_type);
    auto key_type = type{fb->slice(*nested_key_type, *(*fb)->key_type())};
    auto restored_key_type = Option<type>{};
    if (not is<null_type>(key_type)) {
      restored_key_type = key_type.prune();
    }
    auto restored = model{
      .input_count = (*fb)->input_count(),
      .count = (*fb)->count(),
      .null_count = (*fb)->null_count(),
      .key_type = std::move(restored_key_type),
      .counts = {},
    };
    auto entries = std::vector<entry>{};
    entries.reserve((*fb)->values()->size());
    auto total = uint64_t{0};
    for (auto const* item : *(*fb)->values()) {
      if (not item or not item->value() or item->count() == 0) {
        TENZIR_WARN("failed to restore `frequency_table` aggregation "
                    "instance: invalid value entry");
        return false;
      }
      auto value = data{};
      if (auto error = unpack(*item->value(), value); error) {
        TENZIR_WARN("failed to restore `frequency_table` aggregation "
                    "instance: {}",
                    error);
        return false;
      }
      if (is<caf::none_t>(value)) {
        TENZIR_WARN("failed to restore `frequency_table` aggregation "
                    "instance: null value");
        return false;
      }
      if (restored.key_type and not type_check(*restored.key_type, value)) {
        TENZIR_WARN("failed to restore `frequency_table` aggregation "
                    "instance: value does not match persisted key type");
        return false;
      }
      entries.push_back(entry{std::move(value), item->count()});
      auto next = checked_add(total, item->count());
      if (not next) {
        TENZIR_WARN("failed to restore `frequency_table` aggregation "
                    "instance: value count overflow");
        return false;
      }
      total = *next;
    }
    auto classified_count = checked_add(restored.count, restored.null_count);
    if (entries.empty() != (not restored.key_type) or total != restored.count
        or not classified_count or *classified_count != restored.input_count) {
      TENZIR_WARN("failed to restore `frequency_table` aggregation instance: "
                  "inconsistent counters");
      return false;
    }
    if (auto inserted = insert_entries(restored, std::move(entries));
        not inserted) {
      TENZIR_WARN("failed to restore `frequency_table` aggregation instance: "
                  "{}",
                  inserted.unwrap_err());
      return false;
    }
    state_ = std::move(restored);
    failed_ = (*fb)->failed();
    return true;
  }

  auto reset() -> void override {
    // Deliberately keep `warned_`: it deduplicates diagnostics over the
    // lifetime of the instance, not per row.
    state_ = {};
    failed_ = false;
  }

private:
  auto fail(std::string_view message, session ctx) -> void {
    failed_ = true;
    if (not warned_) {
      warned_ = true;
      diagnostic::warning("`frequency_table` failed: {}", message)
        .primary(expr_)
        .emit(ctx);
    }
  }

  ast::expression expr_;
  model state_;
  bool failed_ = false;
  bool warned_ = false;
};

class plugin final : public aggregation_plugin, public model_divergence_plugin {
public:
  auto name() const -> std::string override {
    return std::string{model_name};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto model_version() const -> uint64_t override {
    return ::tenzir::plugins::frequency_table::model_version;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return std::make_unique<instance>(std::move(expr));
  }

  auto list_call_result_type(type const& input_type) const
    -> Option<type> override {
    return result_type(input_type);
  }

  auto make_model_merge_state(record_view3 value) const
    -> Result<Box<model_merge_state>, std::string> override {
    TRY(auto parsed, parse_model(value));
    return Box<model_merge_state>{merge_state{std::move(parsed)}};
  }

  auto make_model_merge_state(record const& value) const
    -> Result<Box<model_merge_state>, std::string> override {
    TRY(auto parsed, parse_model(value));
    return Box<model_merge_state>{merge_state{std::move(parsed)}};
  }

  auto model_divergence(record_view3 lhs, record_view3 rhs,
                        std::string_view method) const
    -> Result<Option<double>, std::string> override {
    if (method != "jensen_shannon") {
      return Err{fmt::format(
        "model `{}` does not support divergence method `{}`", name(), method)};
    }
    auto lhs_model = parse_model(lhs);
    if (not lhs_model) {
      return Err{fmt::format("malformed frequency-table model: {}",
                             lhs_model.unwrap_err())};
    }
    auto rhs_model = parse_model(rhs);
    if (not rhs_model) {
      return Err{fmt::format("malformed frequency-table model: {}",
                             rhs_model.unwrap_err())};
    }
    auto lhs_value = std::move(lhs_model).unwrap();
    auto rhs_value = std::move(rhs_model).unwrap();
    if (lhs_value.count == 0 or rhs_value.count == 0) {
      return None{};
    }
    if (lhs_value.key_type != rhs_value.key_type) {
      return Err{fmt::format("incompatible frequency-table key types `{}` and "
                             "`{}`",
                             *lhs_value.key_type, *rhs_value.key_type)};
    }
    return Option{jensen_shannon(lhs_value, rhs_value)};
  }
};

class count final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "frequency_table_count";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto model_expr = ast::expression{};
    auto value_expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("model", model_expr, "record")
          .positional("x", value_expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [model_expr = std::move(model_expr), value_expr = std::move(value_expr)](
        evaluator eval, session) -> multi_series {
        return map_series(
          eval(model_expr), eval(value_expr),
          [&](series models, series values) -> series {
            auto builder = uint64_type::make_arrow_builder(arrow_memory_pool());
            if (is<null_type>(models.type)) {
              check(builder->AppendNulls(models.length()));
              return series{uint64_type{}, finish(*builder)};
            }
            auto const* actual_record_type
              = try_as<tenzir::record_type>(&models.type);
            if (not actual_record_type) {
              return series::null(uint64_type{}, models.length());
            }
            auto entries_type = actual_record_type->field("values");
            if (not entries_type) {
              return series::null(uint64_type{}, models.length());
            }
            auto const* entries_list_type
              = try_as<tenzir::list_type>(&*entries_type);
            if (not entries_list_type) {
              return series::null(uint64_type{}, models.length());
            }
            auto item_type = entries_list_type->value_type();
            auto const* item_record = try_as<tenzir::record_type>(&item_type);
            auto value_type
              = item_record ? item_record->field("value") : Option<type>{};
            if (not value_type or models.type != result_type(*value_type)) {
              return series::null(uint64_type{}, models.length());
            }
            auto records = models.as<tenzir::record_type>();
            TENZIR_ASSERT(records);
            auto entries_field = records->field("values");
            TENZIR_ASSERT(entries_field);
            auto entries = entries_field->as<list_type>();
            TENZIR_ASSERT(entries);
            auto entry_records = entries->list_values().as<record_type>();
            TENZIR_ASSERT(entry_records);
            auto entry_values = entry_records->field("value");
            auto entry_counts_field = entry_records->field("count");
            TENZIR_ASSERT(entry_values and entry_counts_field);
            auto entry_counts = entry_counts_field->as<uint64_type>();
            TENZIR_ASSERT(entry_counts);
            for (auto row = int64_t{0}; row < models.length(); ++row) {
              auto const query = view_at(*values.array, row);
              if (records->array->IsNull(row) or is<caf::none_t>(query)) {
                check(builder->AppendNull());
                continue;
              }
              if (entries->array->IsNull(row)) {
                check(builder->AppendNull());
                continue;
              }
              auto const begin = entries->array->value_offset(row);
              auto const end = entries->array->value_offset(row + 1);
              auto result = Option{uint64_t{0}};
              for (auto i = begin; i < end; ++i) {
                if (view_at(*entry_values->array, i) == query) {
                  if (entry_counts->array->IsNull(i)) {
                    result = None{};
                    break;
                  }
                  result = entry_counts->array->Value(i);
                  break;
                }
              }
              if (result) {
                check(builder->Append(*result));
              } else {
                check(builder->AppendNull());
              }
            }
            return series{uint64_type{}, finish(*builder)};
          });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::frequency_table

TENZIR_REGISTER_PLUGIN(tenzir::plugins::frequency_table::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::frequency_table::count)
