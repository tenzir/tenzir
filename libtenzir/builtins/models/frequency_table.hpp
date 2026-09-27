//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/checked_math.hpp>
#include <tenzir/detail/distribution.hpp>
#include <tenzir/model.hpp>
#include <tenzir/nova/aggregation/value_counts.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/si_literals.hpp>

#include <algorithm>
#include <cmath>

#include "model_fields.hpp"

namespace tenzir::plugins::frequency_table::native {

using namespace nova;

/// Refines nulls and empty lists without coercing keys or changing record shapes.
inline auto combine_types(type const& lhs, type const& rhs) -> Option<type> {
  if (lhs == rhs or is<null_type>(rhs)) {
    return lhs;
  }
  if (is<null_type>(lhs)) {
    return rhs;
  }
  if (auto l = try_as<list_type>(lhs)) {
    auto r = try_as<list_type>(rhs);
    if (not r) {
      return None{};
    }
    auto element = combine_types(l->value_type(), r->value_type());
    return element ? Option<type>{list_type{*element}} : None{};
  }
  if (auto l = try_as<record_type>(lhs)) {
    auto r = try_as<record_type>(rhs);
    if (not r or l->num_fields() != r->num_fields()) {
      return None{};
    }
    auto fields = std::vector<struct record_type::field>{};
    for (auto i = size_t{0}; i < l->num_fields(); ++i) {
      auto a = l->field(i);
      auto b = r->field(i);
      if (a.name != b.name) {
        return None{};
      }
      auto field = combine_types(a.type, b.type);
      if (not field) {
        return None{};
      }
      fields.push_back({std::string{a.name}, *field});
    }
    return type{record_type{fields}};
  }
  return None{};
}

inline auto key_type(RowView<Data> value) -> Result<type, std::string> {
  return match(
    value, []<class Tag>(RowView<Tag> value) -> Result<type, std::string> {
      if constexpr (std::same_as<Tag, Null>) {
        return type{null_type{}};
      } else if constexpr (std::same_as<Tag, Secret>) {
        // Keys are part of the model, so counting secrets would reveal them.
        return Err{"values must not contain secrets"};
      } else if constexpr (std::same_as<Tag, List>) {
        auto result = type{null_type{}};
        for (auto element : value) {
          TRY(auto t, key_type(element));
          auto next = combine_types(result, t);
          if (not next) {
            return Err{"list keys must contain compatible value types"};
          }
          result = *next;
        }
        return type{list_type{result}};
      } else if constexpr (std::same_as<Tag, Record>) {
        auto fields = std::vector<struct record_type::field>{};
        for (auto [name, field] : value) {
          TRY(auto t, key_type(field));
          fields.push_back({std::string{name}, std::move(t)});
        }
        return type{record_type{fields}};
      } else {
        if constexpr (std::same_as<Tag, Float>) {
          if (not std::isfinite(*value)) {
            return Err{"values must not contain NaN or infinity"};
          }
        }
        return type{data_to_type_t<Tag>{}};
      }
    });
}

struct FrequencyModel {
  uint64_t input_count = 0;
  uint64_t count = 0;
  uint64_t null_count = 0;
  Option<type> key_type;
  tsl::robin_map<Data, uint64_t, nova_value_counts::ValueHash,
                 nova_value_counts::ValueEqual>
    counts;

  auto add(RowView<Data> value, uint64_t amount = 1, bool unique = false)
    -> Result<void, std::string> {
    using namespace si_literals;
    auto input = checked_add(input_count, amount);
    if (not input) {
      return Err{"`input_count` overflow"};
    }
    if (model_fields::is_null(value)) {
      if (unique) {
        return Err{"frequency-table values must not be null"};
      }
      // Both classified counters are bounded by input_count.
      null_count += amount;
      input_count = *input;
      return {};
    }
    TRY(auto incoming, native::key_type(value));
    auto combined
      = key_type ? combine_types(*key_type, incoming) : Option{incoming};
    if (not combined) {
      return Err{fmt::format("cannot combine values of types `{}` and `{}`",
                             *key_type, incoming)};
    }
    auto it = counts.find(value);
    if (it == counts.end()) {
      if (counts.size() >= 1_Mi) {
        return Err{fmt::format("more than {} distinct values", 1_Mi)};
      }
      counts.emplace(to_data(value), amount);
    } else {
      if (unique) {
        return Err{"frequency-table values must be unique"};
      }
      it.value() += amount;
    }
    input_count = *input;
    count += amount;
    key_type = std::move(combined);
    return {};
  }

  auto get() const -> Data {
    // Keep the established total order for equally frequent keys. Materialize
    // only the distinct keys used by this output sort, never the input column.
    struct Entry {
      Data const* value;
      uint64_t count;
      data order;
    };
    auto entries = std::vector<Entry>{};
    entries.reserve(counts.size());
    for (auto const& [value, count] : counts) {
      entries.push_back({&value, count, nova::materialize_legacy(value)});
    }
    std::ranges::sort(entries, [](Entry const& lhs, Entry const& rhs) {
      return lhs.count != rhs.count ? lhs.count > rhs.count
                                    : lhs.order < rhs.order;
    });
    auto values = List{};
    for (auto const& entry : entries) {
      values.push_back(Record{{"value", *entry.value}, {"count", entry.count}});
    }
    return Record{
      {"model", std::string{"frequency_table"}},
      {"version", uint64_t{1}},
      {"input_count", input_count},
      {"count", count},
      {"null_count", null_count},
      {"value_type",
       key_type ? fmt::to_string(*key_type) : std::string{"null"}},
      {"values", std::move(values)},
    };
  }
};

inline auto parse(RowView<Record> record)
  -> Result<FrequencyModel, std::string> {
  TRY(auto envelope, parse_model_envelope(record));
  if (envelope.model != "frequency_table" or envelope.version != 1) {
    return Err{"expected a frequency-table model of version 1"};
  }
  TRY(auto values, model_fields::get<List>(record, "values"));
  auto result = FrequencyModel{};
  for (auto value : values) {
    auto item = try_as<RowView<Record>>(value);
    if (not item) {
      return Err{"invalid frequency-table model shape"};
    }
    auto key = model_fields::field(*item, "value");
    if (not key) {
      return Err{"missing field `value`"};
    }
    TRY(auto count, model_fields::get_uint(*item, "count"));
    if (count == 0) {
      return Err{"frequency-table counts must be positive"};
    }
    TRY(result.add(*key, count, true));
  }
  auto total = checked_add(result.count, envelope.null_count);
  if (result.count != envelope.count or not total
      or *total != envelope.input_count) {
    return Err{"inconsistent frequency-table counters"};
  }
  result.input_count = envelope.input_count;
  result.null_count = envelope.null_count;
  return result;
}

class FrequencyMergeState final : public ModelMergeState {
public:
  explicit FrequencyMergeState(FrequencyModel state)
    : state_{std::move(state)} {
  }

  auto merge(RowView<Record> record) -> Result<void, std::string> override {
    TRY(auto incoming, parse(record));
    // Work on a copy: neither capacity nor overflow failures may partially merge.
    auto merged = state_;
    for (auto const& [value, count] : incoming.counts) {
      TRY(merged.add(RowView<Data>{value}, count));
    }
    auto input = checked_add(merged.input_count, incoming.null_count);
    if (not input) {
      return Err{"counter overflow"};
    }
    merged.input_count = *input;
    merged.null_count += incoming.null_count;
    state_ = std::move(merged);
    return {};
  }

  auto get() const -> Data override {
    return state_.get();
  }

private:
  FrequencyModel state_;
};

inline auto divergence(RowView<Record> lhs, RowView<Record> rhs)
  -> Result<Option<double>, ModelComparisonError> {
  TRY(auto p, parse(lhs));
  TRY(auto q, parse(rhs).map_err(ModelComparisonError::from_rhs));
  if (p.count == 0 or q.count == 0) {
    return None{};
  }
  if (not combine_types(*p.key_type, *q.key_type)) {
    return Err{
      fmt::format("incompatible frequency-table key types `{}` and `{}`",
                  *p.key_type, *q.key_type)};
  }
  auto result = detail::jensen_shannon_accumulator{
    static_cast<double>(p.count), static_cast<double>(q.count)};
  for (auto const& [value, count] : p.counts) {
    auto it = q.counts.find(value);
    result.add(count, it == q.counts.end() ? 0 : it.value());
  }
  for (auto const& [value, count] : q.counts) {
    if (not p.counts.contains(value)) {
      result.add(0, count);
    }
  }
  return Option{result.value()};
}

struct FrequencyArgs {
  ValueArgument x;
};

class FrequencyCounts {
public:
  auto add(RowView<Data> value, location source, diagnostic_handler& dh,
           WarnOnce& warning) -> void {
    if (failed_) {
      return;
    }
    auto result = state_.add(value);
    if (not result) {
      failed_ = true;
      warning(dh, diagnostic::warning("`frequency_table` failed: {}",
                                      result.unwrap_err())
                    .primary(source));
    }
  }

  auto get() const -> Data {
    return failed_ ? Data{} : state_.get();
  }

private:
  FrequencyModel state_;
  bool failed_ = false;
};

class FrequencyFunction {
public:
  static auto eval(FrequencyArgs const& args, EvalFrame frame) -> Array<Data> {
    auto dh = model_fields::DeduplicatingHandler{frame};
    return aggregate_lists(args.x, frame,
                           [&](ListElements const& elements,
                               ArrayBuilder<Data>& out) {
                             auto counts = FrequencyCounts{};
                             auto warning = WarnOnce{};
                             elements.for_each([&](auto value) {
                               counts.add(value, args.x.source, dh, warning);
                             });
                             append_data(out, counts.get());
                           });
  }

  auto update(FrequencyArgs const& args, EvalFrame frame) -> void {
    if (not counts_) {
      counts_.emplace();
    }
    model_fields::for_each(args.x.data, frame.mask(), [&](auto value) {
      counts_->add(value, args.x.source, frame, warning_);
    });
  }

  auto get() const -> Data {
    return counts_ ? counts_->get() : Data{};
  }

  auto reset() -> void {
    // Warning latches belong to the instance, not the resettable model.
    counts_.reset();
  }

private:
  Option<FrequencyCounts> counts_;
  WarnOnce warning_;
};

struct CountArgs {
  ValueArgument model;
  ValueArgument x;
};

class CountFunction {
public:
  static auto eval(CountArgs const& args, EvalFrame frame) -> Array<Data> {
    auto out = ArrayBuilder<Data>{};
    auto records = args.model.data.get_alternative<Record>();
    for (auto row : storage::true_bits(frame.mask())) {
      out.skip_n(row - out.length());
      auto query = args.x.data.get(row);
      if (not records or not records->present.get(row)
          or model_fields::is_null(query)) {
        out.null();
        continue;
      }
      auto parsed = parse(records->data.get(row));
      if (not parsed) {
        out.null();
        continue;
      }
      auto const& model = parsed.unwrap();
      auto query_type = key_type(query);
      if (not query_type or not model.key_type
          or not combine_types(*model.key_type, query_type.unwrap())) {
        out.data(uint64_t{0});
        continue;
      }
      auto found = model.counts.find(query);
      out.data(found == model.counts.end() ? uint64_t{0} : found.value());
    }
    out.skip_n(frame.length() - out.length());
    return out.finish();
  }
};

} // namespace tenzir::plugins::frequency_table::native
