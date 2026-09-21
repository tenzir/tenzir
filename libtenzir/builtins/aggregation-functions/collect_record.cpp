//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/heterogeneous_string_hash.hpp>
#include <tenzir/nova/aggregation.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/stringify.hpp>
#include <tenzir/plugin.hpp>

#include <concepts>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::collect_record {
namespace {

using namespace nova;

struct CollectRecordArgs {
  ValueArgument entries;
  Option<ValueArgument> values;
};

/// Scalar calls borrow their input until the result is built; aggregations
/// own only the latest value of each key, not the batches they came from.
template <class Value>
class CollectedRecord {
public:
  auto put(std::string_view key, RowView<Data> value) -> void {
    auto stored = [&]() -> Value {
      if constexpr (std::same_as<Value, Data>) {
        auto builder = ArrayBuilder<Data>{};
        append_row(builder, value);
        return builder.take_last();
      } else {
        return value;
      }
    }();
    auto it = indices_.find(key);
    if (it != indices_.end()) {
      fields_[it->second].second = std::move(stored);
      return;
    }
    indices_.emplace(std::string{key}, fields_.size());
    fields_.emplace_back(key, std::move(stored));
  }

  auto append_to(ArrayBuilder<Data>& builder) const -> void {
    auto record = builder.record();
    for (auto const& [key, value] : fields_) {
      append_row(record.field(key), RowView<Data>{value});
    }
  }

private:
  detail::heterogeneous_string_hashmap<size_t> indices_;
  std::vector<std::pair<std::string, Value>> fields_;
};

/// Decode each entry in encounter order, including interleaved union types.
/// Record field names are special only for the exact {key, value} shape.
class EntryDecoder {
public:
  EntryDecoder(location source, diagnostic_handler& dh)
    : source_{source}, dh_{dh} {
  }

  auto add_pair(RowView<Data> key, RowView<Data> value, auto& result) -> void {
    match(
      key,
      [&](RowView<String> key) {
        result.put(*key, value);
      },
      [&](auto const&) {
        result.put(stringify(key), value);
      });
  }

  auto add_entry(RowView<Data> entry, auto& result) -> void {
    match(
      entry,
      [&](RowView<Record> record) {
        auto key = Option<RowView<Data>>{};
        auto value = Option<RowView<Data>>{};
        auto count = size_t{0};
        for (auto [name, field] : record) {
          ++count;
          if (name == "key") {
            key = field;
          } else if (name == "value") {
            value = field;
          }
        }
        if (count == 2 and key and value) {
          add_pair(*key, *value, result);
          return;
        }
        for (auto [name, field] : record) {
          result.put(name, field);
        }
      },
      [&](RowView<List> pair) {
        if (pair.length() != 2) {
          bad_pair_(dh_, diagnostic::warning("expected a two-element entry")
                           .primary(source_));
          return;
        }
        add_pair(pair.get(0), pair.get(1), result);
      },
      [](RowView<Null>) {},
      [&](auto const&) {
        bad_entry_(dh_,
                   diagnostic::warning("expected a record or a key/value pair")
                     .primary(source_));
      });
  }

private:
  location source_;
  diagnostic_handler& dh_;
  WarnOnce bad_pair_;
  WarnOnce bad_entry_;
};

class CollectRecordFunction {
public:
  auto eval(CollectRecordArgs const& args, EvalFrame frame) const
    -> Array<Data> {
    // Resolve the list alternatives and validate types once per batch.
    auto entries = args.entries.data.get_alternative<List>();
    auto entry_nulls = args.entries.data.get_alternative<Null>();
    auto values = args.values ? args.values->data.get_alternative<List>()
                              : Option<MaskedArray<Array<List>>>{};
    auto value_nulls = args.values ? args.values->data.get_alternative<Null>()
                                   : Option<MaskedArray<Array<Null>>>{};
    // Either null input propagates silently, even if the other is not a list.
    auto expected_rows = frame.mask();
    if (entry_nulls) {
      expected_rows = std::move(expected_rows).and_not(entry_nulls->present);
    }
    if (value_nulls) {
      expected_rows = std::move(expected_rows).and_not(value_nulls->present);
    }
    auto list_rows = entries ? expected_rows & entries->present
                             : storage::BitMap{frame.length(), false};
    if (expected_rows.and_not(list_rows).any()) {
      diagnostic::warning("expected `list`")
        .primary(args.entries.source)
        .emit(frame);
    }
    if (not entries) {
      return frame.null();
    }
    if (args.values) {
      auto paired_rows = values ? list_rows & values->present
                                : storage::BitMap{frame.length(), false};
      if (list_rows.and_not(paired_rows).any()) {
        diagnostic::warning("expected `list`")
          .primary(args.values->source)
          .emit(frame);
      }
      if (not values) {
        return frame.null();
      }
      list_rows = std::move(paired_rows);
    }
    if (not list_rows.any()) {
      return frame.null();
    }
    auto builder = ArrayBuilder<Data>{};
    auto decoder = EntryDecoder{args.entries.source, frame};
    auto bad_length = WarnOnce{};
    for (auto row : storage::true_bits(list_rows)) {
      builder.skip_n(row - builder.length());
      auto const xs = entries->data.get(row);
      auto const ys = values ? Option{values->data.get(row)} : None{};
      if (ys and xs.length() != ys->length()) {
        bad_length(frame, diagnostic::warning("key and value lists must have "
                                              "the same length")
                            .primary(args.entries.source)
                            .primary(args.values->source));
        builder.null();
        continue;
      }
      // FieldBuilder merges repeated writes (including lists and records).
      // Resolve duplicates first to keep last-value-wins semantics and the
      // first occurrence's field order without materializing input values.
      auto result = CollectedRecord<RowView<Data>>{};
      if (ys) {
        for (auto i = storage::Index{0}; i < xs.length(); ++i) {
          decoder.add_pair(xs.get(i), ys->get(i), result);
        }
      } else {
        for (auto entry : xs) {
          decoder.add_entry(entry, result);
        }
      }
      result.append_to(builder);
    }
    builder.skip_n(frame.length() - builder.length());
    return builder.finish().null_where(frame.mask().and_not(list_rows));
  }

  auto update(CollectRecordArgs const& args, EvalFrame frame) -> void {
    auto decoder = EntryDecoder{args.entries.source, frame};
    for (auto row : storage::true_bits(frame.mask())) {
      if (args.values) {
        decoder.add_pair(args.entries.data.get(row), args.values->data.get(row),
                         result_);
      } else {
        decoder.add_entry(args.entries.data.get(row), result_);
      }
    }
  }

  auto get() const -> Data {
    auto builder = ArrayBuilder<Data>{};
    result_.append_to(builder);
    return builder.take_last();
  }

  auto reset() -> void {
    result_ = {};
  }

private:
  CollectedRecord<Data> result_;
};

class Plugin final : public AggregationPlugin {
public:
  auto name() const -> std::string override {
    return "collect_record";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> AggregationDescription override {
    auto d = AggregationDescriber<CollectRecordArgs, CollectRecordFunction>{};
    d.positional("entries", &CollectRecordArgs::entries);
    d.optional_positional("values", &CollectRecordArgs::values);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    diagnostic::error("`collect_record` requires `--nova`")
      .primary(inv.call)
      .emit(ctx);
    return failure::promise();
  }
};

} // namespace
} // namespace tenzir::plugins::collect_record

TENZIR_REGISTER_PLUGIN(tenzir::plugins::collect_record::Plugin)
