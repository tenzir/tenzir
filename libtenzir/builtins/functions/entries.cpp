//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin.hpp>

#include <concepts>
#include <string>
#include <utility>

namespace tenzir::plugins::entries {
namespace {

using namespace nova;

struct EntriesArgs {
  ValueArgument x;
};

/// Converts each record into a list of `{key, value}` records, one for every
/// top-level field in field order. This is the inverse of `collect_record`.
class EntriesFunction {
public:
  static auto eval(EntriesArgs const& args, EvalFrame frame) -> Array<Data> {
    // Resolve the record and null alternatives once per batch, so that the
    // loop below only visits rows that hold a record.
    auto records = args.x.data.get_alternative<Record>();
    auto nulls = args.x.data.get_alternative<Null>();
    auto record_rows = records ? frame.mask() & records->present
                               : storage::BitMap{frame.length(), false};
    // Null input propagates silently. Everything else is a type error.
    auto invalid = frame.mask().and_not(record_rows);
    if (nulls) {
      invalid = std::move(invalid).and_not(nulls->present);
    }
    if (invalid.any()) {
      auto warn = [&]<data_type Tag>(Array<Tag> const&) {
        if constexpr (not std::same_as<Tag, Record>
                      and not std::same_as<Tag, Null>) {
          diagnostic::warning("expected `record`, got `{}`",
                              Type<Tag>::static_name)
            .primary(args.x.source)
            .emit(frame);
        }
      };
      match(args.x.data, warn, [&](UnionArray const& array) {
        for (auto const& field : array.fields()) {
          if ((invalid & field.present).any()) {
            match(field.data, warn);
          }
        }
      });
    }
    if (not records or not record_rows.any()) {
      return frame.null();
    }
    auto builder = ArrayBuilder<Data>{};
    for (auto row : storage::true_bits(record_rows)) {
      builder.skip_n(row - builder.length());
      // The key is the field name verbatim, without interpreting dots. The
      // value is copied as is, including nested records, lists, and nulls.
      auto list = builder.list();
      for (auto [name, value] : records->data.get(row)) {
        auto entry = list.record();
        entry.field("key").data(name);
        append_row(entry.field("value"), value);
      }
    }
    builder.skip_n(frame.length() - builder.length());
    return builder.finish().null_where(frame.mask().and_not(record_rows));
  }
};

class Plugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "entries";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<EntriesArgs, EntriesFunction>{};
    d.positional("x", &EntriesArgs::x, "record");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    diagnostic::error("`entries` requires `--nova`").primary(inv.call).emit(ctx);
    return failure::promise();
  }
};

} // namespace
} // namespace tenzir::plugins::entries

TENZIR_REGISTER_PLUGIN(tenzir::plugins::entries::Plugin)
