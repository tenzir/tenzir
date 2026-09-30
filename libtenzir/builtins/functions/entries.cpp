//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/record_util.hpp>
#include <tenzir/plugin.hpp>

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
    auto records = resolve_record(args.x, frame);
    if (not records) {
      return frame.null();
    }
    auto builder = ArrayBuilder<Data>{};
    for (auto row : storage::true_bits(records->present)) {
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
    return builder.finish().null_where(frame.mask().and_not(records->present));
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
