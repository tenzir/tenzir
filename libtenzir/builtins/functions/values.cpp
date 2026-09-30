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

namespace tenzir::plugins::values {
namespace {

using namespace nova;

struct ValuesArgs {
  ValueArgument x;
};

/// Converts each record into the list of its top-level field values in field
/// order. Together with `keys`, it is the inverse of the two-argument form of
/// `collect_record`.
class ValuesFunction {
public:
  static auto eval(ValuesArgs const& args, EvalFrame frame) -> Array<Data> {
    auto records = resolve_record(args.x, frame);
    if (not records) {
      return frame.null();
    }
    auto builder = ArrayBuilder<Data>{};
    for (auto row : storage::true_bits(records->present)) {
      builder.skip_n(row - builder.length());
      // Values are copied as is, including nested records, lists, and nulls.
      // A list may hold values of different types.
      auto list = builder.list();
      for (auto const& field : records->data.get(row)) {
        append_row(list, field.second);
      }
    }
    builder.skip_n(frame.length() - builder.length());
    return builder.finish().null_where(frame.mask().and_not(records->present));
  }
};

class Plugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "values";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<ValuesArgs, ValuesFunction>{};
    d.positional("x", &ValuesArgs::x, "record");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    diagnostic::error("`values` requires `--nova`").primary(inv.call).emit(ctx);
    return failure::promise();
  }
};

} // namespace
} // namespace tenzir::plugins::values

TENZIR_REGISTER_PLUGIN(tenzir::plugins::values::Plugin)
