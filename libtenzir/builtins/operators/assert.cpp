//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/printable/tenzir/json.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::assert_ {

namespace {

struct AssertArgs {
  ast::expression invariant;
  std::optional<ast::expression> message;

  friend auto inspect(auto& f, AssertArgs& x) -> bool {
    return f.object(x).fields(f.field("invariant", x.invariant),
                              f.field("message", x.message));
  }
};

class Assert final : public Operator<table_slice, table_slice> {
public:
  explicit Assert(AssertArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto offset = int64_t{0};
    for (auto& filter : eval(args_.invariant, input, ctx)) {
      auto* array = try_as<arrow::BooleanArray>(*filter.array);
      if (not array) {
        diagnostic::warning("expected `bool`, got `{}`", filter.type.kind())
          .primary(args_.invariant)
          .emit(ctx);
        offset += filter.array->length();
        continue;
      }
      if (array->true_count() == array->length()) {
        co_await push(subslice(input, offset, offset + array->length()));
        offset += array->length();
        continue;
      }
      if (array->null_count() > 0) {
        diagnostic::warning("expected `bool`, got `null`")
          .primary(args_.invariant)
          .emit(ctx);
      }
      if (not args_.message) {
        diagnostic::warning("assertion failure")
          .primary(args_.invariant)
          .emit(ctx);
      }
      auto length = array->length();
      auto current_value = array->IsValid(0) && array->Value(0);
      auto current_begin = int64_t{0};
      auto results = std::vector<table_slice>{};
      const auto p = json_printer{json_printer_options{
        .tql = true,
        .oneline = true,
      }};
      auto buf = std::string{};
      const auto print_messages = [&](const int64_t start, const int64_t end) {
        if (start == end) {
          return;
        }
        const auto sub = subslice(input, start, end);
        const auto ms = eval(*args_.message, sub, ctx);
        for (const auto& s : ms) {
          for (auto msg : s.values()) {
            auto it = std::back_inserter(buf);
            p.print(it, msg);
            diagnostic::warning("assertion failed: {}", buf)
              .primary(args_.invariant)
              .emit(ctx);
            buf.clear();
          }
        }
      };
      for (auto i = int64_t{1}; i < length + 1; ++i) {
        const auto next = i != length && array->IsValid(i) && array->Value(i);
        if (current_value == next) {
          continue;
        }
        if (current_value) {
          results.push_back(
            subslice(input, offset + current_begin, offset + i));
        } else if (args_.message) {
          print_messages(offset + current_begin, offset + i);
        }
        current_value = next;
        current_begin = i;
      }
      if (args_.message) {
        print_messages(offset + current_begin, offset + length);
      }
      if (not results.empty()) {
        co_await push(concatenate(std::move(results)));
      }
      offset += length;
    }
  }

  friend auto inspect(auto& f, Assert& x) -> bool {
    return f.apply(x.args_);
  }

private:
  AssertArgs args_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "assert";
  }

  auto describe() const -> Description override {
    auto d = Describer<AssertArgs, Assert>{};
    d.positional("invariant", &AssertArgs::invariant, "bool");
    d.named("message", &AssertArgs::message, "string");
    return d.order_invariant();
  }
};

} // namespace

} // namespace tenzir::plugins::assert_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::assert_::plugin)
