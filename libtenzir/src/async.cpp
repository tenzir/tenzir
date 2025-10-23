//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"

#include <tmc/task.hpp>


namespace tenzir {

class async_ctx {
public:
  virtual ~async_ctx() = default;

  explicit(false) operator diagnostic_handler&() {
    return dh_;
  }

  virtual auto push(table_slice slice) -> tmc::task<void> = 0;

private:
  null_diagnostic_handler dh_;
};

class async_operator {
public:
  virtual ~async_operator() = default;

  virtual auto process(table_slice slice, async_ctx& ctx) -> tmc::task<void> = 0;
};

class pass final : public async_operator {
public:
  auto process(table_slice slice, async_ctx& ctx) -> tmc::task<void> override {
    co_await ctx.push(slice);
  }
};

auto filter2(const table_slice& slice, const ast::expression& expr,
             diagnostic_handler& dh, bool warn) -> std::vector<table_slice> {
  auto results = std::vector<table_slice>{};
  auto offset = int64_t{0};
  for (auto& filter : eval(expr, slice, dh)) {
    auto array = try_as<arrow::BooleanArray>(&*filter.array);
    if (not array) {
      diagnostic::warning("expected `bool`, got `{}`", filter.type.kind())
        .primary(expr)
        .emit(dh);
      offset += filter.array->length();
      continue;
    }
    if (array->true_count() == array->length()) {
      results.push_back(subslice(slice, offset, offset + array->length()));
      offset += array->length();
      continue;
    }
    if (warn) {
      diagnostic::warning("assertion failure").primary(expr).emit(dh);
    }
    auto length = array->length();
    auto current_value = array->Value(0);
    auto current_begin = int64_t{0};
    // We add an artificial `false` at index `length` to flush.
    for (auto i = int64_t{1}; i < length + 1; ++i) {
      const auto next = i != length && array->IsValid(i) && array->Value(i);
      if (current_value == next) {
        continue;
      }
      if (current_value) {
        results.push_back(subslice(slice, offset + current_begin, offset + i));
      }
      current_value = next;
      current_begin = i;
    }
    offset += length;
  }
  return results;
}

class where final : public async_operator {
public:
  auto process(table_slice slice, async_ctx& ctx) -> tmc::task<void> override {
    for (auto result : filter2(slice, expr_, ctx, false)) {
      co_await ctx.push(std::move(result));
    }
  }

private:
  ast::expression expr_;
};


} // namespace tenzir
