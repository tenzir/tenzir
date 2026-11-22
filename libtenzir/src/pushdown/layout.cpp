//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/layout.hpp"

#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval_util.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/variant_traits.hpp"

#include <charconv>

namespace tenzir::pushdown {

namespace {

auto segments(std::span<std::string const> path)
  -> std::vector<ast::field_path::segment> {
  auto result = std::vector<ast::field_path::segment>{};
  for (auto const& name : path) {
    result.push_back({
      .id = ast::identifier{name, location::unknown},
      .has_question_mark = false,
    });
  }
  return result;
}

/// Decodes `value` as `decode` says, keeping values that do not match the
/// expected encoding as they are.
auto decode_column(nova::MaskedArray<nova::Array<nova::Data>> value,
                   Decode decode, std::string_view column,
                   diagnostic_handler& dh)
  -> nova::MaskedArray<nova::Array<nova::Data>> {
  if (decode != Decode::exact and decode != Decode::time
      and decode != Decode::boolean and decode != Decode::duration) {
    return value;
  }
  auto builder = nova::ArrayBuilder<nova::Data>{};
  auto overflow = false;
  for (auto i = nova::storage::Index{0}; i < value.data.length(); ++i) {
    if (not value.present.get(i)) {
      builder.skip();
      continue;
    }
    auto const row = value.data.get(i);
    if (decode == Decode::boolean) {
      if (auto const* x = try_as<nova::RowView<nova::Int>>(row)) {
        builder.data(**x != 0);
        continue;
      }
      if (auto const* x = try_as<nova::RowView<nova::UInt>>(row)) {
        builder.data(**x != 0);
        continue;
      }
      if (auto const* text = try_as<nova::RowView<nova::String>>(row)) {
        auto const view = std::string_view{**text};
        if (view == "1" or view == "true") {
          builder.data(true);
          continue;
        }
        if (view == "0" or view == "false") {
          builder.data(false);
          continue;
        }
      }
      nova::append_row(builder, row);
      continue;
    }
    if (decode == Decode::duration) {
      if (auto const* x = try_as<nova::RowView<nova::Int>>(row)) {
        builder.data(duration{std::chrono::nanoseconds{**x}});
        continue;
      }
      nova::append_row(builder, row);
      continue;
    }
    auto const* text = try_as<nova::RowView<nova::String>>(row);
    if (not text) {
      nova::append_row(builder, row);
      continue;
    }
    auto const view = std::string_view{**text};
    if (decode == Decode::exact) {
      auto result = int64_t{};
      auto const [end, error]
        = std::from_chars(view.data(), view.data() + view.size(), result);
      if (end == view.data() + view.size() and error == std::errc{}) {
        builder.data(result);
        continue;
      }
      if (end == view.data() + view.size()
          and error == std::errc::result_out_of_range) {
        overflow = true;
        builder.null();
        continue;
      }
    } else {
      auto result = time{};
      if (parsers::time(view, result)) {
        builder.data(result);
        continue;
      }
    }
    builder.data(view);
  }
  if (overflow) {
    diagnostic::warning("integer overflow")
      .note("in the result column `{}`", column)
      .emit(dh);
  }
  return {builder.finish(), std::move(value.present)};
}

} // namespace

auto apply_layout(nova::Events events, Layout const& layout,
                  diagnostic_handler& dh) -> nova::Events {
  auto const length = events.length();
  auto meta = layout.schema_name
                ? nova::Events::Meta::make_empty(length, *layout.schema_name)
                : std::move(events.meta);
  if (not layout.fields) {
    if (not layout.hidden.empty()) {
      auto hidden = std::vector<ast::field_path>{};
      for (auto const& name : layout.hidden) {
        auto path = ast::field_path::try_from(
          ast::root_field{ast::identifier{name, location::unknown}, false});
        TENZIR_ASSERT(path);
        hidden.push_back(std::move(*path));
      }
      events.data = nova::DropTree::make(hidden).apply(std::move(events.data),
                                                       events.mask);
    }
    return nova::Events{std::move(events.data), std::move(events.mask),
                        std::move(meta)};
  }
  auto result = nova::Array<nova::Record>::make_empty(length);
  for (auto const& [field, column] : *layout.fields) {
    auto value = events.data.field(column);
    if (not value) {
      continue;
    }
    result = nova::assign_nested_field(
      std::move(result), segments(field),
      decode_column(std::move(*value), layout.decode_of(column), column, dh),
      dh);
  }
  return nova::Events{std::move(result), std::move(events.mask),
                      std::move(meta)};
}

} // namespace tenzir::pushdown
