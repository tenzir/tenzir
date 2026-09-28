//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/option.hpp"

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/api.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::map_keys_function {

namespace {

auto map_names(record_type const& r, ast::lambda_expr const& fn, session ctx)
  -> Option<std::vector<std::string>> {
  auto keys_builder = arrow::StringBuilder{tenzir::arrow_memory_pool()};
  check(keys_builder.Reserve(detail::narrow<int64_t>(r.num_fields())));
  auto names = std::vector<std::string>{};
  auto invalid = std::vector<std::string_view>{};
  auto conflicts = std::vector<std::string_view>{};
  for (auto const& field : r.fields()) {
    check(keys_builder.Append(field.name));
    names.emplace_back(field.name);
  }
  auto mapped = tenzir::eval(
    fn, multi_series{series{string_type{}, finish(keys_builder)}}, ctx);
  TENZIR_ASSERT_EQ(mapped.length(), detail::narrow<int64_t>(r.num_fields()));
  auto i = size_t{0};
  for (auto const& part : mapped) {
    auto strings = part.as<string_type>();
    auto* array = strings ? strings->array.get() : nullptr;
    for (auto j = int64_t{0}; j < part.length(); ++j, ++i) {
      if (array and not array->IsNull(j)) {
        names[i] = array->GetView(j);
      } else {
        invalid.emplace_back(r.field(i).name);
      }
    }
  }
  for (auto i = size_t{0}; i < names.size(); ++i) {
    if (std::ranges::count(names, names[i]) > 1) {
      conflicts.emplace_back(r.field(i).name);
    }
  }
  if (not invalid.empty()) {
    diagnostic::warning("kept fields with non-string results")
      .primary(fn.body)
      .note("kept fields: `{}`", fmt::join(invalid, "`, `"))
      .emit(ctx);
  }
  if (not conflicts.empty()) {
    diagnostic::warning("skipped record with conflicting field names")
      .primary(fn.body)
      .note("conflicting fields: `{}`", fmt::join(conflicts, "`, `"))
      .emit(ctx);
    return None{};
  }
  return names;
}

struct MapKeysArgs {
  nova::ValueArgument x;
  nova::LambdaArgument function;
};

class MapKeysFunction {
public:
  static auto eval(MapKeysArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto source_names = std::vector<std::vector<std::string>>{
      static_cast<size_t>(frame.length())};
    auto mapped_names = source_names;
    auto invalid_type = false;
    for (auto row : storage::true_bits(frame.mask())) {
      match(args.x.data.get(row), [&]<class T>(RowView<T> value) {
        if constexpr (std::same_as<T, Record>) {
          for (auto const& [name, field] : value) {
            source_names[row].emplace_back(name);
            mapped_names[row].emplace_back(name);
          }
        } else if constexpr (not std::same_as<T, Null>) {
          invalid_type = true;
        }
      });
    }
    auto invalid = std::vector<std::string_view>{};
    for (auto index = size_t{0};; ++index) {
      auto key_rows = storage::BitMap::Mutable{frame.length()};
      auto keys = ArrayBuilder<Data>{};
      for (auto row : storage::true_bits(frame.mask())) {
        if (index >= source_names[row].size()) {
          continue;
        }
        keys.skip_n(row - keys.length());
        keys.data(source_names[row][index]);
        key_rows.set(row, true);
      }
      auto present = std::move(key_rows).finish();
      if (not present.any()) {
        break;
      }
      keys.skip_n(frame.length() - keys.length());
      auto subject = MaskedArray<Array<Data>>{keys.finish(), present};
      auto rows = frame.narrow(present);
      auto result
        = frame.input()
            ? rows.eval(args.function, std::move(subject), *frame.input())
            : rows.eval(args.function, std::move(subject));
      auto strings = result.get_alternative<String>();
      for (auto row : storage::true_bits(present)) {
        if (strings and strings->present.get(row)) {
          mapped_names[row][index] = *strings->data.get(row);
        } else {
          invalid.push_back(source_names[row][index]);
        }
      }
    }
    auto conflicts = std::vector<std::string_view>{};
    auto builder = ArrayBuilder<Data>{};
    for (auto row : storage::true_bits(frame.mask())) {
      builder.skip_n(row - builder.length());
      match(args.x.data.get(row), [&]<class T>(RowView<T> value) {
        if constexpr (std::same_as<T, Null>) {
          builder.null();
        } else if constexpr (std::same_as<T, Record>) {
          auto conflicting = false;
          for (auto index = size_t{0}; index < mapped_names[row].size();
               ++index) {
            if (std::ranges::count(mapped_names[row], mapped_names[row][index])
                > 1) {
              conflicting = true;
              conflicts.push_back(source_names[row][index]);
            }
          }
          auto output = builder.record();
          auto index = size_t{0};
          for (auto const& [name, field] : value) {
            auto const& output_name
              = conflicting ? name : mapped_names[row][index];
            append_row(output.field(output_name), field);
            ++index;
          }
        } else {
          builder.null();
        }
      });
    }
    builder.skip_n(frame.length() - builder.length());
    if (invalid_type) {
      diagnostic::warning("expected `record`, got a different type")
        .primary(args.x.source)
        .emit(frame);
    }
    if (not invalid.empty()) {
      diagnostic::warning("kept fields with non-string results")
        .primary(args.function.body())
        .note("kept fields: `{}`", fmt::join(invalid, "`, `"))
        .emit(frame);
    }
    if (not conflicts.empty()) {
      diagnostic::warning("skipped record with conflicting field names")
        .primary(args.function.body())
        .note("conflicting fields: `{}`", fmt::join(conflicts, "`, `"))
        .emit(frame);
    }
    return builder.finish();
  }
};

class map_keys final : public virtual function_plugin,
                       public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "map_keys";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<MapKeysArgs, MapKeysFunction>{};
    d.positional("x", &MapKeysArgs::x, "record");
    d.positional("function", &MapKeysArgs::function, "string => string");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto function = ast::lambda_expr{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "record")
          .positional("function", function, "string => string")
          .parse(inv, ctx));
    if (not function.is_unary()) {
      diagnostic::error("expected unary lambda")
        .primary(function)
        .hint("provide `key => ...`")
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make(
      [expr = std::move(expr), function = std::move(function)](
        evaluator eval, session ctx) -> multi_series {
        return map_series(eval(expr), [&](series subject) -> series {
          if (is<null_type>(subject.type)) {
            return subject;
          }
          const auto* input_type = try_as<record_type>(&subject.type);
          if (not input_type) {
            diagnostic::warning("expected `record`, got `{}`",
                                subject.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, subject.length());
          }
          auto names = map_names(*input_type, function, ctx);
          if (not names) {
            return subject;
          }
          auto fields = std::vector<record_type::field_view>{};
          fields.reserve(input_type->num_fields());
          for (auto [name, field] :
               std::views::zip(*names, input_type->fields())) {
            fields.emplace_back(name, field.type);
          }
          auto result_type = type{record_type{fields}};
          auto result_data = subject.array->data()->Copy();
          result_data->type = result_type.to_arrow_type();
          auto result_array = arrow::MakeArray(std::move(result_data));
          return series{std::move(result_type), std::move(result_array)};
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::map_keys_function

TENZIR_REGISTER_PLUGIN(tenzir::plugins::map_keys_function::map_keys)
