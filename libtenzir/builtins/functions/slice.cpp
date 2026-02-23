//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/array/array_binary.h>
#include <arrow/compute/api.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <utility>

namespace tenzir::plugins::slice {

namespace {

class Slice : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.slice";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto const normalize_bounds
      = [](int64_t length, std::optional<int64_t> begin,
           std::optional<int64_t> end) {
          auto normalized_begin = begin.value_or(0);
          auto normalized_end = end.value_or(length);
          if (normalized_begin < 0) {
            normalized_begin = length + normalized_begin;
          }
          if (normalized_end < 0) {
            normalized_end = length + normalized_end;
          }
          normalized_begin = std::clamp(normalized_begin, int64_t{0}, length);
          normalized_end = std::clamp(normalized_end, int64_t{0}, length);
          return std::pair{normalized_begin, normalized_end};
        };
    auto subject_expr = ast::expression{};
    auto begin = std::optional<located<int64_t>>{};
    auto end = std::optional<located<int64_t>>{};
    auto stride = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string|list")
          .named("begin", begin)
          .named("end", end)
          .named("stride", stride)
          .parse(inv, ctx));
    if (stride) {
      if (stride->inner == 0) {
        diagnostic::error("`stride` must not be 0").primary(*stride).emit(ctx);
      }
    }
    return function_use::make([this, subject_expr = std::move(subject_expr),
                               begin = begin, end = end, stride = stride,
                               normalize_bounds](evaluator eval, session ctx) {
      auto result_type = string_type{};
      return map_series(eval(subject_expr), [&](series subject) {
        auto f = detail::overload{
          [&](arrow::StringArray const& array) {
            if (stride and stride->inner < 0) {
              diagnostic::error("`stride` must be greater 0, but got {}",
                                stride->inner)
                .primary(*stride)
                .emit(ctx);
              return series::null(result_type, subject.length());
            }
            auto options = arrow::compute::SliceOptions(
              begin ? begin->inner : 0,
              end ? end->inner : std::numeric_limits<int64_t>::max(),
              stride ? stride->inner : 1);
            auto result = arrow::compute::CallFunction("utf8_slice_codeunits",
                                                       {array}, &options);
            if (not result.ok()) {
              diagnostic::warning("{}", result.status().ToString())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            }
            return series{result_type, result.MoveValueUnsafe().make_array()};
          },
          [&](arrow::ListArray const& array) -> series {
            auto list_subject = subject.as<list_type>();
            TENZIR_ASSERT(list_subject);
            auto builder
              = list_subject->type.make_arrow_builder(arrow_memory_pool());
            auto slice_stride = stride ? stride->inner : 1;
            auto value_type = list_subject->type.value_type();
            // Arrow's list_slice currently does not implement our required
            // stop/negative-step semantics, so we hand-roll slicing here.
            for (auto i = int64_t{0}; i < array.length(); ++i) {
              if (array.IsNull(i)) {
                check(builder->AppendNull());
                continue;
              }
              auto row_offset = array.value_offset(i);
              auto row_length = array.value_length(i);
              check(builder->Append());
              auto [row_begin, row_end] = normalize_bounds(
                row_length, begin ? std::optional{begin->inner} : std::nullopt,
                end ? std::optional{end->inner} : std::nullopt);
              if (row_end <= row_begin) {
                continue;
              }
              if (slice_stride > 0) {
                for (auto element = row_begin; element < row_end;) {
                  check(append_array_slice(*builder->value_builder(),
                                           value_type, *array.values(),
                                           row_offset + element, 1));
                  if (row_end - element <= slice_stride) {
                    break;
                  }
                  element += slice_stride;
                }
                continue;
              }
              if (slice_stride == std::numeric_limits<int64_t>::min()) {
                check(append_array_slice(*builder->value_builder(), value_type,
                                         *array.values(),
                                         row_offset + row_end - 1, 1));
                continue;
              }
              auto const abs_stride = -slice_stride;
              for (auto element = row_end - 1; element >= row_begin;
                   element -= abs_stride) {
                check(append_array_slice(*builder->value_builder(), value_type,
                                         *array.values(), row_offset + element,
                                         1));
                if (element - row_begin < abs_stride) {
                  break;
                }
              }
            }
            return series{list_subject->type, finish(*builder)};
          },
          [&](arrow::NullArray const& array) {
            if (is<list_type>(subject.type)) {
              return series::null(as<list_type>(subject.type), array.length());
            }
            return series::null(result_type, array.length());
          },
          [&](auto const&) {
            diagnostic::warning("`{}` expected `string` or `list`, but got "
                                "`{}`",
                                name(), subject.type.kind())
              .primary(subject_expr)
              .emit(ctx);
            return series::null(null_type{}, subject.length());
          },
        };
        return match(*subject.array, f);
      });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::slice

TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::Slice)
