//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/option.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/array/array_binary.h>
#include <arrow/compute/api.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace tenzir::plugins::slice {

namespace {

/// Clamps Python-style slice bounds, where negative values count from the
/// end, to `[0, length]`.
auto normalize_slice_bounds(int64_t length, Option<int64_t> begin,
                            Option<int64_t> end)
  -> std::pair<int64_t, int64_t> {
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
  return {normalized_begin, normalized_end};
}

/// Calls `f` with the index of every element of `[0, length)` that the slice
/// selects, in output order.
auto for_each_slice_index(int64_t length, Option<int64_t> begin,
                          Option<int64_t> end, int64_t stride, auto&& f)
  -> void {
  auto const [first, last] = normalize_slice_bounds(length, begin, end);
  if (last <= first) {
    return;
  }
  if (stride > 0) {
    for (auto index = first; index < last;) {
      f(index);
      if (last - index <= stride) {
        break;
      }
      index += stride;
    }
    return;
  }
  if (stride == std::numeric_limits<int64_t>::min()) {
    f(last - 1);
    return;
  }
  auto const abs_stride = -stride;
  for (auto index = last - 1; index >= first; index -= abs_stride) {
    f(index);
    if (index - first < abs_stride) {
      break;
    }
  }
}

struct SliceArgs {
  nova::ValueArgument x;
  Option<located<int64_t>> begin;
  Option<located<int64_t>> end;
  Option<located<int64_t>> stride;
};

class SliceFunction final {
public:
  static auto eval(SliceArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto const begin
      = args.begin ? Option<int64_t>{args.begin->inner} : Option<int64_t>{};
    auto const end
      = args.end ? Option<int64_t>{args.end->inner} : Option<int64_t>{};
    auto const stride = args.stride ? args.stride->inner : int64_t{1};
    auto const& mask = frame.mask();
    auto const length = mask.length();
    auto strings = args.x.data.get_alternative<String>();
    auto lists = args.x.data.get_alternative<List>();
    auto nulls = args.x.data.get_alternative<Null>();
    auto const string_rows
      = strings ? mask & strings->present : storage::BitMap{length, false};
    auto const list_rows
      = lists ? mask & lists->present : storage::BitMap{length, false};
    auto const null_rows
      = nulls ? mask & nulls->present : storage::BitMap{length, false};
    if (mask.and_not(string_rows).and_not(list_rows).and_not(null_rows).any()) {
      diagnostic::warning("`slice` expected `string` or `list`, but got a "
                          "different type")
        .primary(args.x.source)
        .emit(frame);
    }
    // Strings only slice forward, like the legacy implementation.
    auto const slice_strings = stride > 0 or not string_rows.any();
    if (not slice_strings) {
      diagnostic::error("`stride` must be greater 0, but got {}", stride)
        .primary(*args.stride)
        .emit(frame);
    }
    auto builder = ArrayBuilder<Data>{};
    auto code_points = std::vector<size_t>{};
    for (auto row = storage::Index{0}; row < length; ++row) {
      if (string_rows.get(row) and slice_strings) {
        // Bounds count code points, so slice at their byte offsets.
        auto const value = *strings->data.get(row);
        code_points.clear();
        for (auto offset = size_t{0}; offset < value.size(); ++offset) {
          if ((static_cast<unsigned char>(value[offset]) & 0b1100'0000)
              != 0b1000'0000) {
            code_points.push_back(offset);
          }
        }
        auto const count = static_cast<int64_t>(code_points.size());
        code_points.push_back(value.size());
        auto result = std::string{};
        for_each_slice_index(count, begin, end, stride, [&](int64_t index) {
          auto const from = code_points[static_cast<size_t>(index)];
          auto const to = code_points[static_cast<size_t>(index) + 1];
          result += value.substr(from, to - from);
        });
        builder.data(std::string_view{result});
        continue;
      }
      if (list_rows.get(row)) {
        auto const list = lists->data.get(row);
        auto elements = builder.list();
        for_each_slice_index(list.length(), begin, end, stride,
                             [&](int64_t index) {
                               append_row(elements, list.get(index));
                             });
        continue;
      }
      builder.null();
    }
    return builder.finish();
  }
};

class Plugin : public virtual function_plugin,
               public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "slice";
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<SliceArgs, SliceFunction>{};
    d.positional("x", &SliceArgs::x, "string|list");
    d.named("begin", &SliceArgs::begin);
    d.named("end", &SliceArgs::end);
    d.named("stride", &SliceArgs::stride);
    d.validate([](SliceArgs& args, diagnostic_handler& dh) -> failure_or<void> {
      if (args.stride and args.stride->inner == 0) {
        diagnostic::error("`stride` must not be 0")
          .primary(*args.stride)
          .emit(dh);
        return failure::promise();
      }
      return {};
    });
    return std::move(d).finish();
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto const normalize_bounds
      = [](int64_t length, Option<int64_t> begin, Option<int64_t> end) {
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
    auto begin = Option<located<int64_t>>{};
    auto end = Option<located<int64_t>>{};
    auto stride = Option<located<int64_t>>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject_expr, "string|list")
          .named("begin", begin)
          .named("end", end)
          .named("stride", stride)
          .parse(inv, ctx));
    if (stride) {
      if (stride->inner == 0) {
        diagnostic::error("`stride` must not be 0").primary(*stride).emit(ctx);
        return failure::promise();
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
              auto [row_begin, row_end]
                = normalize_bounds(row_length,
                                   begin ? Option{begin->inner} : None{},
                                   end ? Option{end->inner} : None{});
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::Plugin)
