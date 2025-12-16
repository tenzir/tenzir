//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/function.h>
#include <arrow/compute/registry.h>

namespace tenzir::plugins::bit {

namespace {

class unary_fn : public virtual function_plugin {
public:
  unary_fn(std::string name, std::string compute_fn)
    : name_{std::move(name)}, compute_fn_{std::move(compute_fn)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  struct impl final : public function_use {
    auto run(evaluator eval, session ctx) -> multi_series override {
      return map_series(eval(expr), [&](const series& values) -> multi_series {
        if (is<null_type>(values.type)) {
          return series::null(int64_type{}, values.length());
        }
        if (not is<int64_type>(values.type)
            and not is<uint64_type>(values.type)) {
          diagnostic::warning("expected `int64` or `uint64`, got `{}`",
                              values.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(int64_type{}, values.length());
        }
        auto result
          = check(compute_fn->Execute({values.array}, nullptr, nullptr));
        TENZIR_ASSERT(result.is_array());
        return series{
          values.type,
          result.make_array(),
        };
      });
    }

    ast::expression expr;
    std::shared_ptr<arrow::compute::Function> compute_fn;
  };

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto result = std::make_unique<impl>();
    result->compute_fn
      = check(arrow::compute::GetFunctionRegistry()->GetFunction(compute_fn_));
    auto parser = argument_parser2::function(name());
    parser.positional("x", result->expr, "int");
    TRY(parser.parse(inv, ctx));
    return result;
  }

private:
  std::string name_;
  std::string compute_fn_;
};

class binary_fn : public virtual function_plugin {
public:
  binary_fn(std::string name, std::string compute_fn)
    : name_{std::move(name)}, compute_fn_{std::move(compute_fn)} {
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return name_;
  }

  struct impl final : public function_use {
    auto run(evaluator eval, session ctx) -> multi_series override {
      return map_series(
        eval(lhs), eval(rhs),
        [&](const series& lhs_values, series rhs_values) -> multi_series {
          if (is<null_type>(lhs_values.type)
              or is<null_type>(rhs_values.type)) {
            return series::null(int64_type{}, lhs_values.length());
          }
          if (not is<int64_type>(lhs_values.type)
              and not is<uint64_type>(lhs_values.type)) {
            diagnostic::warning("expected `int64` or `uint64`, got `{}`",
                                lhs_values.type.kind())
              .primary(lhs)
              .emit(ctx);
            return series::null(int64_type{}, lhs_values.length());
          }
          if (not is<int64_type>(rhs_values.type)
              and not is<uint64_type>(rhs_values.type)) {
            diagnostic::warning("expected `int64` or `uint64`, got `{}`",
                                rhs_values.type.kind())
              .primary(rhs)
              .emit(ctx);
            return series::null(int64_type{}, lhs_values.length());
          }
          if (validate_rhs) {
            match(
              *rhs_values.array,
              [&]<concepts::one_of<arrow::Int64Array, arrow::UInt64Array> T>(
                const T& array) {
                auto validity_builder = arrow::TypedBufferBuilder<bool>{
                  tenzir::arrow_memory_pool()};
                check(validity_builder.Reserve(array.length()));
                auto warn = false;
                auto null_count = int64_t{};
                const auto max = is<int64_type>(lhs_values.type) ? 62 : 63;
                for (const auto& value : array) {
                  if (not value) {
                    validity_builder.UnsafeAppend(false);
                    ++null_count;
                    continue;
                  }
                  if (std::cmp_less(*value, 0)
                      or std::cmp_greater(*value, max)) {
                    validity_builder.UnsafeAppend(false);
                    ++null_count;
                    warn = true;
                    continue;
                  }
                  validity_builder.UnsafeAppend(true);
                }
                if (warn) {
                  diagnostic::warning("out of range")
                    .primary(rhs, "must be in range [0, {}]", max)
                    .emit(ctx);
                }
                TENZIR_ASSERT(array.data()->buffers.size() == 2);
                rhs_values.type = type{uint64_type{}};
                rhs_values.array = std::make_shared<arrow::UInt64Array>(
                  array.length(), array.data()->buffers[1],
                  check(validity_builder.Finish()), null_count, array.offset());
              },
              [&](const auto&) {
                TENZIR_UNREACHABLE();
              });
          }
          auto result = check(compute_fn->Execute(
            {lhs_values.array, rhs_values.array}, nullptr, nullptr));
          TENZIR_ASSERT(result.is_array());
          // For whatever reason, the result of Arrow's bitwise compute
          // functions is signed if at least of its arguments is signed, and
          // unsigned otherwise.
          const auto signed_result = is<int64_type>(lhs_values.type)
                                     or is<int64_type>(rhs_values.type);
          TENZIR_ASSERT(
            result.type()->id()
            == (signed_result ? arrow::Type::INT64 : arrow::Type::UINT64));
          return series{
            signed_result ? type{int64_type{}} : type{uint64_type{}},
            result.make_array(),
          };
        });
    }

    ast::expression lhs;
    ast::expression rhs;
    bool validate_rhs = false;
    std::shared_ptr<arrow::compute::Function> compute_fn;
  };

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto result = std::make_unique<impl>();
    result->compute_fn
      = check(arrow::compute::GetFunctionRegistry()->GetFunction(compute_fn_));
    result->validate_rhs = compute_fn_.starts_with("shift");
    auto parser = argument_parser2::function(name());
    parser.positional("lhs", result->lhs, "int");
    parser.positional("rhs", result->rhs, "int");
    TRY(parser.parse(inv, ctx));
    return result;
  }

private:
  std::string name_;
  std::string compute_fn_;
};

} // namespace

} // namespace tenzir::plugins::bit

using namespace tenzir::plugins::bit;

TENZIR_REGISTER_PLUGIN(binary_fn{"bit_and", "bit_wise_and"})
TENZIR_REGISTER_PLUGIN(binary_fn{"bit_or", "bit_wise_or"})
TENZIR_REGISTER_PLUGIN(unary_fn{"bit_not", "bit_wise_not"})
TENZIR_REGISTER_PLUGIN(binary_fn{"bit_xor", "bit_wise_xor"})
TENZIR_REGISTER_PLUGIN(binary_fn{"shift_left", "shift_left"})
TENZIR_REGISTER_PLUGIN(binary_fn{"shift_right", "shift_right"})
