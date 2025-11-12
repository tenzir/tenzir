//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval_impl.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/try.hpp"

#include <arrow/compute/api_vector.h>

/// TODO:
/// - Reduce series expansion. For example, `src_ip in [1.2.3.4, 1.2.3.5]`
///   currently creates `length` copies of the list.
/// - Optimize expressions, e.g., constant folding, compute offsets.
/// - Short circuiting, active rows.
/// - Stricter behavior for const-eval, or same behavior? For example, overflow.
/// - Modes for "must be constant", "prefer constant", "prefer runtime", "must
///   be runtime".
/// - Integrate type checker?

namespace tenzir {

namespace {

struct is_deterministic_impl : ast::visitor<is_deterministic_impl> {
  explicit is_deterministic_impl(session ctx) : ctx{ctx} {
  }

  void visit(ast::function_call& x) {
    result = ctx.reg().get(x).is_deterministic();
    if (not result) {
      return;
    }
    for (auto& arg : x.args) {
      enter(arg);
    }
  }

  auto visit(auto& x) {
    if (not result) {
      return;
    }
    enter(x);
  }

  bool result = true;
  session ctx;
};

auto is_deterministic(const ast::expression& expr, session ctx) -> bool {
  auto impl = is_deterministic_impl{ctx};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  impl.visit(const_cast<ast::expression&>(expr));
  return impl.result;
}

class capture_extractor : public ast::visitor<capture_extractor> {
public:
  template <typename T>
  auto visit(const T& x) -> void {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    enter(const_cast<T&>(x));
  }

  auto visit(const ast::index_expr& x) -> void {
    if (auto path = ast::field_path::try_from(x)) {
      captures_.push_back(std::move(*path));
      return;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    enter(const_cast<ast::index_expr&>(x));
  }

  template <typename T>
  auto visit(const T& x) -> void
    requires concepts::one_of<T, ast::root_field, ast::field_access>
  {
    if (auto path = ast::field_path::try_from(x)) {
      captures_.push_back(std::move(*path));
      return;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    enter(const_cast<T&>(x));
  }

  auto result() && -> std::vector<ast::field_path> {
    return std::move(captures_);
  }

private:
  std::vector<ast::field_path> captures_;
};

} // namespace

auto resolve(const ast::field_path& sel, const table_slice& slice)
  -> variant<series, resolve_error> {
  return trace_panic(sel, [&] -> variant<series, resolve_error> {
    TRY(auto offset, resolve(sel, slice.schema()));
    auto [ty, array] = offset.get(slice);
    return series{ty, array};
  });
}

auto resolve(const ast::field_path& sel, type ty)
  -> variant<offset, resolve_error> {
  return trace_panic(sel, [&] -> variant<offset, resolve_error> {
    auto result = offset{};
    const auto& path = sel.path();
    result.reserve(path.size());
    for (const auto& segment : path) {
      const auto* rty = try_as<record_type>(ty);
      if (not rty) {
        return resolve_error{
          segment.id,
          resolve_error::field_of_non_record{ty},
        };
      }
      if (auto idx = rty->resolve_field(segment.id.name)) {
        result.push_back(*idx);
        ty = rty->field(*idx).type;
        continue;
      }
      if (segment.has_question_mark) {
        return resolve_error{
          segment.id,
          resolve_error::field_not_found_no_error{},
        };
      }
      return resolve_error{
        segment.id,
        resolve_error::field_not_found{},
      };
    }
    return result;
  });
}

auto eval(const ast::expression& expr, const table_slice& input,
          diagnostic_handler& dh) -> multi_series {
  return trace_panic(expr, [&] -> multi_series {
    // TODO: Do not create a new session here.
    auto sp = session_provider::make(dh);
    auto result = evaluator{&input, sp.as_session()}.eval(expr);
    TENZIR_ASSERT(result.length() == detail::narrow<int64_t>(input.rows()));
    return result;
  });
}

auto eval(const ast::field_path& expr, const table_slice& input,
          diagnostic_handler& dh) -> series {
  return trace_panic(expr, [&] -> series {
    auto result = eval(expr.inner(), input, dh);
    TENZIR_ASSERT(result.length() == detail::narrow<int64_t>(input.rows()));
    TENZIR_ASSERT(result.parts().size() == 1);
    return std::move(result.parts()[0]);
  });
}

auto eval(const ast::constant& expr, const table_slice& input,
          diagnostic_handler& dh) -> series {
  return trace_panic(expr, [&] -> series {
    auto result = eval(ast::expression{expr}, input, dh);
    TENZIR_ASSERT(result.length() == detail::narrow<int64_t>(input.rows()));
    TENZIR_ASSERT(result.parts().size() == 1);
    return std::move(result.parts()[0]);
  });
}

auto const_eval(const ast::expression& expr, diagnostic_handler& dh)
  -> failure_or<data> {
  return trace_panic(expr, [&] -> failure_or<data> {
    // TODO: Do not create a new session here.
    try {
      auto sp = session_provider::make(dh);
      auto result = evaluator{nullptr, sp.as_session()}.eval(expr);
      TENZIR_ASSERT(result.length() == 1);
      TENZIR_ASSERT(result.parts().size() == 1);
      auto& part = result.part(0);
      return materialize(value_at(part.type, *part.array, 0));
    } catch (failure fail) {
      return fail;
    }
  });
}

auto try_const_eval(const ast::expression& expr, session ctx)
  -> std::optional<data> {
  return trace_panic(expr, [&] -> std::optional<data> {
    if (not is_deterministic(expr, ctx)) {
      // TODO: This check is not ideal, as it is a bit too broad, and
      // incorrectly marks short-circuited expressions like `random() if false`
      // as non-deterministic just because they contain a call to a
      // non-deterministic function.
      return {};
    }
    auto const_dh = collecting_diagnostic_handler{};
    auto const_sp = session_provider::make(const_dh);
    if (auto result = const_eval(expr, const_dh)) {
      std::move(const_dh).forward_to(ctx);
      return std::move(*result);
    }
    return {};
  });
}

auto eval(const ast::lambda_expr& lambda, const basic_series<list_type>& input,
          const table_slice& slice, diagnostic_handler& dh) -> multi_series {
  return trace_panic(lambda, [&] -> multi_series {
    TENZIR_ASSERT(input.array);
    TENZIR_ASSERT(input.array->values());
    TENZIR_ASSERT(std::cmp_equal(slice.rows(), input.length()));
    auto visitor = capture_extractor{};
    visitor.visit(lambda.right);
    const auto captures = std::move(visitor).result();
    const auto ty = input.type.value_type();
    auto capture_offsets = std::vector<offset>{};
    for (const auto& capture : captures) {
      if (capture.path().front().id.name == lambda.left.name) {
        continue;
      }
      auto resolved = resolve(capture, slice.schema());
      if (const auto* off = try_as<offset>(resolved)) {
        capture_offsets.push_back(*off);
      }
    }
    if (capture_offsets.empty()) {
      return eval(lambda, series{ty, input.array->values()}, dh);
    }
    const auto to
      = check(ast::field_path::try_from(ast::root_field{lambda.left, false}));
    auto b = arrow::Int64Builder{tenzir::arrow_memory_pool()};
    check(b.Reserve(input.array->values()->length()));
    for (auto i = int64_t{}; i < input.array->length(); ++i) {
      for (auto j = int64_t{}; j < input.array->value_length(i); ++j) {
        b.UnsafeAppend(i);
      }
    }
    auto repeated = table_slice{
      check(arrow::compute::Take(to_record_batch(slice), finish(b)))
        .record_batch(),
      slice.schema(),
    };
    repeated.import_time(slice.import_time());
    const auto slice = assign(to, {ty, input.array->values()}, repeated, dh);
    return eval(lambda.right, slice, dh);
  });
}

auto eval(const ast::lambda_expr& lambda, const multi_series& input,
          diagnostic_handler& dh) -> multi_series {
  return trace_panic(lambda, [&] -> multi_series {
    auto result = multi_series{};
    for (const auto& part : input) {
      if (part.length() == 0) {
        continue;
      }
      // TODO: This is rather expensive; instead of evaluating the lambda on a
      // newly created table slice, we should much rather support them in the
      // evaluator directly.
      auto schema = type{
        "lambda",
        record_type{
          {lambda.left.name, part.type},
        },
      };
      auto arrow_schema = schema.to_arrow_schema();
      auto slice = table_slice{
        arrow::RecordBatch::Make(std::move(arrow_schema), input.length(),
                                 arrow::ArrayVector{part.array}),
        std::move(schema),
      };
      result.append(eval(lambda.right, slice, dh));
    }
    return result;
  });
}

auto eval(const ast::lambda_expr& lambda, const data& input,
          diagnostic_handler& dh) -> data {
  return trace_panic(lambda, [&] -> data {
    const auto result = eval(lambda, data_to_series(input, int64_t{1}), dh);
    TENZIR_ASSERT(result.parts().size() == 1);
    TENZIR_ASSERT(result.part(0).length() == 1);
    return materialize(value_at(result.part(0).type, *result.part(0).array, 0));
  });
}

} // namespace tenzir
