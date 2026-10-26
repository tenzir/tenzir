//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/fbs/data.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/eval_impl.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/try.hpp"

#include <arrow/compute/api_vector.h>

#include <algorithm>
#include <iterator>
#include <string_view>

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

auto ensure_unary_lambda(const ast::lambda_expr& lambda) -> void {
  if (lambda.is_unary()) {
    return;
  }
  diagnostic::error("expected unary lambda")
    .primary(lambda)
    .hint("binary lambdas are only supported for `sort(..., cmp=...)`")
    .throw_();
}

/// Constant evaluates `expr` with the Nova evaluator.
auto const_eval_nova(ast::expression const& expr, base_ctx ctx)
  -> failure_or<data> {
  TRY(auto value,
      nova::const_eval(expr, nova::InstantiateCtx{ctx.dh(), ctx.reg()}));
  return nova::materialize_legacy(value);
}

/// Whether `call` is a call to the standard library function `name`.
auto is_std_call(ast::function_call const& call, std::string_view name)
  -> bool {
  auto const& ref = call.fn.ref;
  return ref.pkg() == entity_pkg_std and ref.segments().size() == 1
         and ref.segments()[0] == name;
}

/// Whether `value`, a `data` or the value of an `ast::constant`, holds a
/// secret at any depth.
template <class T>
auto contains_secret(T const& value) -> bool {
  return tenzir::match(
    value,
    [](secret const&) {
      return true;
    },
    [](record const& x) {
      return std::ranges::any_of(x, [](auto const& field) {
        return contains_secret(field.second);
      });
    },
    [](list const& x) {
      return std::ranges::any_of(x, [](data const& element) {
        return contains_secret(element);
      });
    },
    [](auto const&) {
      return false;
    });
}

/// Returns `value` with every nested secret replaced by `substitute`.
auto substitute_secrets(data value, data const& substitute) -> data {
  return tenzir::match(
    std::move(value),
    [&](secret const&) -> data {
      return substitute;
    },
    [&](record x) -> data {
      for (auto& field : x) {
        field.second = substitute_secrets(std::move(field.second), substitute);
      }
      return x;
    },
    [&](list x) -> data {
      for (auto& element : x) {
        element = substitute_secrets(std::move(element), substitute);
      }
      return x;
    },
    [](auto x) -> data {
      return x;
    });
}

/// Finds the sources of unresolved secrets in an expression: calls to `secret`
/// and constants that hold a secret, such as the value of a `let` binding.
class secret_source_finder : public ast::visitor<secret_source_finder> {
public:
  template <class T>
  auto visit(T const& x) -> void {
    if (not found) {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
      enter(const_cast<T&>(x));
    }
  }

  auto visit(ast::function_call const& x) -> void {
    if (is_std_call(x, "secret")) {
      found = true;
      return;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    enter(const_cast<ast::function_call&>(x));
  }

  auto visit(ast::constant const& x) -> void {
    found = found or contains_secret(x.value);
  }

  bool found = false;
};

auto contains_secret_source(ast::expression const& expr) -> bool {
  auto finder = secret_source_finder{};
  finder.visit(expr);
  return finder.found;
}

/// Constant evaluates an expression that produces unresolved secrets.
///
/// A Nova secret holds the resolved plain text, which is only available once
/// an operator runs. Constants instead carry the secret's name together with
/// its pending transformations, which this folder builds symbolically. It
/// supports the operations that the legacy evaluator supported on secrets:
/// concatenation via `+` and format strings, the `encode_*` and `decode_*`
/// functions, nesting in records and lists, and field access and indexing.
/// Everything without a secret goes to Nova.
class secret_folder {
public:
  explicit secret_folder(base_ctx ctx) : ctx_{ctx} {
  }

  auto fold(ast::expression const& expr) -> failure_or<data> {
    if (not contains_secret_source(expr)) {
      return const_eval_nova(expr, ctx_);
    }
    return expr.match([&](auto const& x) -> failure_or<data> {
      return fold_node(x);
    });
  }

private:
  auto fold_node(ast::constant const& x) -> failure_or<data> {
    return tenzir::match(x.value, [](auto const& value) -> data {
      return value;
    });
  }

  auto fold_node(ast::function_call const& x) -> failure_or<data> {
    if (is_std_call(x, "secret")) {
      if (x.args.size() != 1) {
        diagnostic::error("`secret` expects exactly one string argument")
          .primary(x)
          .emit(ctx_);
        return failure::promise();
      }
      TRY(auto name, fold(x.args.front()));
      auto* name_string = try_as<std::string>(&name);
      if (not name_string) {
        diagnostic::error("`secret` expects a string")
          .primary(x.args.front())
          .emit(ctx_);
        return failure::promise();
      }
      return secret::make_managed(*name_string);
    }
    if (auto operation = transformation_of(x)) {
      TRY(auto value, fold(x.args.front()));
      if (auto* s = try_as<secret>(&value)) {
        return s->with_operation(*operation);
      }
    }
    return unsupported(x);
  }

  auto fold_node(ast::binary_expr const& x) -> failure_or<data> {
    if (x.op != ast::binary_op::add) {
      return unsupported(x);
    }
    TRY(auto left, fold(x.left));
    TRY(auto right, fold(x.right));
    auto* left_secret = try_as<secret>(&left);
    auto* right_secret = try_as<secret>(&right);
    auto* left_string = try_as<std::string>(&left);
    auto* right_string = try_as<std::string>(&right);
    if (left_secret and right_secret) {
      return left_secret->with_appended(*right_secret);
    }
    if (left_secret and right_string) {
      return left_secret->with_appended(*right_string);
    }
    if (left_string and right_secret) {
      return right_secret->with_prepended(*left_string);
    }
    diagnostic::error("binary operator `+` not implemented for `{}` and `{}`",
                      type_kind_of_data(left), type_kind_of_data(right))
      .primary(x)
      .emit(ctx_);
    return failure::promise();
  }

  auto fold_node(ast::format_expr const& x) -> failure_or<data> {
    // Consecutive segments without secrets form a format expression of their
    // own, so that Nova formats their replacements.
    auto result = variant<std::string, secret>{};
    auto pending = std::vector<ast::format_expr::segment>{};
    auto append = [&](data value) {
      if (auto* str = try_as<std::string>(&value)) {
        if (auto* result_str = try_as<std::string>(&result)) {
          result_str->append(*str);
        } else {
          result = as<secret>(result).with_appended(*str);
        }
        return;
      }
      auto& value_secret = as<secret>(value);
      if (auto* result_str = try_as<std::string>(&result)) {
        result
          = result_str->empty()
              ? std::move(value_secret)
              : secret::make_literal(*result_str).with_appended(value_secret);
      } else {
        result = as<secret>(result).with_appended(value_secret);
      }
    };
    auto flush = [&]() -> failure_or<void> {
      if (pending.empty()) {
        return {};
      }
      auto group = ast::expression{
        ast::format_expr{std::exchange(pending, {}), x.get_location()}};
      TRY(auto value, const_eval_nova(group, ctx_));
      TENZIR_ASSERT(is<std::string>(value));
      append(std::move(value));
      return {};
    };
    for (auto const& segment : x.segments) {
      auto const* replacement = try_as<ast::format_expr::replacement>(segment);
      if (not replacement or not contains_secret_source(replacement->expr)) {
        pending.push_back(segment);
        continue;
      }
      TRY(auto value, fold(replacement->expr));
      if (not is<secret>(value)) {
        // Like the legacy evaluator, stringify other values, which prints
        // their nested secrets as `"***"`.
        pending.emplace_back(ast::format_expr::replacement{
          ast::constant::make(located{
            substitute_secrets(std::move(value), std::string{"***"}),
            replacement->expr.get_location(),
          }),
        });
        continue;
      }
      TRY(flush());
      append(std::move(value));
    }
    TRY(flush());
    return tenzir::match(std::move(result), [](auto x) -> data {
      return x;
    });
  }

  auto fold_node(ast::record const& x) -> failure_or<data> {
    // Like at runtime, a later item overwrites an earlier field of the same
    // name.
    auto result = record{};
    for (auto const& item : x.items) {
      if (auto const* field = try_as<ast::record::field>(item)) {
        TRY(auto value, fold(field->expr));
        result[field->name.name] = std::move(value);
        continue;
      }
      auto const& spread = as<ast::spread>(item);
      TRY(auto value, fold(spread.expr));
      if (auto* fields = try_as<record>(&value)) {
        for (auto& [name, field] : *fields) {
          result[name] = std::move(field);
        }
      } else if (not is<caf::none_t>(value)) {
        diagnostic::warning("expected `record`").primary(spread.expr).emit(ctx_);
      }
    }
    return result;
  }

  auto fold_node(ast::list const& x) -> failure_or<data> {
    auto result = list{};
    for (auto const& item : x.items) {
      if (auto const* expr = try_as<ast::expression>(item)) {
        TRY(auto value, fold(*expr));
        result.push_back(std::move(value));
        continue;
      }
      auto const& spread = as<ast::spread>(item);
      TRY(auto value, fold(spread.expr));
      if (auto* elements = try_as<list>(&value)) {
        std::ranges::move(*elements, std::back_inserter(result));
      } else if (not is<caf::none_t>(value)) {
        diagnostic::warning("expected `list`").primary(spread.expr).emit(ctx_);
      }
    }
    return result;
  }

  auto fold_node(ast::field_access const& x) -> failure_or<data> {
    TRY(auto subject, fold(x.left));
    if (auto* fields = try_as<record>(&subject)) {
      if (auto it = fields->find(x.name.name); it != fields->end()) {
        return std::move(it->second);
      }
    }
    auto miss = x;
    miss.left = without_secrets(std::move(subject), x.left);
    return fold_miss(x, ast::expression{std::move(miss)});
  }

  auto fold_node(ast::index_expr const& x) -> failure_or<data> {
    TRY(auto subject, fold(x.expr));
    TRY(auto index, fold(x.index));
    if (contains_secret(index)) {
      return unsupported(x.index);
    }
    if (auto* fields = try_as<record>(&subject)) {
      if (auto* name = try_as<std::string>(&index)) {
        if (auto it = fields->find(*name); it != fields->end()) {
          return std::move(it->second);
        }
      }
    } else if (auto* elements = try_as<list>(&subject)) {
      if (auto* position = try_as<int64_t>(&index)) {
        // Negative indices count from the end, like at runtime.
        auto const size = detail::narrow<int64_t>(elements->size());
        auto const resolved = *position < 0 ? size + *position : *position;
        if (resolved >= 0 and resolved < size) {
          return std::move((*elements)[detail::narrow<size_t>(resolved)]);
        }
      }
    }
    auto miss = x;
    miss.expr = without_secrets(std::move(subject), x.expr);
    miss.index
      = ast::constant::make(located{std::move(index), x.index.get_location()});
    return fold_miss(x, ast::expression{std::move(miss)});
  }

  auto fold_node(auto const& x) -> failure_or<data> {
    return unsupported(x);
  }

  /// A constant for the value of `source` with its secrets replaced by `null`.
  static auto without_secrets(data value, ast::expression const& source)
    -> ast::expression {
    return ast::constant::make(located{
      substitute_secrets(std::move(value), data{}), source.get_location()});
  }

  /// Evaluates a field access or index `miss` that did not find a value, so
  /// that Nova emits the same warnings as at runtime. The secrets in its
  /// subject are replaced by `null`, as they cannot be part of the result.
  auto fold_miss(auto const& x, ast::expression const& miss)
    -> failure_or<data> {
    TRY(auto value, const_eval_nova(miss, ctx_));
    if (not is<caf::none_t>(value)) {
      // A hit that the folder does not know, which must not yield a value
      // without its secrets.
      return unsupported(x);
    }
    return value;
  }

  /// Returns the secret transformation of a call to `encode_*` or `decode_*`
  /// with a single argument.
  static auto transformation_of(ast::function_call const& x)
    -> Option<fbs::data::SecretTransformations> {
    if (x.args.size() != 1) {
      return None{};
    }
    for (auto operation : fbs::data::EnumValuesSecretTransformations()) {
      if (is_std_call(x, fbs::data::EnumNameSecretTransformations(operation))) {
        return operation;
      }
    }
    return None{};
  }

  auto unsupported(auto const& x) -> failure_or<data> {
    diagnostic::error("unsupported operation on a secret")
      .primary(x)
      .note("constant expressions combine secrets only with `+`, format "
            "strings, the `encode_*` and `decode_*` functions, field access, "
            "and indexing")
      .emit(ctx_);
    return failure::promise();
  }

  base_ctx ctx_;
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

auto eval(const ast::expression& expr, const table_slice& input, base_ctx ctx)
  -> multi_series {
  return trace_panic(expr, [&] -> multi_series {
    // TODO: Do not create a new session here.
    auto sp = session_provider::make(ctx.dh(), ctx.reg());
    auto result = evaluator{&input, sp.as_session()}.eval(expr, {});
    TENZIR_ASSERT(result.length() == detail::narrow<int64_t>(input.rows()));
    return result;
  });
}

auto eval(const ast::field_path& expr, const table_slice& input, base_ctx ctx)
  -> series {
  return trace_panic(expr, [&] -> series {
    auto result = eval(expr.inner(), input, ctx);
    TENZIR_ASSERT(result.length() == detail::narrow<int64_t>(input.rows()));
    TENZIR_ASSERT(result.parts().size() == 1);
    return std::move(result.parts()[0]);
  });
}

auto eval(const ast::constant& expr, const table_slice& input, base_ctx ctx)
  -> series {
  return trace_panic(expr, [&] -> series {
    auto result = eval(ast::expression{expr}, input, ctx);
    TENZIR_ASSERT(result.length() == detail::narrow<int64_t>(input.rows()));
    TENZIR_ASSERT(result.parts().size() == 1);
    return std::move(result.parts()[0]);
  });
}

auto const_eval_series(const ast::expression& expr, base_ctx ctx)
  -> failure_or<series> {
  return trace_panic(expr, [&] -> failure_or<series> {
    // TODO: Do not create a new session here.
    try {
      auto sp = session_provider::make(ctx.dh(), ctx.reg());
      auto result = evaluator{nullptr, sp.as_session()}.eval(expr, {});
      TENZIR_ASSERT(result.length() == 1);
      TENZIR_ASSERT(result.parts().size() == 1);
      return std::move(result.part(0));
    } catch (failure fail) {
      return fail;
    }
  });
}

auto const_eval(const ast::expression& expr, base_ctx ctx)
  -> failure_or<located<data>> {
  return trace_panic(expr, [&] -> failure_or<located<data>> {
    TRY(auto value, secret_folder{ctx}.fold(expr));
    return located{std::move(value), expr.get_location()};
  });
}

auto try_const_eval(const ast::expression& expr, base_ctx ctx)
  -> Option<located<data>> {
  return trace_panic(expr, [&] -> Option<located<data>> {
    if (not expr.is_deterministic(ctx.reg())) {
      // TODO: This check is not ideal, as it is a bit too broad, and
      // incorrectly marks short-circuited expressions like `random() if false`
      // as non-deterministic just because they contain a call to a
      // non-deterministic function.
      return None{};
    }
    auto const_dh = collecting_diagnostic_handler{};
    if (auto result = const_eval(expr, base_ctx{const_dh, ctx.reg()})) {
      std::move(const_dh).forward_to(ctx);
      return std::move(*result);
    }
    return None{};
  });
}

auto eval(const ast::lambda_expr& lambda, const basic_series<list_type>& input,
          const table_slice& slice, base_ctx ctx) -> multi_series {
  return trace_panic(lambda, [&] -> multi_series {
    TENZIR_ASSERT(input.array);
    TENZIR_ASSERT(input.array->values());
    ensure_unary_lambda(lambda);
    TENZIR_ASSERT(std::cmp_equal(slice.rows(), input.length()));
    auto visitor = capture_extractor{};
    visitor.visit(lambda.body);
    const auto captures = std::move(visitor).result();
    const auto ty = input.type.value_type();
    // The input list array may be a slice of a larger backing array (e.g.,
    // from cached constant broadcasting). We must use only the values
    // referenced by this slice, not the full backing values array.
    auto values_start = input.array->value_offset(0);
    auto values_end = input.array->value_offset(input.array->length());
    auto values
      = input.array->values()->Slice(values_start, values_end - values_start);
    auto capture_offsets = std::vector<offset>{};
    for (const auto& capture : captures) {
      if (capture.path().front().id.name == lambda.param(0).name) {
        continue;
      }
      auto resolved = resolve(capture, slice.schema());
      if (const auto* off = try_as<offset>(resolved)) {
        capture_offsets.push_back(*off);
      }
    }
    if (capture_offsets.empty()) {
      return eval(lambda, series{ty, values}, ctx);
    }
    const auto to = check(
      ast::field_path::try_from(ast::root_field{lambda.param(0), false}));
    auto b = arrow::Int64Builder{tenzir::arrow_memory_pool()};
    check(b.Reserve(values->length()));
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
    const auto slice = assign(to, {ty, values}, repeated, ctx);
    return eval(lambda.body, slice, ctx);
  });
}

auto eval(const ast::lambda_expr& lambda, const multi_series& input,
          base_ctx ctx) -> multi_series {
  return trace_panic(lambda, [&] -> multi_series {
    ensure_unary_lambda(lambda);
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
          {lambda.param(0).name, part.type},
        },
      };
      auto arrow_schema = schema.to_arrow_schema();
      auto slice = table_slice{
        arrow::RecordBatch::Make(std::move(arrow_schema), input.length(),
                                 arrow::ArrayVector{part.array}),
        std::move(schema),
      };
      result.append(eval(lambda.body, slice, ctx));
    }
    return result;
  });
}

auto eval(const ast::lambda_expr& lambda, const data& input, base_ctx ctx)
  -> data {
  return trace_panic(lambda, [&] -> data {
    ensure_unary_lambda(lambda);
    const auto result = eval(lambda, data_to_series(input, int64_t{1}), ctx);
    TENZIR_ASSERT(result.parts().size() == 1);
    TENZIR_ASSERT(result.part(0).length() == 1);
    return materialize(view_at(*result.part(0).array, 0));
  });
}

} // namespace tenzir
