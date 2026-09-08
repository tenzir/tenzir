//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/operator_plugin.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"

#include <algorithm>
#include <map>
#include <ranges>

namespace tenzir::_::operator_plugin {

/// Wraps a diagnostic_handler to track whether any errors were emitted.
class error_tracking_handler : public diagnostic_handler {
public:
  explicit error_tracking_handler(diagnostic_handler& inner) : inner_{&inner} {
  }

  void emit(diagnostic d) override {
    if (d.severity == severity::error) {
      had_error_ = true;
    }
    inner_->emit(std::move(d));
  }

  auto had_error() const -> bool {
    return had_error_;
  }

private:
  diagnostic_handler* inner_;
  bool had_error_ = false;
};

/// A shared handle to a `Description` object that is also inspectable.
class SharedDescription {
public:
  SharedDescription() = default;

  explicit SharedDescription(std::string origin,
                             std::shared_ptr<const Description> desc)
    : origin_{std::move(origin)}, desc_{std::move(desc)} {
  }

  auto operator*() const -> const Description& {
    TENZIR_ASSERT(desc_);
    return *desc_;
  }

  auto operator->() const -> const Description* {
    TENZIR_ASSERT(desc_);
    return desc_.get();
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, SharedDescription& x) -> bool {
    auto ok = f.apply(x.origin_);
    if (ok and Inspector::is_loading) {
      auto plugin = plugins::find<OperatorPlugin>(x.origin_);
      TENZIR_ASSERT(plugin);
      x.desc_ = plugin->describe_shared();
    }
    return ok;
  }

private:
  std::string origin_;
  std::shared_ptr<const Description> desc_;
};

namespace {

auto is_hidden_argument(std::string_view name) -> bool {
  return name.starts_with('_');
}

auto is_hidden_argument(const Named& named) -> bool {
  return std::ranges::all_of(named.names, [](const auto& name) {
    return is_hidden_argument(name);
  });
}

auto display_names(const Named& named) -> std::string {
  auto names = named.names | std::views::filter([](const auto& name) {
                 return not is_hidden_argument(name);
               });
  return fmt::format("{}", fmt::join(names, "|"));
}

auto primary_name(const Named& named) -> std::string_view {
  TENZIR_ASSERT(not named.names.empty());
  return named.names.front();
}

auto setter_to_type_string(const AnySetter& setter) -> std::string {
  return match(
    setter,
    []<class T>(const Setter<located<T>>&) -> std::string {
      if constexpr (std::same_as<T, std::string>) {
        return "string";
      } else if constexpr (std::same_as<T, secret>) {
        return "string";
      } else if constexpr (std::same_as<T, int64_t>
                           or std::same_as<T, uint64_t>) {
        return "int";
      } else if constexpr (std::same_as<T, double>) {
        return "number";
      } else if constexpr (std::same_as<T, bool>) {
        return "bool";
      } else if constexpr (std::same_as<T, ir::pipeline>) {
        return "{ … }";
      } else if constexpr (std::same_as<T, data>) {
        return "any";
      } else {
        // Fall back to type kind if available.
        return fmt::format("{}", type_kind::of<data_to_type_t<T>>);
      }
    },
    [](const Setter<ast::expression>&) -> std::string {
      return "any";
    },
    [](const Setter<ast::field_path>&) -> std::string {
      return "field";
    },
    [](const Setter<ast::lambda_expr>&) -> std::string {
      return "lambda";
    });
}

} // namespace

auto get_usage(const Description& desc) -> std::string {
  auto result = desc.name;
  auto has_previous = false;
  auto in_brackets = false;
  // Print positional arguments.
  for (auto [idx, positional] : detail::enumerate(desc.positional)) {
    if (is_hidden_argument(positional.name)) {
      continue;
    }
    auto is_optional = desc.first_optional and idx >= *desc.first_optional;
    if (std::exchange(has_previous, true)) {
      result += ", ";
    } else {
      result += ' ';
    }
    if (is_optional and not in_brackets) {
      result += '[';
      in_brackets = true;
    }
    result += positional.name;
    result += ':';
    result += positional.type.empty() ? setter_to_type_string(positional.setter)
                                      : positional.type;
  }
  // Print required named arguments first.
  for (const auto& named : desc.named) {
    if (not named.required) {
      continue;
    }
    if (is_hidden_argument(named)) {
      continue;
    }
    if (in_brackets) {
      result += ']';
      in_brackets = false;
    }
    if (std::exchange(has_previous, true)) {
      result += ", ";
    } else {
      result += ' ';
    }
    result += display_names(named);
    result += '=';
    result
      += named.type.empty() ? setter_to_type_string(named.setter) : named.type;
  }
  // Print optional named arguments.
  for (const auto& named : desc.named) {
    if (named.required) {
      continue;
    }
    if (is_hidden_argument(named)) {
      continue;
    }
    if (std::exchange(has_previous, true)) {
      result += ", ";
    } else {
      result += ' ';
    }
    if (not in_brackets) {
      result += '[';
      in_brackets = true;
    }
    result += display_names(named);
    result += '=';
    result
      += named.type.empty() ? setter_to_type_string(named.setter) : named.type;
  }
  if (in_brackets) {
    result += ']';
  }
  return result;
}

auto ArgumentStore::parse(const Description& desc, ast::entity op,
                          std::vector<ast::expression> args, compile_ctx ctx)
  -> failure_or<ArgumentStore> {
  auto result = ArgumentStore{};
  result.op_ = std::move(op);
  // Bind non-pipeline arguments.
  for (auto& arg : args) {
    if (is<ast::assignment>(arg) or not is<ast::pipeline_expr>(arg)) {
      TRY(arg.bind(ctx));
    }
  }
  auto failed = false;
  auto emit = [&](diagnostic_builder d) {
    failed = true;
    std::move(d).usage(get_usage(desc)).docs(desc.docs).emit(ctx);
  };
  // Track which named arguments have been found.
  auto named_found = std::vector<Option<location>>(desc.named.size());
  // Parse arguments, separating positional from named.
  auto positional_idx = size_t{0};
  auto min_positional = desc.first_optional.value_or(desc.positional.size());
  auto max_positional = desc.positional.size();
  for (auto& arg : args) {
    if (auto* assignment = try_as<ast::assignment>(arg)) {
      // Named argument.
      auto selector = ast::selector::try_from(assignment->left);
      auto* sel = selector ? try_as<ast::field_path>(&*selector) : nullptr;
      if (not sel or sel->has_this() or sel->path().size() != 1
          or sel->path()[0].has_question_mark) {
        emit(
          diagnostic::error("invalid argument name").primary(assignment->left));
        continue;
      }
      auto& name = sel->path()[0].id.name;
      auto it = std::ranges::find_if(desc.named, [&](const Named& named) {
        return std::ranges::find(named.names, name) != named.names.end();
      });
      if (it == desc.named.end()) {
        emit(diagnostic::error("named argument `{}` does not exist", name)
               .primary(assignment->left));
        continue;
      }
      auto idx = static_cast<size_t>(it - desc.named.begin());
      if (named_found[idx]) {
        emit(diagnostic::error("duplicate named argument `{}`", name)
               .primary(*named_found[idx])
               .primary(assignment->left.get_location()));
        continue;
      }
      named_found[idx] = assignment->left.get_location();
      // Store the named argument for later processing.
      result.named_args_.push_back(
        NamedArg{idx, Incomplete{assignment->right}});
    } else if (auto* pipe_expr = try_as<ast::pipeline_expr>(arg)) {
      if (not desc.pipeline) {
        emit(diagnostic::error("no pipeline argument expected").primary(arg));
        continue;
      }
      if (result.pipeline_) {
        emit(diagnostic::error("duplicate pipeline argument")
               .primary(arg)
               .secondary(result.pipeline_->pipeline.source,
                          "previously provided here"));
        continue;
      }
      result.pipeline_ = PipelineArg{};
      const auto& pipe = desc.pipeline;
      // Open a scope if there are let bindings.
      auto scope = ctx.open_scope();
      for (auto [binding_idx, binding] :
           detail::enumerate(pipe->let_bindings)) {
        auto id = scope.let(binding.name);
        // Store let_id for later application during spawn().
        auto [_, inserted] = result.pipeline_->let_ids.emplace(binding_idx, id);
        TENZIR_ASSERT(inserted);
      }
      // Compile the pipeline (with the scope if present).
      auto pipe_loc = pipe_expr->get_location();
      TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
      result.pipeline_->pipeline = located{std::move(pipe_ir), pipe_loc};
    } else {
      // Positional argument.
      // Check if this is a variadic argument position
      auto is_variadic
        = desc.variadic_index and positional_idx == *desc.variadic_index;

      if (positional_idx >= max_positional and not is_variadic) {
        emit(diagnostic::error("too many positional arguments").primary(arg));
        continue;
      }
      result.args_.push_back(Incomplete{std::move(arg)});
      // Don't increment positional_idx if we're at the variadic position
      if (not is_variadic) {
        ++positional_idx;
      }
    }
  }
  // Check for missing required variadic arguments ()
  if (desc.variadic_index
      and *desc.variadic_index
            < desc.first_optional.value_or(desc.positional.size())) {
    // Variadic is required, so we need at least one argument at that position
    if (result.args_.size() < min_positional) {
      auto specifier
        = min_positional == max_positional ? "exactly" : "at least";
      emit(diagnostic::error("expected {} {} positional arguments", specifier,
                             min_positional)
             .primary(result.op_));
    }
  } else if (positional_idx < min_positional) {
    auto specifier = min_positional == max_positional ? "exactly" : "at least";
    emit(diagnostic::error("expected {} {} positional argument{}", specifier,
                           min_positional, min_positional == 1 ? "" : "s")
           .primary(result.op_));
  }
  // Check for missing required named arguments.
  for (auto [idx, named] : detail::enumerate(desc.named)) {
    if (named.required and not named_found[idx]) {
      emit(diagnostic::error("required argument `{}` was not provided",
                             primary_name(named))
             .primary(result.op_));
    }
  }
  // Check for missing required subpipeline.
  if (desc.pipeline and desc.pipeline->required and not result.pipeline_) {
    emit(diagnostic::error("required subpipeline was not provided")
           .primary(result.op_));
  }
  if (failed) {
    return failure::promise();
  }
  return result;
}

auto ArgumentStore::substitute(const Description& desc, substitute_ctx ctx,
                               bool instantiate) -> failure_or<void> {
  // Helper to substitute an argument using its setter.
  auto substitute_arg = [&](Arg& arg, const AnySetter& setter,
                            bool is_named) -> failure_or<void> {
    auto* incomplete = try_as<Incomplete>(arg);
    if (not incomplete) {
      return {};
    }
    auto& expr = incomplete->expr;
    TRY(auto subst, expr.substitute(ctx));
    auto remaining = subst == ast::substitute_result::some_remaining;
    if (remaining) {
      return {};
    }
    auto is_constant = not is<Setter<ast::expression>>(setter)
                       and not is<Setter<ast::lambda_expr>>(setter)
                       and not is<Setter<ast::field_path>>(setter);
    if (is_constant) {
      if (instantiate or expr.is_deterministic(ctx)) {
        // Handle boolean flags for named arguments.
        if (is_named and is<Setter<located<bool>>>(setter)) {
          TRY(auto constant, const_eval(expr, ctx));
          auto value = std::move(constant.inner);
          auto* boolean = try_as<bool>(value);
          if (not boolean) {
            diagnostic::error("expected bool but got {}", "TODO")
              .primary(expr)
              .docs(desc.docs)
              .emit(ctx);
            return failure::promise();
          }
          arg = located{*boolean, expr.get_location()};
          return {};
        }
        TRY(auto constant, const_eval(expr, ctx));
        auto value = std::move(constant.inner);
        if (auto* integer = try_as<int64_t>(value);
            integer and is<Setter<located<uint64_t>>>(setter)) {
          if (*integer < 0) {
            diagnostic::error("expected positive integer, got `{}`", *integer)
              .primary(expr)
              .docs(desc.docs)
              .emit(ctx);
            return failure::promise();
          }
          value = detail::narrow<uint64_t>(*integer);
        }
        if (auto* str = try_as<std::string>(value);
            str and is<Setter<located<secret>>>(setter)) {
          value = secret::make_literal(*str);
        }
        auto result = match(
          setter,
          [&]<class T>(const Setter<located<T>>&) -> failure_or<Arg> {
            auto* cast = try_as<T>(value);
            if (not cast) {
              diagnostic::error("expected argument of type `{}`, but got `{}`",
                                type_kind::of<data_to_type_t<T>>,
                                type_kind_of_data(value))
                .primary(expr)
                .emit(ctx);
              return failure::promise();
            }
            return located{std::move(*cast), expr.get_location()};
          },
          [&](const Setter<located<data>>&) -> failure_or<Arg> {
            return located{std::move(value), expr.get_location()};
          },
          [&](const Setter<located<ir::pipeline>>&) -> failure_or<Arg> {
            // Pipelines are compiled in `parse()`, not during substitute.
            TENZIR_UNREACHABLE();
          },
          [&]<class T>(const Setter<T>&) -> failure_or<Arg> {
            TENZIR_TODO();
          },
          [&](const Setter<ast::expression>&) -> failure_or<Arg> {
            // We already checked this above.
            TENZIR_UNREACHABLE();
          });
        TRY(arg, result);
      }
    } else {
      TRY(match(
        setter,
        [&](const Setter<ast::lambda_expr>&) -> failure_or<void> {
          auto* lambda = try_as<ast::lambda_expr>(*expr.kind);
          if (not lambda) {
            diagnostic::error("expected a lambda expression")
              .primary(expr)
              .emit(ctx);
            return failure::promise();
          }
          arg = std::move(*lambda);
          return {};
        },
        [&](const Setter<ast::field_path>&) -> failure_or<void> {
          auto loc = expr.get_location();
          auto fp = ast::field_path::try_from(std::move(expr));
          if (not fp) {
            diagnostic::error("expected a field path").primary(loc).emit(ctx);
            return failure::promise();
          }
          arg = std::move(*fp);
          return {};
        },
        [&](const Setter<ast::expression>&) -> failure_or<void> {
          arg = std::move(expr);
          return {};
        },
        [&]<class T>(const Setter<T>&) -> failure_or<void> {
          TENZIR_UNREACHABLE();
        }));
    }
    return {};
  };
  // Substitute positional arguments.
  for (auto [idx, arg] : detail::enumerate(args_)) {
    // For variadic arguments, all args >= variadic_index map to variadic_index
    auto pos_idx = idx;
    if (desc.variadic_index and idx >= *desc.variadic_index) {
      pos_idx = *desc.variadic_index;
    }
    TENZIR_ASSERT(pos_idx < desc.positional.size());
    TRY(substitute_arg(arg, desc.positional[pos_idx].setter, false));
  }
  // Substitute named arguments.
  for (auto& named_arg : named_args_) {
    TENZIR_ASSERT(named_arg.index < desc.named.size());
    TRY(substitute_arg(named_arg.value, desc.named[named_arg.index].setter,
                       true));
  }
  // Substitute the subpipeline if present.
  if (pipeline_) {
    TENZIR_ASSERT(desc.pipeline);
    TRY(pipeline_->pipeline.inner.substitute(
      ctx, instantiate and desc.pipeline->instantiate));
  }
  // Run custom validation if provided.
  if (desc.validator) {
    auto error_tracker = error_tracking_handler{ctx};
    auto validate_ctx = describe_ctx(desc, error_tracker);
    (*desc.validator)(validate_ctx);
    if (error_tracker.had_error()) {
      return failure::promise();
    }
  }
  return {};
}

namespace {

/// Applies a parsed argument to the argument bundle.
auto apply_arg(const Arg& arg, const AnySetter& setter, Any& args) -> void {
  match(
    arg,
    [&]<class T>(const T& x) {
      as<Setter<T>>(setter)(args, x);
    },
    [&](const Incomplete& x) {
      // Arguments that the operator receives as an expression need no
      // evaluation, so they can be materialized even before substitution.
      // Operators that are created during optimization rely on this.
      const auto* expr = try_as<Setter<ast::expression>>(&setter);
      TENZIR_ASSERT(expr, "argument was not resolved during substitution");
      (*expr)(args, x.expr);
    });
}

} // namespace

auto ArgumentStore::materialize(const Description& desc,
                                ir::OptimizeRequest consumed) const -> Any {
  auto args = desc.make_args();
  for (auto [idx, arg] : detail::enumerate(args_)) {
    // For variadic arguments, all args >= variadic_index map to variadic_index.
    auto pos_idx = idx;
    if (desc.variadic_index and idx >= *desc.variadic_index) {
      pos_idx = *desc.variadic_index;
    }
    TENZIR_ASSERT(pos_idx < desc.positional.size());
    apply_arg(arg, desc.positional[pos_idx].setter, args);
  }
  for (const auto& named_arg : named_args_) {
    TENZIR_ASSERT(named_arg.index < desc.named.size());
    apply_arg(named_arg.value, desc.named[named_arg.index].setter, args);
  }
  if (pipeline_) {
    // This is already checked in `parse()`.
    TENZIR_ASSERT(desc.pipeline);
    if (desc.pipeline->setter) {
      (*desc.pipeline->setter)(args, pipeline_->pipeline);
    }
    for (const auto& [binding_idx, id] : pipeline_->let_ids) {
      auto& binding = desc.pipeline->let_bindings[binding_idx];
      binding.setter(args, id);
    }
  }
  if (desc.set_filter) {
    (*desc.set_filter)(args, std::move(consumed.filter));
  } else {
    TENZIR_ASSERT(consumed.filter.empty());
  }
  if (desc.set_limit) {
    (*desc.set_limit)(args, consumed.limit);
  } else {
    TENZIR_ASSERT(not consumed.limit);
  }
  if (desc.set_projection) {
    (*desc.set_projection)(args, std::move(consumed.projection));
  } else {
    TENZIR_ASSERT(not consumed.projection);
  }
  if (desc.set_operator_location) {
    (*desc.set_operator_location)(args, main_location());
  }
  if (desc.set_order) {
    (*desc.set_order)(args, consumed.order);
  }
  return args;
}

auto ArgumentStore::describe_ctx(const Description& desc,
                                 diagnostic_handler& dh) const -> DescribeCtx {
  return DescribeCtx{args_, named_args_, pipeline_, desc, main_location(), dh};
}

class GenericIr final : public ir::Operator {
public:
  GenericIr() = default;

  static auto make(SharedDescription desc, ast::entity op,
                   std::vector<ast::expression> args, compile_ctx ctx)
    -> failure_or<GenericIr> {
    TRY(auto store,
        ArgumentStore::parse(*desc, std::move(op), std::move(args), ctx));
    auto result = GenericIr{};
    result.store_ = std::move(store);
    result.desc_ = std::move(desc);
    return result;
  }

  auto name() const -> std::string override {
    return "GenericIr";
  }

  auto display_name() const -> std::string override {
    return desc_->name;
  }

  auto parallelizable() const -> bool override {
    if (desc_->parallelizable_when) {
      return (*desc_->parallelizable_when)(materialize_args());
    }
    return desc_->parallelizable;
  }

  auto partition_keys() const -> std::vector<ast::expression> override {
    if (not desc_->partition_keys) {
      return {};
    }
    return (*desc_->partition_keys)(materialize_args());
  }

  auto copy() const -> Box<ir::Operator> override {
    return GenericIr{*this};
  }

  auto move() && -> Box<ir::Operator> override {
    return GenericIr{std::move(*this)};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (desc_->spawner) {
      auto ctx = describe_ctx(dh);
      TRY(auto spawn, (*desc_->spawner)(input, ctx));
      if (spawn) {
        return match(
          *spawn,
          []<class Input, class Output, bool MultipleOutputPorts>(
            Spawn<Input, Output, MultipleOutputPorts>&) -> element_type_tag {
            return tag_v<Output>;
          });
      }
    }
    for (auto& spawn : desc_->spawns) {
      auto output
        = match(spawn,
                [&]<class Input, class Output, bool MultipleOutputPorts>(
                  const Spawn<Input, Output, MultipleOutputPorts>&)
                  -> Option<element_type_tag> {
                  if (input.is<Input>()) {
                    return tag_v<Output>;
                  }
                  return None{};
                });
      if (output) {
        return *output;
      }
    }
    diagnostic::error("operator does not accept {}", input)
      .primary(main_location())
      .docs(desc_->docs)
      .emit(dh);
    return failure::promise();
  }

  auto materialize_args() const -> Any {
    return store_.materialize(*desc_, ir::OptimizeRequest{
                                        .filter = filter_,
                                        .order = order_,
                                        .limit = limit_,
                                        .projection = projection_,
                                      });
  }

  auto describe_ctx(diagnostic_handler& dh) const -> DescribeCtx {
    return store_.describe_ctx(*desc_, dh);
  }

  auto spawn(element_type_tag input) const -> AnyOperator override {
    auto spawner = Option<AnySpawn>{};
    if (desc_->spawner) {
      auto noop_dh = null_diagnostic_handler{};
      auto ctx = describe_ctx(noop_dh);
      auto result = (*desc_->spawner)(input, ctx);
      TENZIR_ASSERT(result);
      if (*result) {
        spawner = std::move(**result);
      }
    }
    auto args = materialize_args();
    auto with_name = [&](AnyOperator op) -> AnyOperator {
      match(op, [&](auto& op) {
        op->with_name(desc_->name);
      });
      return op;
    };
    if (spawner) {
      return with_name(match(*spawner, [&](auto& spawner) -> AnyOperator {
        return spawner(std::move(args));
      }));
    }
    for (auto& spawn : desc_->spawns) {
      auto result
        = match(spawn,
                [&]<class Input, class Output, bool MultipleOutputPorts>(
                  const Spawn<Input, Output, MultipleOutputPorts>& spawn)
                  -> Option<AnyOperator> {
                  if (input.is<Input>()) {
                    return spawn(std::move(args));
                  }
                  return None{};
                });
      if (result) {
        return with_name(std::move(*result));
      }
    }
    TENZIR_UNREACHABLE();
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(store_.substitute(*desc_, ctx, instantiate));
    for (auto& expr : filter_) {
      TRY(expr.substitute(ctx));
    }
    return {};
  }

  auto optimize(ir::OptimizeRequest req,
                const ir::OptimizeCtx& octx) && -> ir::OptimizeResult override {
    TENZIR_ASSERT(desc_->optimizer);
    auto filter = std::move(req.filter);
    auto order = req.order;
    auto& pipe = store_.pipeline();
    // subpipeline
    if (pipe and desc_->pipeline) {
      switch (desc_->pipeline->sub_optimize) {
        case SubOptimize::from_downstream: {
          // The subpipeline produces our output, so it can consume downstream
          // hints too. Repeated subpipelines may overproduce, but downstream
          // head/select still enforce the global result.
          auto sub
            = std::move(pipe->pipeline.inner)
                .optimize(ir::OptimizeRequest{.filter = std::move(filter),
                                              .order = order,
                                              .limit = req.limit,
                                              .projection
                                              = std::move(req.projection)},
                          octx);
          // use sub's request instead of the downstream one
          filter = std::move(sub.filter);
          order = sub.order;
          req.limit = sub.limit;
          req.projection = std::move(sub.projection);
          pipe->pipeline.inner = std::move(sub.replacement);
          break;
        }
        case SubOptimize::fork: {
          // independent optimize; the branch's limit and projection describe
          // the branch input and are discarded
          auto sub
            = std::move(pipe->pipeline.inner)
                .optimize(ir::OptimizeRequest{.filter = {},
                                              .order = EventOrder::ordered},
                          octx);
          // fork is filter barrier
          sub.replacement.prepend(std::move(sub.filter));
          pipe->pipeline.inner = std::move(sub.replacement);
          // only relax upstream ordering if both branches are ok with unordered
          order = stronger_event_order(order, sub.order);
          break;
        }
        case SubOptimize::off:
          break;
      }
    }
    order_ = weaker_event_order(order_, order);
    // extract filters into the operator
    if (desc_->set_filter) {
      filter_.append_range(filter | std::views::as_rvalue);
      filter = ir::OptimizeFilter{};
    }
    // extract limit and projection into the operator
    if (desc_->set_limit) {
      // The limit counts events that pass the filter, so an operator can only
      // interpret it if it also consumes the filter.
      TENZIR_ASSERT(desc_->set_filter);
      if (req.limit) {
        limit_ = limit_ ? std::min(*limit_, *req.limit) : *req.limit;
      }
      req.limit = None{};
    }
    if (desc_->set_projection) {
      // This operator already incorporates earlier pushdown. A later pass
      // may narrow its output further, but must not undo those restrictions
      // when optimizing a subpipeline without its enclosing consumer.
      ir::intersect_projection(projection_, req.projection);
      req.projection = None{};
    }
    // run optimizer
    auto noop_dh = null_diagnostic_handler{};
    auto ctx = describe_ctx(noop_dh);
    auto optimization
      = (*desc_->optimizer)(ctx, ir::OptimizeRequest{
                                   .filter = std::move(filter),
                                   .order = order_,
                                   .limit = req.limit,
                                   .projection = std::move(req.projection),
                                 });
    // A projection pushed upstream must cover the references of the predicates
    // kept behind this operator, because those run on upstream's fields.
    for (const auto& expr : optimization.filter_self) {
      ir::add_refs_to_projection(optimization.projection_upstream, expr);
    }
    auto replacement = std::vector<Box<Operator>>{};
    // construct replacement
    if (pipe and desc_->pipeline
        and desc_->pipeline->sub_optimize == SubOptimize::from_downstream) {
      // special case: down stream is the subpipeline
      pipe->pipeline.inner.prepend(std::move(optimization.filter_self));
      optimization.filter_self.clear();
    }
    if (not optimization.drop) {
      replacement.emplace_back(std::move(*this));
    }
    for (auto& expr : optimization.filter_self) {
      replacement.push_back(make_where_ir(expr));
    }
    return {
      .filter = std::move(optimization.filter_upstream),
      .order = optimization.order,
      .replacement = ir::pipeline{{}, std::move(replacement)},
      .limit = optimization.limit_upstream,
      .projection = std::move(optimization.projection_upstream),
    };
  }

  auto main_location() const -> location override {
    return store_.main_location();
  }

private:
  friend auto inspect(auto& f, GenericIr& x) -> bool {
    return f.object(x).fields(
      f.field("desc", x.desc_), f.field("store", x.store_),
      f.field("filter", x.filter_), f.field("order", x.order_),
      f.field("limit", x.limit_), f.field("projection", x.projection_));
  }

  /// The parsed arguments of the invocation.
  ArgumentStore store_;

  /// The filter passed to `optimize` (only if the operator wants to consume it).
  ir::OptimizeFilter filter_;

  /// The weakest ordering guarantee seen across all `optimize()` calls.
  /// Initialized to `ordered` (strongest); each call takes the max.
  EventOrder order_ = EventOrder::ordered;

  /// The limit passed to `optimize` (only if the operator wants to consume it).
  /// Across repeated `optimize()` calls, the smallest limit wins.
  Option<uint64_t> limit_;

  /// The projection passed to `optimize` (only if the operator wants to consume
  /// it). Repeated passes intersect the hints; `None` is unrestricted.
  Option<ir::OptimizeProjection> projection_;

  /// The object describing the available parameters.
  SharedDescription desc_;
};

auto OperatorPlugin::compile(ast::invocation inv, compile_ctx ctx) const
  -> failure_or<ir::CompileResult> {
  TRY(auto ir, GenericIr::make(SharedDescription{name(), describe_shared()},
                               std::move(inv.op), std::move(inv.args), ctx));
  return ir;
}

auto OperatorPlugin::describe_shared() const
  -> std::shared_ptr<const Description> {
  std::call_once(desc_init_flag_, [this] {
    auto desc = std::make_shared<Description>(describe());
    if (desc->name.empty()) {
      desc->name = name();
    }
    constexpr auto tql2_prefix = std::string_view{"tql2."};
    if (desc->name.starts_with(tql2_prefix)) {
      desc->name.erase(0, tql2_prefix.size());
    }
    if (desc->docs.empty()) {
      // Names are flat because modules are reserved for packages, so the name
      // is also the documentation slug.
      desc->docs = "https://tenzir.com/docs/reference/operators/" + desc->name;
    }
    cached_desc_ = std::move(desc);
  });
  return cached_desc_;
}

} // namespace tenzir::_::operator_plugin

TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::_::operator_plugin::GenericIr>)
