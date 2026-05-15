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

#include <map>

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

auto display_names(const Named& named) -> std::string {
  return fmt::format("{}", fmt::join(named.names, "|"));
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

auto get_usage(const Description& desc) -> std::string {
  auto result = desc.name;
  auto has_previous = false;
  auto in_brackets = false;
  // Print positional arguments.
  for (auto [idx, positional] : detail::enumerate(desc.positional)) {
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

} // namespace

class GenericIr final : public ir::Operator {
public:
  GenericIr() = default;

  static auto make(SharedDescription desc, ast::entity op,
                   std::vector<ast::expression> args, compile_ctx ctx)
    -> failure_or<GenericIr> {
    auto result = GenericIr{};
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
      std::move(d).usage(get_usage(*desc)).docs(desc->docs).emit(ctx);
    };
    // Track which named arguments have been found.
    auto named_found = std::vector<std::optional<location>>(desc->named.size());
    // Parse arguments, separating positional from named.
    auto positional_idx = size_t{0};
    auto min_positional
      = desc->first_optional.value_or(desc->positional.size());
    auto max_positional = desc->positional.size();
    for (auto& arg : args) {
      if (auto* assignment = try_as<ast::assignment>(arg)) {
        // Named argument.
        auto* sel = try_as<ast::field_path>(assignment->left);
        if (not sel or sel->has_this() or sel->path().size() != 1
            or sel->path()[0].has_question_mark) {
          emit(diagnostic::error("invalid argument name")
                 .primary(assignment->left));
          continue;
        }
        auto& name = sel->path()[0].id.name;
        auto it = std::ranges::find_if(desc->named, [&](const Named& named) {
          return std::ranges::find(named.names, name) != named.names.end();
        });
        if (it == desc->named.end()) {
          emit(diagnostic::error("named argument `{}` does not exist", name)
                 .primary(assignment->left));
          continue;
        }
        auto idx = static_cast<size_t>(it - desc->named.begin());
        if (named_found[idx]) {
          emit(diagnostic::error("duplicate named argument `{}`", name)
                 .primary(*named_found[idx])
                 .primary(arg.get_location()));
          continue;
        }
        named_found[idx] = arg.get_location();
        // Store the named argument for later processing.
        result.named_args_.push_back(
          NamedArg{idx, Incomplete{assignment->right}});
      } else if (auto* pipe_expr = try_as<ast::pipeline_expr>(arg)) {
        if (not desc->pipeline) {
          emit(diagnostic::error("no pipeline argument expected").primary(arg));
          continue;
        }
        result.pipeline_ = PipelineArg{};
        const auto& pipe = desc->pipeline;
        // Open a scope if there are let bindings.
        auto scope = ctx.open_scope();
        for (auto [binding_idx, binding] :
             detail::enumerate(pipe->let_bindings)) {
          auto id = scope.let(binding.name);
          // Store let_id for later application during spawn().
          auto [_, inserted]
            = result.pipeline_->let_ids.emplace(binding_idx, id);
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
          = desc->variadic_index and positional_idx == *desc->variadic_index;

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
    if (desc->variadic_index
        and *desc->variadic_index
              < desc->first_optional.value_or(desc->positional.size())) {
      // Variadic is required, so we need at least one argument at that position
      if (result.args_.size() < min_positional) {
        auto specifier
          = min_positional == max_positional ? "exactly" : "at least";
        emit(diagnostic::error("expected {} {} positional arguments", specifier,
                               min_positional)
               .primary(result.op_));
      }
    } else if (positional_idx < min_positional) {
      auto specifier
        = min_positional == max_positional ? "exactly" : "at least";
      emit(diagnostic::error("expected {} {} positional argument{}", specifier,
                             min_positional, min_positional == 1 ? "" : "s")
             .primary(result.op_));
    }
    // Check for missing required named arguments.
    for (auto [idx, named] : detail::enumerate(desc->named)) {
      if (named.required and not named_found[idx]) {
        emit(diagnostic::error("required argument `{}` was not provided",
                               primary_name(named))
               .primary(result.op_));
      }
    }
    // Check for missing required subpipeline.
    if (desc->pipeline and desc->pipeline->required and not result.pipeline_) {
      emit(diagnostic::error("required subpipeline was not provided")
             .primary(result.op_));
    }
    if (failed) {
      return failure::promise();
    }
    result.desc_ = std::move(desc);
    return result;
  }

  auto name() const -> std::string override {
    return "GenericIr";
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (desc_->spawner) {
      auto ctx = DescribeCtx{args_,  named_args_,     pipeline_,
                             *desc_, main_location(), dh};
      TRY(auto spawn, (*desc_->spawner)(input, ctx));
      if (spawn) {
        return match(*spawn,
                     []<class Input, class Output>(
                       Spawn<Input, Output>&) -> element_type_tag {
                       return tag_v<Output>;
                     });
      }
    }
    for (auto& spawn : desc_->spawns) {
      auto output = match(
        spawn,
        [&]<class Input, class Output>(
          const Spawn<Input, Output>&) -> std::optional<element_type_tag> {
          if (input.is<Input>()) {
            return tag_v<Output>;
          }
          return std::nullopt;
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

  auto spawn(element_type_tag input) && -> AnyOperator override {
    // The spawner must be retrieved before filling args, because we move them
    // out and thus the passed `DescribeCtx` would be incomplete.
    auto spawner = std::optional<AnySpawn>{};
    if (desc_->spawner) {
      auto noop_dh = null_diagnostic_handler{};
      auto ctx = DescribeCtx{args_,  named_args_,     pipeline_,
                             *desc_, main_location(), noop_dh};
      auto result = (*desc_->spawner)(input, ctx);
      TENZIR_ASSERT(result);
      if (*result) {
        spawner = std::move(**result);
      }
    }
    auto args = desc_->make_args();
    for (auto [idx, arg] : detail::enumerate(args_)) {
      match(
        std::move(arg),
        [&]<class T>(T x) {
          // For variadic arguments, all args >= variadic_index map to
          // variadic_index
          auto pos_idx = idx;
          if (desc_->variadic_index and idx >= *desc_->variadic_index) {
            pos_idx = *desc_->variadic_index;
          }
          TENZIR_ASSERT(pos_idx < desc_->positional.size());
          as<Setter<T>>(desc_->positional[pos_idx].setter)(args, std::move(x));
        },
        [](Incomplete) {
          TENZIR_UNREACHABLE();
        });
    }
    for (auto& named_arg : named_args_) {
      match(
        std::move(named_arg.value),
        [&]<class T>(T x) {
          TENZIR_ASSERT(named_arg.index < desc_->named.size());
          as<Setter<T>>(desc_->named[named_arg.index].setter)(args,
                                                              std::move(x));
        },
        [](Incomplete) {
          TENZIR_UNREACHABLE();
        });
    }
    if (pipeline_) {
      // This is already checked in make().
      TENZIR_ASSERT(desc_->pipeline);
      desc_->pipeline->setter(args, std::move(pipeline_->pipeline));
      for (const auto& [binding_idx, id] : pipeline_->let_ids) {
        auto& binding = desc_->pipeline->let_bindings[binding_idx];
        binding.setter(args, id);
      }
    }
    if (desc_->set_filter) {
      (*desc_->set_filter)(args, std::move(filter_));
    } else {
      TENZIR_ASSERT(filter_.empty());
    }
    if (desc_->set_operator_location) {
      (*desc_->set_operator_location)(args, main_location());
    }
    if (desc_->set_order) {
      (*desc_->set_order)(args, order_);
    }
    if (spawner) {
      return match(*spawner, [&](auto& spawner) -> AnyOperator {
        return spawner(std::move(args));
      });
    }
    for (auto& spawn : desc_->spawns) {
      auto result = match(
        spawn,
        [&]<class Input, class Output>(
          const Spawn<Input, Output>& spawn) -> std::optional<AnyOperator> {
          if (input.is<Input>()) {
            return spawn(std::move(args));
          }
          return std::nullopt;
        });
      if (result) {
        return std::move(*result);
      }
    }
    TENZIR_UNREACHABLE();
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
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
            TRY(auto value, const_eval(expr, ctx));
            auto* boolean = try_as<bool>(value);
            if (not boolean) {
              diagnostic::error("expected bool but got {}", "TODO")
                .primary(expr)
                .docs(desc_->docs)
                .emit(ctx);
              return failure::promise();
            }
            arg = located{*boolean, expr.get_location()};
            return {};
          }
          TRY(auto value, const_eval(expr, ctx));
          if (auto* integer = try_as<int64_t>(value);
              integer and is<Setter<located<uint64_t>>>(setter)) {
            if (*integer < 0) {
              diagnostic::error("expected positive integer, got `{}`", *integer)
                .primary(expr)
                .docs(desc_->docs)
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
                diagnostic::error(
                  "expected argument of type `{}`, but got `{}`",
                  type_kind::of<data_to_type_t<T>>, type_kind_of_data(value))
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
              // Pipelines are compiled in make(), not during substitute.
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
      if (desc_->variadic_index and idx >= *desc_->variadic_index) {
        pos_idx = *desc_->variadic_index;
      }
      TENZIR_ASSERT(pos_idx < desc_->positional.size());
      TRY(substitute_arg(arg, desc_->positional[pos_idx].setter, false));
    }
    // Substitute named arguments.
    for (auto& named_arg : named_args_) {
      TENZIR_ASSERT(named_arg.index < desc_->named.size());
      TRY(substitute_arg(named_arg.value, desc_->named[named_arg.index].setter,
                         true));
    }
    // Substitute the subpipeline if present.
    if (pipeline_) {
      TRY(pipeline_->pipeline.inner.substitute(ctx, false));
    }
    // Run custom validation if provided.
    if (desc_->validator) {
      auto error_tracker = error_tracking_handler{ctx};
      auto validate_ctx = DescribeCtx{args_,  named_args_,     pipeline_,
                                      *desc_, main_location(), error_tracker};
      (*desc_->validator)(validate_ctx);
      if (error_tracker.had_error()) {
        return failure::promise();
      }
    }
    for (auto& expr : filter_) {
      TRY(expr.substitute(ctx));
    }
    return {};
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    TENZIR_ASSERT(desc_->optimizer);
    order_ = weaker_event_order(order_, order);
    if (desc_->set_filter) {
      filter_.append_range(filter | std::views::as_rvalue);
      filter = ir::optimize_filter{};
    }
    auto noop_dh = null_diagnostic_handler{};
    auto ctx = DescribeCtx{args_,  named_args_,     pipeline_,
                           *desc_, main_location(), noop_dh};
    auto optimization = (*desc_->optimizer)(ctx, order_, std::move(filter));
    auto replacement = std::vector<Box<Operator>>{};
    if (not optimization.drop) {
      replacement.emplace_back(std::move(*this));
    }
    for (auto& expr : optimization.filter_self) {
      replacement.push_back(make_where_ir(expr));
    }
    return {std::move(optimization.filter_upstream), optimization.order,
            ir::pipeline{{}, std::move(replacement)}};
  }

  auto main_location() const -> location override {
    return op_.get_location();
  }

private:
  friend auto inspect(auto& f, GenericIr& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op_), f.field("desc", x.desc_), f.field("args", x.args_),
      f.field("filter", x.filter_), f.field("order", x.order_),
      f.field("named_args", x.named_args_), f.field("pipeline", x.pipeline_));
  }

  /// The entity that this operator was created for.
  ast::entity op_;

  /// Contains expression for positional arguments that are not yet evaluated.
  std::vector<Arg> args_;

  /// Contains named arguments with their indices.
  std::vector<NamedArg> named_args_;

  /// Pre-compiled pipeline with source location and let_ids.
  std::optional<PipelineArg> pipeline_;

  /// The filter passed to `optimize` (only if the operator wants to consume it).
  ir::optimize_filter filter_;

  /// The weakest ordering guarantee seen across all `optimize()` calls.
  /// Initialized to `ordered` (strongest); each call takes the max.
  event_order order_ = event_order::ordered;

  /// The object describing the available parameters.
  SharedDescription desc_;
};

auto OperatorPlugin::compile(ast::invocation inv, compile_ctx ctx) const
  -> failure_or<ir::CompileResult> {
  TRY(auto ir, GenericIr::make(SharedDescription{name(), describe_shared()},
                               std::move(inv.op), std::move(inv.args), ctx));
  return ir;
}

// TODO: Clean this up. We might want to be able to just use
// `TENZIR_REGISTER_PLUGINS` also from `libtenzir` itself.
auto register_plugins_somewhat_hackily = std::invoke([]() {
  auto ptr
    = plugin_ptr::make_builtin(new inspection_plugin<ir::Operator, GenericIr>{},
                               [](plugin* plugin) {
                                 delete plugin;
                               },
                               nullptr, {});
  const auto it = std::ranges::upper_bound(plugins::get_mutable(), ptr);
  plugins::get_mutable().insert(it, std::move(ptr));
  return std::monostate{};
});

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
      desc->docs = "https://docs.tenzir.com/reference/operators/" + desc->name;
    }
    cached_desc_ = std::move(desc);
  });
  return cached_desc_;
}

} // namespace tenzir::_::operator_plugin
