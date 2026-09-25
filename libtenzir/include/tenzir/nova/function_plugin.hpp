//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/any.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/nova/lambda.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"

#include <concepts>
#include <functional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::nova {

/// A constant argument of any type together with the location of its
/// expression. It stands in for `located<Data>`, which cannot be instantiated
/// because its defaulted comparison recurses through `Data`.
struct ConstantArgument {
  Data inner;
  location source;

  auto get_location() const -> location {
    return source;
  }
};

/// Constant argument types that a nova function can register. Expressions
/// arrive as an evaluated `ValueArgument` (or a borrowed `LazyArgument`),
/// lambdas as a `LambdaArgument`.
template <class T>
concept ArgType
  = (tenzir::_::operator_plugin::ArgType<T>
     and not concepts::one_of<T, ast::expression, ast::lambda_expr,
                              located<ir::pipeline>, data, located<data>>)
    or concepts::one_of<T, Data, ConstantArgument, Secret, located<Secret>>;

/// The kernel of a nova function: a default constructible type with
///
///     static auto eval(Args const& args, EvalFrame frame) -> Array<Data>;
///
/// that produces the rows of `frame.mask()`, with diagnostics going to
/// `frame`. Everything it needs is in `Args`: the framework evaluates the
/// constant arguments once at instantiation and the argument expressions
/// before every call. One immutable kernel is shared by every call site, so
/// implementations cannot retain call-, batch-, or mask-dependent state.
/// Derive precomputed data from constants in `validate`, which may store it in
/// `Args`.
template <class Impl, class Args>
concept FunctionImpl = std::default_initializable<Impl>
                       and requires(Args const& args, EvalFrame&& frame) {
                             {
                               Impl::eval(args, std::move(frame))
                             } -> std::same_as<Array<Data>>;
                           };

/// The registered arguments of a nova function together with the recipe for
/// turning them into a `CallSite`. Built with `FunctionDescriber`.
class FunctionDescription {
public:
  /// A prepared call site together with the argument expressions whose call
  /// sites the caller still has to prepare: those are borrowed from `call`
  /// rather than owned, and belong to the enclosing expression.
  struct Instantiation {
    _::CallSite call_site;
    std::vector<ast::expression*> deferred;
  };

  /// Validates `call` against the registered arguments, constant evaluates
  /// the constant ones with `nova::const_eval`, records the dynamic ones, and
  /// constructs the call site. Instantiation consumes the constant arguments
  /// of `call` but borrows every argument it evaluates later — values,
  /// lambdas, and lazy expressions — so `call` must outlive the returned
  /// instance. Emits diagnostics for every problem it finds and fails
  /// whenever an error was emitted. `name` is the user-facing function name,
  /// which only appears in diagnostics.
  auto instantiate(std::string_view name, ast::function_call& call,
                   InstantiateCtx ctx) const -> failure_or<Instantiation>;

  /// The usage string shown in diagnostics for a function called `name`,
  /// e.g. `split(x:string, pattern:string, [max=int, reverse=bool])`.
  auto usage(std::string_view name) const -> std::string;

private:
  template <class Args, FunctionImpl<Args> Impl>
  friend class FunctionDescriber;

  /// What one registered argument does with its expression: constants are
  /// evaluated into the bundle right away, everything else is recorded as a
  /// value slot or borrowed as a handle.
  struct PrepareSink {
    Any& args;
    std::vector<_::ValueSlot>& slots;
    std::vector<ast::expression*>& deferred;
  };
  using Prepare = std::function<
    auto(PrepareSink, ast::expression&, InstantiateCtx)->failure_or<void>>;
  using Validator
    = std::function<auto(Any&, diagnostic_handler&)->failure_or<void>>;

  struct Positional {
    std::string name;
    std::string type;
    Prepare prepare;
  };

  struct Named {
    std::vector<std::string> names;
    std::string type;
    bool required = false;
    Prepare prepare;
  };

  std::function<auto()->Any> make_args_;
  std::vector<Positional> positional_;
  Option<size_t> first_optional_;
  Option<size_t> variadic_index_;
  std::vector<Named> named_;
  Option<std::function<auto(Any&, location)->void>> set_call_location_;
  Option<Validator> validator_;
  _::CallSite::Kernel kernel_ = nullptr;
};

/// A nova function plugin only registers its arguments. The returned
/// description turns concrete calls into `CallSite`s.
class FunctionPlugin : public virtual function_plugin {
public:
  virtual auto describe() const -> FunctionDescription = 0;

  /// Instantiates `call` against `describe()`, under this plugin's function
  /// name. See `FunctionDescription::instantiate`.
  auto instantiate(ast::function_call& call, InstantiateCtx ctx) const
    -> failure_or<FunctionDescription::Instantiation>;
};

namespace _ {

/// The constant value a member of type `Member` is populated from: the
/// payload of `located<T>` and `Option<T>`, and `bool` for flags that only
/// record their location.
template <class Member>
struct ValueType : std::type_identity<Member> {};

template <class T>
struct ValueType<located<T>> : std::type_identity<T> {};

template <>
struct ValueType<ConstantArgument> : std::type_identity<Data> {};

template <class T>
struct ValueType<Option<T>> : ValueType<T> {};

template <>
struct ValueType<Option<location>> : std::type_identity<bool> {};

template <class Member>
using value_type_t = typename ValueType<Member>::type;

/// The usage type name for a constant argument when none is given.
auto default_type_name(type_kind kind) -> std::string;

template <class T>
auto default_type_name() -> std::string {
  if constexpr (std::same_as<T, Data>) {
    return "any";
  } else if constexpr (std::same_as<T, Secret>) {
    return "string|secret";
  } else if constexpr (std::same_as<T, ast::field_path>) {
    return "field";
  } else {
    return default_type_name(type_kind::of<data_to_type_t<T>>);
  }
}

/// Constant evaluates `expr` and converts the result to `T`.
template <class T>
auto prepare_constant(ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<located<T>> {
  TRY(auto constant, const_eval(expr, ctx));
  if constexpr (std::same_as<T, Data>) {
    return located<T>{std::move(constant), expr.get_location()};
  } else {
    if constexpr (std::same_as<T, std::uint64_t>) {
      if (auto const* signed_value = try_as<std::int64_t>(&constant)) {
        if (*signed_value < 0) {
          diagnostic::error("expected positive integer, got `{}`",
                            *signed_value)
            .primary(expr.get_location())
            .emit(ctx);
          return failure::promise();
        }
        return located<T>{static_cast<T>(*signed_value), expr.get_location()};
      }
    }
    if (auto const* value = try_as<T>(&constant)) {
      return located<T>{*value, expr.get_location()};
    }
    auto const actual = match(constant, []<class V>(V const&) {
      return Type<V>::static_name;
    });
    diagnostic::error("expected argument of type `{}`, but got `{}`",
                      default_type_name<T>(), actual)
      .primary(expr.get_location())
      .emit(ctx);
    return failure::promise();
  }
}

/// Evaluates a constant expression and returns its Nova Data value.
auto prepare_data(ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<Data>;

/// The payload type of `Option<T>`, or `T` itself.
template <class T>
struct OptionValue : std::type_identity<T> {};

template <class T>
struct OptionValue<Option<T>> : std::type_identity<T> {};

template <class T>
using option_value_t = typename OptionValue<T>::type;

/// Turns a constant into a `Data` or `ConstantArgument` member.
template <class T>
auto into_data_member(Data value, location source) -> T {
  if constexpr (std::same_as<T, ConstantArgument>) {
    return ConstantArgument{std::move(value), source};
  } else {
    static_assert(std::same_as<T, Data>);
    return value;
  }
}

/// Converts a resolved-secret AST node or a plain string constant to Secret.
auto prepare_secret(ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<located<Secret>>;
/// Turns a constant into the representation of a member of type `Member`.
template <class Member>
  requires(not std::same_as<value_type_t<Member>, Data>)
auto into_member(located<value_type_t<Member>> value) -> Member {
  if constexpr (std::same_as<Member, Option<location>>) {
    if (value.inner) {
      return value.source;
    }
    return None{};
  } else if constexpr (detail::is_specialization_of<Option, Member>::value) {
    return Member{into_member<typename Member::value_type>(std::move(value))};
  } else if constexpr (detail::is_specialization_of<located, Member>::value) {
    return std::move(value);
  } else {
    return std::move(value.inner);
  }
}

/// The `CallSite::Kernel` of `Impl`: runs `Impl::eval` over a type-erased
/// `Args` bundle.
template <class Args, FunctionImpl<Args> Impl>
auto function_kernel(Any const& args, EvalFrame frame) -> Array<Data> {
  return Impl::eval(args.as<Args>(), std::move(frame));
}

} // namespace _

/// Registers the arguments of a nova function. `Args` is the bundle the
/// arguments are stored in, `Impl` the `FunctionImpl<Args>` that consumes it.
///
/// Members of `Args` are typed after what they receive: `ValueArgument` for an
/// evaluated expression, `LambdaArgument` for a lambda, `LazyArgument` for an
/// expression the function evaluates itself, `ast::field_path` for selectors,
/// `located<T>` (or a bare `T`) for constants, `Option<...>` for optional
/// arguments, `bool` for flags, and `location` for `call_location`.
template <class Args, FunctionImpl<Args> Impl>
class FunctionDescriber {
public:
  FunctionDescriber() {
    desc_.make_args_ = []() -> Any {
      return Args{};
    };
    desc_.kernel_ = &_::function_kernel<Args, Impl>;
  }

  /// Registers a required unary lambda, applied through `EvalFrame::eval`.
  auto positional(std::string name, LambdaArgument Args::* ptr,
                  std::string type = "any -> any") -> void {
    add_positional(std::move(name), std::move(type), prepare_lambda(ptr),
                   false);
  }

  /// Registers a required expression, evaluated before every call.
  auto positional(std::string name, ValueArgument Args::* ptr,
                  std::string type = "any") -> void {
    add_positional(std::move(name), std::move(type), prepare_value(ptr), false);
  }

  /// Registers a required expression that the function evaluates itself.
  auto positional(std::string name, LazyArgument Args::* ptr,
                  std::string type = "any") -> void {
    add_positional(std::move(name), std::move(type), prepare_expr(ptr), false);
  }

  /// Registers an optional unary lambda. The member stays default
  /// constructed when the caller omits the argument, which `LambdaArgument`'s
  /// `operator bool` reports.
  auto optional_positional(std::string name, LambdaArgument Args::* ptr,
                           std::string type = "any -> any") -> void {
    add_positional(std::move(name), std::move(type), prepare_lambda(ptr), true);
  }

  /// Registers an optional expression. The member stays `None` when the
  /// caller omits the argument.
  auto optional_positional(std::string name, Option<ValueArgument> Args::* ptr,
                           std::string type = "any") -> void {
    add_positional(std::move(name), std::move(type), prepare_value(ptr), true);
  }

  /// Registers an optional expression that the function evaluates itself.
  auto optional_positional(std::string name, LazyArgument Args::* ptr,
                           std::string type = "any") -> void {
    add_positional(std::move(name), std::move(type), prepare_expr(ptr), true);
  }

  /// Registers a required named expression, evaluated before every call.
  auto named(std::string name, ValueArgument Args::* ptr,
             std::string type = "any") -> void {
    add_named({std::move(name)}, std::move(type), prepare_value(ptr), true);
  }

  /// Registers an optional named expression. The member stays `None` when the
  /// caller omits the argument.
  auto named_optional(std::string name, Option<ValueArgument> Args::* ptr,
                      std::string type = "any") -> void {
    add_named({std::move(name)}, std::move(type), prepare_value(ptr), false);
  }

  /// Registers a required constant argument.
  template <ArgType T>
  auto positional(std::string name, T Args::* ptr, std::string type = "")
    -> void {
    add_positional(std::move(name), type_or_default<T>(std::move(type)),
                   prepare_member(ptr), false);
  }

  /// Registers an optional constant argument.
  template <ArgType T>
  auto
  positional(std::string name, Option<T> Args::* ptr, std::string type = "")
    -> void {
    add_positional(std::move(name), type_or_default<T>(std::move(type)),
                   prepare_member(ptr), true);
  }

  /// Registers an optional constant argument whose member keeps its default
  /// when the caller omits it.
  template <ArgType T>
  auto
  optional_positional(std::string name, T Args::* ptr, std::string type = "")
    -> void {
    add_positional(std::move(name), type_or_default<T>(std::move(type)),
                   prepare_member(ptr), true);
  }

  /// Registers a variadic constant argument that requires at least one value.
  template <ArgType T>
  auto
  variadic(std::string name, std::vector<T> Args::* ptr, std::string type = "")
    -> void {
    add_variadic(std::move(name), type_or_default<T>(std::move(type)),
                 prepare_element(ptr), false);
  }

  /// Registers a variadic constant argument that accepts zero or more values.
  template <ArgType T>
  auto optional_variadic(std::string name, std::vector<T> Args::* ptr,
                         std::string type = "") -> void {
    add_variadic(std::move(name), type_or_default<T>(std::move(type)),
                 prepare_element(ptr), true);
  }

  /// Registers a required named constant argument. A bare `T` member makes
  /// the argument required; use `Option<T>` or `named_optional` for an
  /// optional one.
  template <ArgType T>
  auto named(std::string name, T Args::* ptr, std::string type = "") -> void {
    add_named({std::move(name)}, type_or_default<T>(std::move(type)),
              prepare_member(ptr), true);
  }

  /// Registers a required named constant argument with multiple aliases. A
  /// bare `T` member makes the argument required.
  template <ArgType T>
  auto
  named(std::vector<std::string> names, T Args::* ptr, std::string type = "")
    -> void {
    add_named(std::move(names), type_or_default<T>(std::move(type)),
              prepare_member(ptr), true);
  }

  /// Registers an optional named constant argument. An `Option<T>` member
  /// makes the argument optional and records whether it was provided.
  template <ArgType T>
  auto named(std::string name, Option<T> Args::* ptr, std::string type = "")
    -> void {
    add_named({std::move(name)}, type_or_default<T>(std::move(type)),
              prepare_member(ptr), false);
  }

  /// Registers an optional named constant argument whose member keeps its
  /// default when the caller omits it.
  template <ArgType T>
  auto named_optional(std::string name, T Args::* ptr, std::string type = "")
    -> void {
    add_named({std::move(name)}, type_or_default<T>(std::move(type)),
              prepare_member(ptr), false);
  }

  /// Registers an optional boolean flag.
  auto named(std::string name, bool Args::* ptr, std::string type = "")
    -> void {
    add_named({std::move(name)}, type_or_default<bool>(std::move(type)),
              prepare_member(ptr), false);
  }

  /// Registers an optional flag that records where it was set.
  auto named(std::string name, Option<location> Args::* ptr,
             std::string type = "") -> void {
    add_named({std::move(name)}, type_or_default<bool>(std::move(type)),
              prepare_member(ptr), false);
  }

  /// Registers a member of `Args` to be populated with the call's location.
  auto call_location(location Args::* ptr) -> void {
    TENZIR_ASSERT(not desc_.set_call_location_);
    desc_.set_call_location_ = [ptr](Any& args, location loc) {
      args.as<Args>().*ptr = loc;
    };
  }

  /// Registers a check that runs on the fully materialized `Args`. It may
  /// normalize the arguments in place. Emitting an error or returning a
  /// failure aborts the instantiation.
  template <class F>
    requires concepts::invokable_r<failure_or<void>, F&, Args&,
                                   diagnostic_handler&>
  auto validate(F f) -> void {
    TENZIR_ASSERT(not desc_.validator_);
    desc_.validator_
      = [check = std::move(f)](Any& args,
                               diagnostic_handler& dh) -> failure_or<void> {
      return check(args.as<Args>(), dh);
    };
  }

  auto finish() && -> FunctionDescription {
    return std::move(desc_);
  }

private:
  using Prepare = FunctionDescription::Prepare;
  using PrepareSink = FunctionDescription::PrepareSink;

  template <class T>
  static auto type_or_default(std::string type) -> std::string {
    if (type.empty()) {
      return _::default_type_name<_::value_type_t<T>>();
    }
    return type;
  }

  static auto prepare_lambda(LambdaArgument Args::* ptr) -> Prepare {
    return [ptr](PrepareSink sink, ast::expression& expr,
                 InstantiateCtx ctx) -> failure_or<void> {
      auto* lambda = try_as<ast::lambda_expr>(&expr);
      if (not lambda) {
        diagnostic::error("expected a lambda").primary(expr).emit(ctx);
        return failure::promise();
      }
      // Borrowed, not moved: the body belongs to the enclosing expression,
      // which is where its call sites are prepared.
      TRY(auto prepared, LambdaArgument::make(*lambda, ctx));
      sink.args.as<Args>().*ptr = std::move(prepared);
      sink.deferred.push_back(std::addressof(lambda->body));
      return {};
    };
  }

  /// Records a value slot for `expr`, which the framework evaluates before
  /// every call.
  template <class Member>
  static auto prepare_value(Member Args::* ptr) -> Prepare {
    return [ptr](PrepareSink sink, ast::expression& expr,
                 InstantiateCtx ctx) -> failure_or<void> {
      TRY(reject_lambda(expr, ctx));
      sink.slots.push_back({
        std::addressof(expr),
        [ptr](Any& args, ValueArgument value) {
          args.as<Args>().*ptr = Member{std::move(value)};
        },
      });
      sink.deferred.push_back(std::addressof(expr));
      return {};
    };
  }

  /// Borrows `expr` for the function to evaluate itself.
  static auto prepare_expr(LazyArgument Args::* ptr) -> Prepare {
    return [ptr](PrepareSink sink, ast::expression& expr,
                 InstantiateCtx ctx) -> failure_or<void> {
      TRY(reject_lambda(expr, ctx));
      sink.args.as<Args>().*ptr = LazyArgument{expr};
      sink.deferred.push_back(std::addressof(expr));
      return {};
    };
  }

  static auto reject_lambda(ast::expression& expr, InstantiateCtx ctx)
    -> failure_or<void> {
    if (is<ast::lambda_expr>(expr)) {
      diagnostic::error("expected an expression, got a lambda")
        .primary(expr)
        .emit(ctx);
      return failure::promise();
    }
    return {};
  }

  /// Stores a constant into a member of type `Member`, including `Option`s
  /// thereof.
  template <class Member>
  static auto prepare_member(Member Args::* ptr) -> Prepare {
    if constexpr (std::same_as<_::value_type_t<Member>, ast::field_path>) {
      return [ptr](PrepareSink sink, ast::expression& expr,
                   InstantiateCtx ctx) -> failure_or<void> {
        auto path = ast::field_path::try_from(expr);
        if (not path) {
          diagnostic::error("expected a selector").primary(expr).emit(ctx);
          return failure::promise();
        }
        sink.args.as<Args>().*ptr = Member{std::move(*path)};
        return {};
      };
    } else if constexpr (std::same_as<_::value_type_t<Member>, Data>) {
      return [ptr](PrepareSink sink, ast::expression& expr,
                   InstantiateCtx ctx) -> failure_or<void> {
        TRY(auto value, _::prepare_data(expr, ctx));
        sink.args.as<Args>().*ptr
          = Member{_::into_data_member<_::option_value_t<Member>>(
            std::move(value), expr.get_location())};
        return {};
      };
    } else if constexpr (std::same_as<_::value_type_t<Member>, Secret>) {
      return [ptr](PrepareSink sink, ast::expression& expr,
                   InstantiateCtx ctx) -> failure_or<void> {
        TRY(auto value, _::prepare_secret(expr, ctx));
        sink.args.as<Args>().*ptr = _::into_member<Member>(std::move(value));
        return {};
      };
    } else {
      return [ptr](PrepareSink sink, ast::expression& expr,
                   InstantiateCtx ctx) -> failure_or<void> {
        TRY(auto value,
            _::prepare_constant<_::value_type_t<Member>>(expr, ctx));
        sink.args.as<Args>().*ptr = _::into_member<Member>(std::move(value));
        return {};
      };
    }
  }

  /// Appends a constant to a variadic member.
  template <class T>
  static auto prepare_element(std::vector<T> Args::* ptr) -> Prepare {
    return [ptr](PrepareSink sink, ast::expression& expr,
                 InstantiateCtx ctx) -> failure_or<void> {
      if constexpr (std::same_as<_::value_type_t<T>, Data>) {
        TRY(auto value, _::prepare_data(expr, ctx));
        (sink.args.as<Args>().*ptr)
          .push_back(
            _::into_data_member<T>(std::move(value), expr.get_location()));
        return {};
      } else if constexpr (std::same_as<_::value_type_t<T>, Secret>) {
        TRY(auto value, _::prepare_secret(expr, ctx));
        (sink.args.as<Args>().*ptr)
          .push_back(_::into_member<T>(std::move(value)));
        return {};
      } else {
        TRY(auto value, _::prepare_constant<_::value_type_t<T>>(expr, ctx));
        (sink.args.as<Args>().*ptr)
          .push_back(_::into_member<T>(std::move(value)));
        return {};
      }
    };
  }

  auto add_positional(std::string name, std::string type, Prepare prepare,
                      bool optional) -> void {
    if (desc_.variadic_index_) {
      panic("cannot add positional argument after variadic argument");
    }
    if (optional) {
      if (not desc_.first_optional_) {
        desc_.first_optional_ = desc_.positional_.size();
      }
    } else if (desc_.first_optional_) {
      panic("cannot have required positional after optional positional");
    }
    desc_.positional_.push_back(
      {std::move(name), std::move(type), std::move(prepare)});
  }

  auto add_variadic(std::string name, std::string type, Prepare prepare,
                    bool optional) -> void {
    if (desc_.variadic_index_) {
      panic("cannot have multiple variadic positional arguments");
    }
    if (optional) {
      if (not desc_.first_optional_) {
        desc_.first_optional_ = desc_.positional_.size();
      }
    } else if (desc_.first_optional_) {
      panic("cannot have required variadic after optional positional");
    }
    desc_.variadic_index_ = desc_.positional_.size();
    desc_.positional_.push_back(
      {std::move(name), type + "...", std::move(prepare)});
  }

  auto add_named(std::vector<std::string> names, std::string type,
                 Prepare prepare, bool required) -> void {
    TENZIR_ASSERT(not names.empty());
    desc_.named_.push_back(
      {std::move(names), std::move(type), required, std::move(prepare)});
  }

  FunctionDescription desc_;
};

} // namespace tenzir::nova
