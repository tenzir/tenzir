//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/operator_plugin.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/substitute_ctx.hpp"

namespace tenzir::_::operator_plugin {

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

auto get_usage(const Description& desc) -> std::string {
  // TODO
  auto result = desc.name;
  auto in_brackets = false;
  for (auto& positional : desc.positional) {
    result += ' ';
    if (desc.first_optional
        and *desc.first_optional == &positional - desc.positional.data()) {
      result += '[';
      in_brackets = true;
    }
    result += positional.name;
    result += ':';
    result += match(positional.setter, []<class T>(const Setter<T>&) {
      return typeid(T).name();
    });
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
    auto min_positional = desc->first_optional.value_or(0);
    auto max_positional = desc->positional.size();
    if (args.size() < min_positional) {
      auto specifier
        = min_positional == max_positional ? "exactly" : "at least";
      diagnostic::error("expected {} {} positional argument{}", specifier,
                        min_positional, min_positional == 1 ? "" : "s")
        .primary(op)
        .docs(desc->docs)
        .emit(ctx);
      return failure::promise();
    }
    if (args.size() > max_positional) {
       auto specifier
        = min_positional == max_positional ? "exactly" : "at most";
      auto first_extra = args.begin() + max_positional;
      diagnostic::error("expected {} {} positional argument{}", specifier,
                        max_positional, max_positional == 1 ? "" : "s")
        .primary(*first_extra)
        .usage(get_usage(*desc))
        .docs(desc->docs)
        .emit(ctx);
      return failure::promise();
    }
    auto result = GenericIr{};
    result.op_ = std::move(op);
    result.desc_ = std::move(desc);
    result.args_.reserve(args.size());
    for (auto& arg : args) {
      result.args_.push_back(Incomplete{std::move(arg)});
    }
    return result;
  }

  auto name() const -> std::string override {
    return "GenericIr";
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
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
    auto args = desc_->args;
    for (auto [idx, arg] : detail::enumerate(args_)) {
      match(
        std::move(arg),
        [&]<class T>(T x) {
          TENZIR_ASSERT(idx < desc_->positional.size());
          as<Setter<T>>(desc_->positional[idx].setter)(args, std::move(x));
        },
        [](Incomplete) {
          TENZIR_UNREACHABLE();
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
    for (auto [idx, arg] : detail::enumerate(args_)) {
      auto incomplete = try_as<Incomplete>(arg);
      if (not incomplete) {
        continue;
      }
      auto& expr = incomplete->expr;
      TRY(auto subst, expr.substitute(ctx));
      auto remaining = subst == ast::substitute_result::some_remaining;
      if (remaining) {
        continue;
      }
      // TODO
      TENZIR_ASSERT(idx < desc_->positional.size());
      auto& desc = desc_->positional[idx];
      auto is_constant = not is<Setter<ast::expression>>(desc.setter);
      if (is_constant) {
        if (instantiate or expr.is_deterministic(ctx)) {
          TRY(auto value, const_eval(expr, ctx));
          if (auto integer = try_as<int64_t>(value);
              integer
              and is<Setter<located<uint64_t>>>(
                desc_->positional[idx].setter)) {
            if (*integer < 0) {
              diagnostic::error("expected positive integer, got `{}`", *integer)
                .primary(expr)
                .docs(desc_->docs)
                .emit(ctx);
              return failure::promise();
            }
            value = detail::narrow<uint64_t>(*integer);
          }
          auto result = match(
            desc_->positional[idx].setter,
            [&]<class T>(const Setter<located<T>>&) -> failure_or<Arg> {
              auto cast = try_as<T>(value);
              if (not cast) {
                diagnostic::error("expected {} but got {}", "TODO",
                                  match(value,
                                        []<class U>(const U& x) {
                                          // TODO: Proper type.
                                          return detail::pretty_type_name(x);
                                          // return
                                          // type_kind::of<data_to_type_t<U>>;
                                        }))
                  .primary(expr)
                  .emit(ctx);
                return failure::promise();
              }
              return located{std::move(*cast), expr.get_location()};
            },
            [&](const Setter<located<data>>&) -> failure_or<Arg> {
              TENZIR_TODO();
            },
            [&](const Setter<located<tenzir::pipeline>>&) -> failure_or<Arg> {
              TENZIR_TODO();
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
        TENZIR_TODO();
        arg = std::move(expr);
      }
    }
    return {};
  }

  auto main_location() const -> location override {
    return op_.get_location();
  }

private:
  /// Argument that is not yet fully parsed.
  struct Incomplete {
    Incomplete() = default;

    explicit Incomplete(ast::expression expr) : expr{std::move(expr)} {
    }

    friend auto inspect(auto& f, Incomplete& x) -> bool {
      return f.apply(x.expr);
    }

    ast::expression expr;
  };

  using WithIncomplete
    = detail::tl_concat_t<LocatedTypes, detail::type_list<Incomplete>>;

  using Arg = detail::tl_apply_t<WithIncomplete, variant>;

  friend auto inspect(auto& f, GenericIr& x) -> bool {
    return f.object(x).fields(f.field("op", x.op_), f.field("desc", x.desc_),
                              f.field("args", x.args_));
  }

  /// The entity that this operator was created for.
  ast::entity op_;

  /// Contains expression for arguments that are not yet evaluated.
  std::vector<Arg> args_;

  /// The object describing the available parameters.
  SharedDescription desc_;
};

auto OperatorPlugin::compile(ast::invocation inv, compile_ctx ctx) const
  -> failure_or<Box<ir::Operator>> {
  return GenericIr::make(SharedDescription{name(), describe_shared()},
                         std::move(inv.op), std::move(inv.args), ctx);
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
  static const auto desc = std::invoke([&] {
    auto desc = std::make_shared<Description>(describe());
    if (desc->name.empty()) {
      desc->name = name();
    }
    if (desc->docs.empty()) {
      desc->docs = "https://docs.tenzir.com/reference/operators/" + desc->name;
    }
    return desc;
  });
  return desc;
}

} // namespace tenzir::_::operator_plugin
