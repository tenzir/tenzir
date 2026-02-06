//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/any.hpp"
#include "tenzir/async.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/type_list.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/let_id.hpp"

#include <mutex>
#include <span>

namespace tenzir {

namespace _::operator_plugin {

using LocatedTypes = detail::tl_concat_t<
  detail::tl_map_t<detail::tl_filter_not_type_t<data::types, pattern>,
                   as_located>,
  caf::type_list<located<ir::pipeline>, ast::expression, ast::field_path,
                 ast::lambda_expr, located<data>>>;

template <class T>
concept LocatedType = detail::tl_contains_v<LocatedTypes, T>;

using BareTypes
  = detail::tl_map_t<detail::tl_filter_t<argument_parser_full_types, is_located>,
                     detail::value_type_of>;

using ArgTypes = detail::tl_concat_t<LocatedTypes, BareTypes>;

template <class T>
concept ArgType = detail::tl_contains_v<ArgTypes, T>;

template <class T>
using Setter = std::function<void(Any&, T)>;

template <class T>
struct AsSetter : std::type_identity<Setter<T>> {};

using Setters = detail::tl_map_t<LocatedTypes, AsSetter>;

using AnySetter = detail::tl_apply_t<Setters, variant>;

struct Positional {
  Positional(std::string name, std::string type, AnySetter setter)
    : name{std::move(name)}, type{std::move(type)}, setter{std::move(setter)} {
  }

  std::string name;
  std::string type;
  AnySetter setter;
};

struct Named {
  Named(std::string name, std::string type, AnySetter setter, bool required)
    : name{std::move(name)},
      type{std::move(type)},
      setter{std::move(setter)},
      required{required} {
  }

  std::string name;
  std::string type;
  AnySetter setter;
  bool required = false;
};

/// Specification for a let binding to inject into a subpipeline.
struct LetBinding {
  std::string name;
  Setter<let_id> setter;
};

struct Pipeline {
  Pipeline(Setter<located<ir::pipeline>> setter,
           std::vector<LetBinding> let_bindings, bool required)
    : setter{std::move(setter)},
      let_bindings{std::move(let_bindings)},
      required{required} {
  }

  Setter<located<ir::pipeline>> setter;
  std::vector<LetBinding> let_bindings;
  bool required = false;
};

template <class Input, class Output>
using Spawn = std::function<auto(Any)->Box<Operator<Input, Output>>>;

// Variant for different operator spawn functions (matches AnyOperator).
using AnySpawn
  = variant<Spawn<void, chunk_ptr>, Spawn<void, table_slice>,
            Spawn<chunk_ptr, chunk_ptr>, Spawn<chunk_ptr, table_slice>,
            Spawn<table_slice, chunk_ptr>, Spawn<table_slice, table_slice>,
            Spawn<table_slice, void>, Spawn<chunk_ptr, void>>;

// FIXME: Do we need this?
class Empty {
public:
  Empty() = default;

  explicit(false) Empty(std::nullopt_t) {
  }
};

class ValidateCtx;

using Validator = std::function<Empty(ValidateCtx&)>;

struct Description {
public:
  Description() = default;

  std::string name;
  std::string docs;
  std::function<Any()> make_args;
  std::vector<Positional> positional;
  std::optional<Pipeline> pipeline;
  std::optional<size_t> first_optional;
  std::vector<Named> named;
  std::vector<AnySpawn> spawns;
  std::optional<Validator> validator;
  std::optional<Setter<ir::optimize_filter>> set_filter;
};

class OperatorPlugin : public virtual operator_compiler_plugin {
public:
  virtual auto describe() const -> Description = 0;

  auto describe_shared() const -> std::shared_ptr<const Description>;

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> final;

private:
  mutable std::once_flag desc_init_flag_;
  mutable std::shared_ptr<const Description> cached_desc_;
};

template <class Args, ArgType T>
class Argument {
public:
  Argument() = default;

  Argument(bool is_named, size_t index) : is_named_{is_named}, index_{index} {
  }

  auto is_named() const -> bool {
    return is_named_;
  }

  auto index() const -> size_t {
    return index_;
  }

  // TODO
  auto positive() -> Argument;
  auto non_negative() -> Argument;

private:
  bool is_named_ = false;
  size_t index_ = 0;
};

template <class Args, class T>
auto make_setter(T Args::* ptr) -> auto {
  using Value = decltype(std::invoke([] {
    if constexpr (detail::is_specialization_of<std::optional, T>::value) {
      return tag_v<typename T::value_type>;
    } else {
      return tag_v<T>;
    }
  }))::type;
  if constexpr (std::same_as<T, std::optional<location>>) {
    return Setter<located<bool>>{[ptr](Any& args, located<bool> value) {
      if (value.inner) {
        (&args.as<Args>())->*ptr = value.source;
      } else {
        (&args.as<Args>())->*ptr = std::nullopt;
      }
    }};
  } else if constexpr (std::same_as<T, bool>) {
    return Setter<located<bool>>{[ptr](Any& args, located<bool> value) {
      (&args.as<Args>())->*ptr = value.inner;
    }};
  } else if constexpr (argument_parser_bare_type<Value>) {
    return Setter<located<Value>>{[ptr](Any& args, located<Value> value) {
      (&args.as<Args>())->*ptr = std::move(value.inner);
    }};
  } else if constexpr (std::same_as<Value, ir::pipeline>) {
    // Pipeline arguments come as located<ir::pipeline>, extract the inner value.
    return Setter<located<ir::pipeline>>{
      [ptr](Any& args, located<ir::pipeline> value) {
        (&args.as<Args>())->*ptr = std::move(value.inner);
      }};
  } else {
    return Setter<Value>{[ptr](Any& args, Value value) {
      (&args.as<Args>())->*ptr = std::move(value);
    }};
  }
}

/// Represents an argument that is not yet fully evaluated.
struct Incomplete {
  Incomplete() = default;

  explicit Incomplete(ast::expression expr) : expr{std::move(expr)} {
  }

  ast::expression expr;

  friend auto inspect(auto& f, Incomplete& x) -> bool {
    return f.apply(x.expr);
  }
};

using ArgWithIncomplete
  = detail::tl_concat_t<LocatedTypes, detail::type_list<Incomplete>>;

using Arg = detail::tl_apply_t<ArgWithIncomplete, variant>;

/// Named argument with its index in the description and its value.
struct NamedArg {
  NamedArg() = default;

  NamedArg(size_t index, Arg value) : index{index}, value{std::move(value)} {
  }

  size_t index = 0;
  Arg value;

  friend auto inspect(auto& f, NamedArg& x) -> bool {
    return f.object(x).fields(f.field("index", x.index),
                              f.field("value", x.value));
  }
};

struct PipelineArg {
  located<ir::pipeline> pipeline;
  // Index by the id position in the description, so we can pass it to the
  // correct setter.
  std::map<size_t, let_id> let_ids;

  friend auto inspect(auto& f, PipelineArg& x) -> bool {
    return f.object(x).fields(f.field("pipeline", x.pipeline),
                              f.field("let_ids", x.let_ids));
  }
};

class ValidateCtx {
public:
  ValidateCtx(std::span<const Arg> args, std::span<const NamedArg> named_args,
              const Description& /*desc*/, diagnostic_handler& dh)
    : args_{args}, named_args_{named_args}, dh_{&dh} {
  }

  template <class Args, class T>
  auto get(Argument<Args, T> arg) -> std::optional<T> {
    const Arg* value = nullptr;
    if (arg.is_named()) {
      // Find the named argument with matching index
      for (const auto& named : named_args_) {
        if (named.index == arg.index()) {
          value = &named.value;
          break;
        }
      }
      if (not value) {
        // Named argument was not provided
        return std::nullopt;
      }
    } else {
      // Positional argument
      if (arg.index() >= args_.size()) {
        // Positional argument was not provided
        return std::nullopt;
      }
      value = &args_[arg.index()];
    }
    // Check if still incomplete
    if (is<Incomplete>(*value)) {
      return std::nullopt;
    }
    // Extract the value
    return match(
      *value,
      [](const Incomplete&) -> std::optional<T> {
        TENZIR_UNREACHABLE();
      },
      []<class U>(const located<U>& v) -> std::optional<T> {
        if constexpr (std::same_as<T, U>) {
          return v.inner;
        } else if constexpr (std::same_as<T, located<U>>) {
          return v;
        } else {
          return std::nullopt;
        }
      },
      [](const auto&) -> std::optional<T> {
        return std::nullopt;
      });
  }

  template <class Args, class T>
  auto get_location(Argument<Args, T> arg) -> std::optional<location> {
    const Arg* value = nullptr;
    if (arg.is_named()) {
      for (const auto& named : named_args_) {
        if (named.index == arg.index()) {
          value = &named.value;
          break;
        }
      }
      if (not value) {
        return std::nullopt;
      }
    } else {
      if (arg.index() >= args_.size()) {
        return std::nullopt;
      }
      value = &args_[arg.index()];
    }
    return match(
      *value,
      [](const Incomplete& v) -> std::optional<location> {
        return v.expr.get_location();
      },
      []<class U>(const located<U>& v) -> std::optional<location> {
        return v.source;
      },
      [](const auto& v) -> std::optional<location> {
        return v.get_location();
      });
  }

  explicit(false) operator diagnostic_handler&() {
    return *dh_;
  }

private:
  std::span<const Arg> args_;
  std::span<const NamedArg> named_args_;
  diagnostic_handler* dh_;
};

/// For some types, we do not want to implicitly default to a generic string.
/// If your code fails to compile because of this constraint, add a third
/// parameter which describes the argument "type".
template <class T>
  requires(not concepts::one_of<T, ast::expression, list, located<list>>)
static constexpr char const* type_default = "";

template <class Args, class... Impls>
class Describer {
public:
  explicit Describer(std::string docs = "") {
    desc_.docs = std::move(docs);
    desc_.make_args = []() -> Any {
      return Args{};
    };
    (impl<Impls>(), ...);
  }

  explicit Describer(Args initial, std::string docs = "") {
    desc_.docs = std::move(docs);
    desc_.make_args = [initial = std::move(initial)]() -> Any {
      return initial;
    };
    (impl<Impls>(), ...);
  }

  /// Add an operator implementation.
  template <class Impl>
  auto impl() -> void {
    std::invoke(
      [&]<class Input, class Output>(Operator<Input, Output>*) {
        desc_.spawns.push_back(
          Spawn<Input, Output>{[](Any args) -> Box<Operator<Input, Output>> {
            return Impl{std::move(args).as<Args>()};
          }});
      },
      static_cast<Impl*>(nullptr));
  }

  auto docs(std::string url) -> void {
    desc_.docs = std::move(url);
  }

  // TODO: Implement callable-based positional for custom type transformations.
  // template <class F>
  // auto positional(std::string name, F&& f);

  template <ArgType T>
  auto positional(std::string name, T Args::* ptr,
                  std::string type = type_default<T>) -> Argument<Args, T> {
    // TODO: Check exact type?
    if (desc_.first_optional) {
      panic("cannot have required positional after optional positional");
    }
    auto index = desc_.positional.size();
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{false, index};
  }

  template <ArgType T>
  auto positional(std::string name, std::optional<T> Args::* ptr,
                  std::string type = type_default<T>) -> Argument<Args, T> {
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    auto index = desc_.positional.size();
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{false, index};
  }

  template <ArgType T>
  auto optional_positional(std::string name, T Args::* ptr,
                           std::string type = type_default<T>)
    -> Argument<Args, T> {
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    auto index = desc_.positional.size();
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{false, index};
  }

  auto pipeline(located<ir::pipeline> Args::* ptr) -> void {
    TENZIR_ASSERT(not desc_.pipeline);
    desc_.pipeline = Pipeline{
      make_setter(ptr),
      {},
      true,
    };
  }

  auto pipeline(std::optional<located<ir::pipeline>> Args::* ptr) -> void {
    TENZIR_ASSERT(not desc_.pipeline);
    desc_.pipeline = Pipeline{
      make_setter(ptr),
      {},
      false,
    };
  }

  /// Pipeline with let bindings that are injected into the subpipeline.
  /// Usage: `d.pipeline(&Args::pipe, {{"var_name", &Args::var_let_id}, ...})`
  auto pipeline(
    located<ir::pipeline> Args::* ptr,
    std::initializer_list<std::pair<std::string_view, let_id Args::*>> bindings)
    -> void {
    TENZIR_ASSERT(not desc_.pipeline);
    auto let_bindings = std::vector<LetBinding>{};
    for (const auto& [name, member_ptr] : bindings) {
      let_bindings.push_back(
        {std::string{name}, [member_ptr](Any& args, let_id id) {
           (&args.as<Args>())->*member_ptr = id;
         }});
    }
    desc_.pipeline = Pipeline{
      make_setter(ptr),
      std::move(let_bindings),
      true,
    };
  }

  auto pipeline(
    std::optional<located<ir::pipeline>> Args::* ptr,
    std::initializer_list<std::pair<std::string_view, let_id Args::*>> bindings)
    -> void {
    TENZIR_ASSERT(not desc_.pipeline);
    auto let_bindings = std::vector<LetBinding>{};
    for (const auto& [name, member_ptr] : bindings) {
      let_bindings.push_back(
        {std::string{name}, [member_ptr](Any& args, let_id id) {
           (&args.as<Args>())->*member_ptr = id;
         }});
    }
    desc_.pipeline = Pipeline{
      make_setter(ptr),
      std::move(let_bindings),
      false,
    };
  }

  // TODO
  // template<typename T>
  // auto variadic(std::string,
  //               vector<T> Args::* ptr,
  //               std::string type = type_default<T>) {
  //
  // }

  /// Adds a required named argument.
  template <ArgType T>
  auto
  named(std::string name, T Args::* ptr, std::string type = type_default<T>)
    -> Argument<Args, T> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      true,
    });
    return Argument<Args, T>{true, index};
  }

  /// Adds an optional named argument.
  template <ArgType T>
  auto named(std::string name, std::optional<T> Args::* ptr,
             std::string type = type_default<T>) -> Argument<Args, T> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, T>{true, index};
  }

  /// Adds an optional named argument with a default value.
  template <ArgType T>
  auto named_optional(std::string name, T Args::* ptr,
                      std::string type = type_default<T>) -> Argument<Args, T> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, T>{true, index};
  }

  /// Adds an optional boolean flag.
  auto named(std::string name, bool Args::* ptr, std::string type = "")
    -> Argument<Args, bool> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, bool>{true, index};
  }

  /// Adds an optional location flag.
  auto named(std::string name, std::optional<location> Args::* ptr,
             std::string type = "") -> Argument<Args, bool> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, bool>{true, index};
  }

  auto validate(Validator validator) -> void {
    desc_.validator = std::move(validator);
  }

  // TODO
  template <class F>
  auto optimize(F&& f) -> Description;

  auto without_optimize() -> Description {
    // TODO
    return std::move(desc_);
  }

  auto optimize_filter(ir::optimize_filter Args::* ptr) -> Description {
    desc_.set_filter = make_setter(ptr);
    return std::move(desc_);
  }

  auto order_invariant() -> Description {
    // TODO
    return std::move(desc_);
  }

private:
  Description desc_;
};

} // namespace _::operator_plugin

using _::operator_plugin::Describer;
using _::operator_plugin::Description;
using _::operator_plugin::Empty;
using _::operator_plugin::OperatorPlugin;
using _::operator_plugin::ValidateCtx;

} // namespace tenzir
