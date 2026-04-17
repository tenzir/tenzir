//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/any.hpp"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/async.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/type_list.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/let_id.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <mutex>
#include <span>
#include <string_view>

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
  Named(std::vector<std::string> names, std::string type, AnySetter setter,
        bool required)
    : names{std::move(names)},
      type{std::move(type)},
      setter{std::move(setter)},
      required{required} {
    TENZIR_ASSERT(not this->names.empty());
  }

  Named(std::string name, std::string type, AnySetter setter, bool required)
    : Named(std::vector{std::move(name)}, std::move(type), std::move(setter),
            required) {
  }

  std::vector<std::string> names;
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

template <class Args, class Input, class Output>
using SpawnFn = std::function<auto(Args)->Box<Operator<Input, Output>>>;

template <class Input, class Output>
using Spawn = SpawnFn<Any, Input, Output>;

// Variant for different operator spawn functions (matches AnyOperator).
using AnySpawn
  = variant<Spawn<void, void>, Spawn<void, chunk_ptr>, Spawn<void, table_slice>,
            Spawn<chunk_ptr, chunk_ptr>, Spawn<chunk_ptr, table_slice>,
            Spawn<table_slice, chunk_ptr>, Spawn<table_slice, table_slice>,
            Spawn<table_slice, void>, Spawn<chunk_ptr, void>>;

template <class Args, class Input>
using SpawnWith
  = variant<SpawnFn<Args, Input, void>, SpawnFn<Args, Input, chunk_ptr>,
            SpawnFn<Args, Input, table_slice>>;

// FIXME: Do we need this?
class Empty {
public:
  Empty() = default;

  explicit(false) Empty(std::nullopt_t) {
  }
};

class DescribeCtx;

using Spawner = std::function<
  auto(element_type_tag input, DescribeCtx& ctx)->failure_or<Option<AnySpawn>>>;

using Validator = std::function<auto(DescribeCtx&)->Empty>;

struct Description {
  std::string name;
  std::string docs;
  std::function<auto()->Any> make_args;
  std::vector<Positional> positional;
  std::optional<Pipeline> pipeline;
  std::optional<size_t> first_optional;
  std::optional<size_t> variadic_index;
  std::vector<Named> named;
  std::optional<Validator> validator;
  std::optional<Setter<ir::optimize_filter>> set_filter;
  std::optional<Setter<location>> set_operator_location;
  std::optional<Setter<event_order>> set_order;
  bool propagate_order = false;
  event_order initial_order = event_order::ordered;
  // FIXME: Document.
  std::optional<Spawner> spawner;
  std::vector<AnySpawn> spawns;
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

enum class ArgumentType { positional, named, pipeline };

template <class Args, ArgType T>
class Argument {
public:
  Argument() = default;

  Argument(ArgumentType type, size_t index) : type_{type}, index_{index} {
    if (type == ArgumentType::pipeline) {
      TENZIR_ASSERT(index == 0);
    }
  }

  auto type() const -> ArgumentType {
    return type_;
  }

  auto index() const -> size_t {
    return index_;
  }

  // TODO
  auto positive() -> Argument;
  auto non_negative() -> Argument;

private:
  ArgumentType type_ = ArgumentType::positional;
  size_t index_ = 0;
};

template <class Args, class T>
auto make_setter(T Args::* ptr) -> auto {
  using Value = decltype(std::invoke([] {
    if constexpr (detail::is_specialization_of<std::optional, T>::value
                  or detail::is_specialization_of<Option, T>::value) {
      return tag_v<typename T::value_type>;
    } else {
      return tag_v<T>;
    }
  }))::type;
  if constexpr (std::same_as<T, std::optional<location>>
                or std::same_as<T, Option<location>>) {
    return Setter<located<bool>>{[ptr](Any& args, located<bool> value) {
      if (value.inner) {
        (&args.as<Args>())->*ptr = value.source;
      } else {
        if constexpr (std::same_as<T, std::optional<location>>) {
          (&args.as<Args>())->*ptr = std::nullopt;
        } else {
          (&args.as<Args>())->*ptr = None{};
        }
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

template <class Args, class T>
auto make_variadic_setter(std::vector<T> Args::* ptr) -> auto {
  using Value = T;
  if constexpr (argument_parser_bare_type<Value>) {
    return Setter<located<Value>>{[ptr](Any& args, located<Value> value) {
      ((&args.as<Args>())->*ptr).push_back(std::move(value.inner));
    }};
  } else {
    return Setter<Value>{[ptr](Any& args, Value value) {
      ((&args.as<Args>())->*ptr).push_back(std::move(value));
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

class DescribeCtx {
public:
  DescribeCtx(std::span<const Arg> args, std::span<const NamedArg> named_args,
              std::optional<const PipelineArg> pipeline,
              const Description& desc, diagnostic_handler& dh)
    : args_{args},
      named_args_{named_args},
      pipeline_{std::move(pipeline)},
      desc_{&desc},
      dh_{&dh} {
  }

  /// Returns the parsed argument value or `std::nullopt` if the caller omitted
  /// the argument. This also applies to omitted `named_optional(...)`
  /// arguments, even when the destination member in `Args` has a default
  /// initializer. Validation callbacks must apply args-struct defaults
  /// explicitly when they need an effective value.
  template <class Args, class T>
  auto get(Argument<Args, T> arg) -> std::optional<T> {
    const Arg* value = nullptr;
    switch (arg.type()) {
      case ArgumentType::named: {
        for (const auto& named : named_args_) {
          if (named.index == arg.index()) {
            value = &named.value;
            break;
          }
        }
        if (not value) {
          return std::nullopt;
        }
        break;
      }
      case ArgumentType::positional: {
        if (arg.index() >= args_.size()) {
          return std::nullopt;
        }
        value = &args_[arg.index()];
        break;
      }
      case ArgumentType::pipeline: {
        if (not pipeline_) {
          return std::nullopt;
        }
        if constexpr (std::same_as<T, located<ir::pipeline>>) {
          return pipeline_->pipeline;
        } else {
          return std::nullopt;
        }
      }
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
      []<class U>(const U& v) -> std::optional<T> {
        if constexpr (std::same_as<T, U>) {
          return v;
        } else {
          return std::nullopt;
        }
      });
  }

  template <class Args, class T>
  auto get_all(Argument<Args, T> arg) -> std::vector<std::optional<T>> {
    auto result = std::vector<std::optional<T>>{};
    if (arg.type() != ArgumentType::positional or not desc_->variadic_index
        or arg.index() != *desc_->variadic_index) {
      result.push_back(get(arg));
      return result;
    }
    for (auto i = arg.index(); i < args_.size(); ++i) {
      result.push_back(get_impl<T>(args_[i]));
    }
    return result;
  }

  template <class Args, class T>
  auto get_location(Argument<Args, T> arg) -> std::optional<location> {
    const Arg* value = nullptr;
    switch (arg.type()) {
      case ArgumentType::named: {
        for (const auto& named : named_args_) {
          if (named.index == arg.index()) {
            value = &named.value;
            break;
          }
        }
        if (not value) {
          return std::nullopt;
        }
        break;
      }
      case ArgumentType::positional: {
        if (arg.index() >= args_.size()) {
          return std::nullopt;
        }
        value = &args_[arg.index()];
        break;
      }
      case ArgumentType::pipeline: {
        if (not pipeline_) {
          return std::nullopt;
        }
        return pipeline_->pipeline.source;
      }
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

  template <class Args, class T>
  auto get_locations(Argument<Args, T> arg) -> std::vector<location> {
    auto result = std::vector<location>{};
    if (arg.type() != ArgumentType::positional or not desc_->variadic_index
        or arg.index() != *desc_->variadic_index) {
      if (auto loc = get_location(arg)) {
        result.push_back(*loc);
      }
      return result;
    }
    for (auto i = arg.index(); i < args_.size(); ++i) {
      if (auto loc = get_location_impl(args_[i])) {
        result.push_back(*loc);
      }
    }
    return result;
  }

  explicit(false) operator diagnostic_handler&() {
    return *dh_;
  }

private:
  template <class T>
  static auto get_impl(const Arg& value) -> std::optional<T> {
    if (is<Incomplete>(value)) {
      return std::nullopt;
    }
    return match(
      value,
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
      []<class U>(const U& v) -> std::optional<T> {
        if constexpr (std::same_as<T, U>) {
          return v;
        } else {
          return std::nullopt;
        }
      });
  }

  static auto get_location_impl(const Arg& value) -> std::optional<location> {
    return match(
      value,
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

  std::span<const Arg> args_;
  std::span<const NamedArg> named_args_;
  std::optional<const PipelineArg> pipeline_;
  const Description* desc_;
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
    if (desc_.variadic_index) {
      panic("cannot add positional argument after variadic argument");
    }
    if (desc_.first_optional) {
      panic("cannot have required positional after optional positional");
    }
    auto index = desc_.positional.size();
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{ArgumentType::positional, index};
  }

  template <ArgType T>
  auto positional(std::string name, Option<T> Args::* ptr,
                  std::string type = type_default<T>) -> Argument<Args, T> {
    if (desc_.variadic_index) {
      panic("cannot add positional argument after variadic argument");
    }
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    auto index = desc_.positional.size();
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{ArgumentType::positional, index};
  }

  template <ArgType T>
  auto optional_positional(std::string name, T Args::* ptr,
                           std::string type = type_default<T>)
    -> Argument<Args, T> {
    if (desc_.variadic_index) {
      panic("cannot add positional argument after variadic argument");
    }
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    auto index = desc_.positional.size();
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{ArgumentType::positional, index};
  }

  auto pipeline(located<ir::pipeline> Args::* ptr)
    -> Argument<Args, located<ir::pipeline>> {
    TENZIR_ASSERT(not desc_.pipeline);
    desc_.pipeline = Pipeline{
      make_setter(ptr),
      {},
      true,
    };
    return Argument<Args, located<ir::pipeline>>{ArgumentType::pipeline, 0};
  }

  auto pipeline(Option<located<ir::pipeline>> Args::* ptr)
    -> Argument<Args, located<ir::pipeline>> {
    TENZIR_ASSERT(not desc_.pipeline);
    desc_.pipeline = Pipeline{
      make_setter(ptr),
      {},
      false,
    };
    return Argument<Args, located<ir::pipeline>>{ArgumentType::pipeline, 0};
  }

  /// Pipeline with let bindings that are injected into the subpipeline.
  /// Usage: `d.pipeline(&Args::pipe, {{"var_name", &Args::var_let_id}, ...})`
  auto pipeline(
    located<ir::pipeline> Args::* ptr,
    std::initializer_list<std::pair<std::string_view, let_id Args::*>> bindings)
    -> Argument<Args, located<ir::pipeline>> {
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
    return Argument<Args, located<ir::pipeline>>{ArgumentType::pipeline, 0};
  }

  auto pipeline(
    Option<located<ir::pipeline>> Args::* ptr,
    std::initializer_list<std::pair<std::string_view, let_id Args::*>> bindings)
    -> Argument<Args, located<ir::pipeline>> {
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
    return Argument<Args, located<ir::pipeline>>{ArgumentType::pipeline, 0};
  }

  /// Adds a variadic positional argument (requires at least one value).
  template <ArgType T>
  auto variadic(std::string name, std::vector<T> Args::* ptr,
                std::string type = type_default<T>) -> Argument<Args, T> {
    if (desc_.variadic_index) {
      panic("cannot have multiple variadic positional arguments");
    }
    auto index = desc_.positional.size();
    desc_.variadic_index = index;
    // Append "..." to the type string to indicate variadic
    auto variadic_type = type.empty() ? "..." : type + "...";
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(variadic_type),
      make_variadic_setter(ptr),
    });
    return Argument<Args, T>{ArgumentType::positional, index};
  }

  /// Adds an optional variadic positional argument (accepts zero or more values).
  template <ArgType T>
  auto optional_variadic(std::string name, std::vector<T> Args::* ptr,
                         std::string type = type_default<T>)
    -> Argument<Args, T> {
    if (desc_.variadic_index) {
      panic("cannot have multiple variadic positional arguments");
    }
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    auto index = desc_.positional.size();
    desc_.variadic_index = index;
    // Append "..." to the type string to indicate variadic
    auto variadic_type = type.empty() ? "..." : type + "...";
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(variadic_type),
      make_variadic_setter(ptr),
    });
    return Argument<Args, T>{ArgumentType::positional, index};
  }

  /// Adds a required named argument.
  template <ArgType T>
  auto
  named(std::string name, T Args::* ptr, std::string type = type_default<T>)
    -> Argument<Args, T> {
    return named(std::vector{std::move(name)}, ptr, std::move(type));
  }

  /// Adds a required named argument with multiple aliases.
  template <ArgType T>
  auto named(std::vector<std::string> names, T Args::* ptr,
             std::string type = type_default<T>) -> Argument<Args, T> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(names),
      std::move(type),
      make_setter(ptr),
      true,
    });
    return Argument<Args, T>{ArgumentType::named, index};
  }

  template <ArgType T, class Name>
    requires std::constructible_from<std::string_view, Name>
  auto named(std::initializer_list<Name> names, T Args::* ptr,
             std::string type = type_default<T>) -> Argument<Args, T> {
    auto names_vec = std::vector<std::string>{};
    names_vec.reserve(names.size());
    for (auto name : names) {
      names_vec.emplace_back(name);
    }
    return named(names_vec, ptr, std::move(type));
  }

  /// Adds an optional named argument.
  template <ArgType T>
  auto named(std::string name, Option<T> Args::* ptr,
             std::string type = type_default<T>) -> Argument<Args, T> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, T>{ArgumentType::named, index};
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
    return Argument<Args, T>{ArgumentType::named, index};
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
    return Argument<Args, bool>{ArgumentType::named, index};
  }

  /// Adds an optional location flag.
  auto named(std::string name, Option<location> Args::* ptr,
             std::string type = "") -> Argument<Args, bool> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, bool>{ArgumentType::named, index};
  }

  /// Low-level named arg registration with a custom setter.
  template <ArgType T>
  auto
  named_with_setter(std::string name, Setter<located<T>> setter,
                    bool required = false, std::string type = type_default<T>)
    -> Argument<Args, T> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      std::move(setter),
      required,
    });
    return Argument<Args, T>{ArgumentType::named, index};
  }

  /// Low-level boolean flag registration with a custom setter.
  auto named_flag_with_setter(std::string name, Setter<located<bool>> setter)
    -> Argument<Args, bool> {
    auto index = desc_.named.size();
    desc_.named.push_back(Named{
      std::move(name),
      std::string{},
      std::move(setter),
      false,
    });
    return Argument<Args, bool>{ArgumentType::named, index};
  }

  auto validate(Validator validator) -> void {
    desc_.validator = std::move(validator);
  }

  /// Customize the spawning logic of the operator.
  ///
  /// This is particularly useful when the output type of the operator depends
  /// on a subpipeline, but it can also be used to customize the spawning logic
  /// based on other parameters.
  ///
  /// The given spawner `s` needs to be templated-callable. The template
  /// parameter is the input type (void, bytes, or events). It can then return
  /// one of three things:
  /// - `failure` if the operator can not be spawned with that type and when a
  ///   custom diagnostic is to be emitted.
  /// - `None` if we don't know yet whether the operator can be be spawned for
  ///   that input type, OR if we don't want to customize the logic for a given
  ///   case. If this remains the case after instantiation, then a generic error
  ///   message will be produced.
  /// - `SpawnWith<Args, Input>`, where `Input` is the template parameter. This
  ///   is a variant of spawning functions for all possible output types. Return
  ///   this if the operator can be spawned for that input type.
  ///
  /// When returning `None`, then the normal spawning logic (that uses the
  /// template parameters of `Describer` to get the implementation types) will
  /// be used instead.
  ///
  /// Note that this function will also be used for type inference. There should
  /// this be no side-effects besides potentially emitting diagnostics.
  template <class Spawner>
    requires requires(Spawner& s, DescribeCtx& ctx) {
      {
        s.template operator()<void>(ctx)
      } -> std::same_as<failure_or<Option<SpawnWith<Args, void>>>>;
      {
        s.template operator()<chunk_ptr>(ctx)
      } -> std::same_as<failure_or<Option<SpawnWith<Args, chunk_ptr>>>>;
      {
        s.template operator()<table_slice>(ctx)
      } -> std::same_as<failure_or<Option<SpawnWith<Args, table_slice>>>>;
    }
  auto spawner(Spawner spawner) -> void {
    desc_.spawner = [spawner = std::move(spawner)](
                      element_type_tag input,
                      DescribeCtx& ctx) -> failure_or<Option<AnySpawn>> {
      return match(
        input, [&]<class Input>(tag<Input>) -> failure_or<Option<AnySpawn>> {
          auto result = spawner.template operator()<Input>(ctx);
          TRY(auto option, std::move(result));
          TRY(auto spawn, std::move(option));
          return match(
            spawn,
            [&]<class Output>(SpawnFn<Args, Input, Output>& spawn) -> AnySpawn {
              return [spawn = std::move(spawn)](
                       Any args) -> Box<Operator<Input, Output>> {
                return spawn(args.as<Args>());
              };
            });
        });
    };
  }

  // TODO
  template <class F>
  auto optimize(F&& f) -> Description;

  auto without_optimize() -> Description {
    return std::move(desc_);
  }

  auto optimize_filter(ir::optimize_filter Args::* ptr) -> Description {
    desc_.set_filter = make_setter(ptr);
    return std::move(desc_);
  }

  /// Registers a member of `Args` to be populated with the operators location.
  auto operator_location(location Args::* ptr) {
    TENZIR_ASSERT(not desc_.set_operator_location);
    desc_.set_operator_location = make_setter(ptr);
  }

  /// Registers a member of `Args` to be populated with the optimization
  /// order, i.e., the weakest ordering guarantee from downstream.
  auto optimization_order(event_order Args::* ptr) {
    TENZIR_ASSERT(not desc_.set_order);
    desc_.set_order = make_setter(ptr);
  }

  auto order_invariant() -> Description {
    desc_.propagate_order = true;
    return std::move(desc_);
  }

  auto unordered() -> Description {
    desc_.propagate_order = true;
    desc_.initial_order = event_order::unordered;
    return std::move(desc_);
  }

private:
  Description desc_;
};

} // namespace _::operator_plugin

using _::operator_plugin::Argument;
using _::operator_plugin::DescribeCtx;
using _::operator_plugin::Describer;
using _::operator_plugin::Description;
using _::operator_plugin::Empty;
using _::operator_plugin::OperatorPlugin;
using _::operator_plugin::Spawn;
using _::operator_plugin::SpawnWith;

} // namespace tenzir
