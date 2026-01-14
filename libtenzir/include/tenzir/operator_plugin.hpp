//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/type_list.hpp"
#include "tenzir/ir.hpp"

#include <any>

namespace tenzir {

namespace _::operator_plugin {

using LocatedTypes = detail::tl_concat_t<
  detail::tl_map_t<detail::tl_filter_not_type_t<data::types, pattern>,
                   as_located>,
  caf::type_list<located<pipeline>, ast::expression, ast::field_path,
                 ast::lambda_expr, located<data>>>;

template <class T>
concept LocatedType = detail::tl_contains_v<LocatedTypes, T>;

using BareTypes
  = detail::tl_map_t<detail::tl_filter_t<argument_parser_full_types, is_located>,
                     caf::detail::value_type_of>;

using ArgTypes = detail::tl_concat_t<LocatedTypes, BareTypes>;

template <class T>
concept ArgType = detail::tl_contains_v<ArgTypes, T>;

template <LocatedType T>
using Setter = std::function<void(std::any&, T)>;

template <LocatedType T>
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

template <class Input, class Output>
using Spawn = std::function<auto(std::any)->Box<Operator<Input, Output>>>;

// TODO: Complete.
using AnySpawn = variant<Spawn<table_slice, table_slice>>;

struct Description {
public:
  Description() = default;

  std::string name;
  std::string docs;
  std::any args;
  std::vector<Positional> positional;
  std::optional<size_t> first_optional;
  std::vector<Named> named;
  std::vector<AnySpawn> spawns;
};

class OperatorPlugin : public virtual operator_compiler_plugin {
public:
  virtual auto describe() const -> Description = 0;

  auto describe_shared() const -> std::shared_ptr<const Description>;

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> final;
};

// FIXME: Do we need this?
class Empty {
public:
  Empty() = default;

  explicit(false) Empty(std::nullopt_t) {
  }
};

template <class Args, ArgType T>
class Argument {
public:
  auto value() const -> std::optional<T>;

  auto get_location() const -> location;

  // TODO
  auto positive() -> Argument;
  auto non_negative() -> Argument;
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
    return Setter<located<bool>>{[ptr](std::any& args, located<bool> value) {
      if (value.inner) {
        (&std::any_cast<Args&>(args))->*ptr = value.source;
      } else {
        (&std::any_cast<Args&>(args))->*ptr = std::nullopt;
      }
    }};
  } else if constexpr (std::same_as<T, bool>) {
    return Setter<located<bool>>{[ptr](std::any& args, located<bool> value) {
      (&std::any_cast<Args&>(args))->*ptr = value.inner;
    }};
  } else if constexpr (argument_parser_bare_type<Value>) {
    return Setter<located<Value>>{[ptr](std::any& args, located<Value> value) {
      (&std::any_cast<Args&>(args))->*ptr = std::move(value.inner);
    }};
  } else {
    return Setter<Value>{[ptr](std::any& args, Value value) {
      (&std::any_cast<Args&>(args))->*ptr = std::move(value);
    }};
  }
}

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
    desc_.args = Args{};
    (impl<Impls>(), ...);
  }

  /// Add an operator implementation.
  template <class Impl>
  auto impl() -> void {
    // TODO
    desc_.spawns.push_back(
      [](std::any args) -> Box<Operator<table_slice, table_slice>> {
        return Impl{std::any_cast<Args>(std::move(args))};
      });
  }

  auto docs(std::string url) -> void {
    desc_.docs = std::move(url);
  }

  template <ArgType T>
  auto positional(std::string name, T Args::* ptr,
                  std::string type = type_default<T>) -> Argument<Args, T> {
    // TODO: Check exact type?
    if (desc_.first_optional) {
      panic("cannot have required positional after optional positional");
    }
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{};
  }

  template <ArgType T>
  auto positional(std::string name, std::optional<T> Args::* ptr,
                  std::string type = type_default<T>) -> Argument<Args, T> {
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{};
  }

  template <ArgType T>
  auto optional_positional(std::string name, T Args::* ptr,
                           std::string type = type_default<T>)
    -> Argument<Args, T> {
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    desc_.positional.push_back(Positional{
      std::move(name),
      std::move(type),
      make_setter(ptr),
    });
    return Argument<Args, T>{};
  }

  /// Adds a required named argument.
  template <ArgType T>
  auto
  named(std::string name, T Args::* ptr, std::string type = type_default<T>)
    -> Argument<Args, T> {
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      true,
    });
    return Argument<Args, T>{};
  }

  /// Adds an optional named argument.
  template <ArgType T>
  auto named(std::string name, std::optional<T> Args::* ptr,
             std::string type = type_default<T>) -> Argument<Args, T> {
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, T>{};
  }

  /// Adds an optional named argument with a default value.
  template <ArgType T>
  auto named_optional(std::string name, T Args::* ptr,
                      std::string type = type_default<T>) -> Argument<Args, T> {
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, T>{};
  }

  /// Adds an optional boolean flag.
  auto named(std::string name, bool Args::* ptr, std::string type = "")
    -> Argument<Args, bool> {
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
    return Argument<Args, bool>{};
  }

  /// Adds an optional location flag.
  auto named(std::string name, std::optional<location> Args::* ptr,
             std::string type = "") -> void {
    desc_.named.push_back(Named{
      std::move(name),
      std::move(type),
      make_setter(ptr),
      false,
    });
  }

  auto validate(std::function<auto(diagnostic_handler&)->Empty>) -> void;

  // TODO
  template <class F>
  auto optimize(F&& f) -> Description;

  auto without_optimize() -> Description {
    // TODO
    return std::move(desc_);
  }

  auto order_invariant() -> Description;

private:
  Description desc_;
};

} // namespace _::operator_plugin

using _::operator_plugin::Describer;
using _::operator_plugin::Description;
using _::operator_plugin::OperatorPlugin;

} // namespace tenzir
