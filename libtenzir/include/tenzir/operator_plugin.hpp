//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/async.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/detail/type_list.hpp"

#include <any>

namespace tenzir {

namespace _::operator_plugin {

using DataArgTypes = detail::tl_filter_not_type_t<data::types, pattern>;

using ArgTypes
  = detail::tl_concat_t<DataArgTypes, caf::type_list<ast::expression>>;

template <class T>
concept ArgType = detail::tl_contains_v<ArgTypes, T>;

template <ArgType T>
using Setter = std::function<void(std::any&, T)>;

template <ArgType T>
struct AsSetter : std::type_identity<Setter<T>> {};

using Setters = detail::tl_map_t<ArgTypes, AsSetter>;

using AnySetter = detail::tl_apply_t<Setters, variant>;

struct Positional {
  std::string name;
  AnySetter setter;
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
  auto positional(std::string name, T Args::* ptr) -> Argument<Args, T> {
    // TODO: Check exact type?
    if (desc_.first_optional) {
      panic("cannot have required positional after optional positional");
    }
    desc_.positional.push_back(Positional{
      std::move(name),
      Setter<T>{[ptr](std::any& args, T value) {
        (&std::any_cast<Args&>(args))->*ptr = std::move(value);
      }},
    });
    return Argument<Args, T>{};
  }

  template <ArgType T>
  auto positional(std::string name, std::optional<T> Args::* ptr)
    -> Argument<Args, T> {
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    desc_.positional.push_back(Positional{
      std::move(name),
      Setter<T>{[ptr](std::any& args, T value) {
        (&std::any_cast<Args&>(args))->*ptr = std::move(value);
      }},
    });
    return Argument<Args, T>{};
  }

  template <ArgType T>
  auto optional_positional(std::string name, T Args::* ptr)
    -> Argument<Args, T> {
    if (not desc_.first_optional) {
      desc_.first_optional = desc_.positional.size();
    }
    desc_.positional.push_back(Positional{
      std::move(name),
      Setter<T>{[ptr](std::any& args, T value) {
        (&std::any_cast<Args&>(args))->*ptr = std::move(value);
      }},
    });
    return Argument<Args, T>{};
  }

  template <class T>
  auto named(std::string name, T Args::* ptr) -> Argument<Args, T>;

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

using _::operator_plugin::OperatorPlugin;
using _::operator_plugin::Description;
using _::operator_plugin::Describer;

} // namespace tenzir
