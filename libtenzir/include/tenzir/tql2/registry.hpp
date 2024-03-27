//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"

namespace tenzir::tql2 {

class context;

// TODO: Change `entity_def` and everything related to it.

class function_def {
public:
  // bad argument count
  // bad argument type error
  // + maybe type

  class check_info {
  public:
    check_info(location fn, std::span<const ast::expression> exprs,
               std::span<const std::optional<type>> types)
      : fn_{fn}, exprs_{exprs}, types_{types} {
    }

    auto arg_count() const -> size_t {
      return exprs_.size();
    }

    auto fn_loc() const -> location {
      return fn_;
    }

    auto arg_loc(size_t x) const -> location {
      TENZIR_ASSERT(x < arg_count());
      return exprs_[x].get_location();
    }

    auto arg_type(size_t x) const -> const std::optional<type>& {
      TENZIR_ASSERT(x < arg_count());
      return types_[x];
    }

  private:
    location fn_;
    std::span<const ast::expression> exprs_;
    std::span<const std::optional<type>> types_;
  };

  virtual ~function_def() = default;

  virtual auto check(check_info info, context& ctx) const -> std::optional<type>
    = 0;

  virtual auto
  evaluate(location fn, std::vector<located<data>> args, context& ctx) const
    -> std::optional<data>
    = 0;
};

class operator_use {
public:
  virtual ~operator_use() = default;

  virtual auto debug(debug_writer& f) -> bool {
    TENZIR_UNUSED(f);
    TENZIR_TODO();
  }

  friend auto inspect(caf::serializer& f, operator_use& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      auto name = caf::detail::pretty_type_name(typeid(x));
      auto dot = name.find_last_of('.');
      if (dot != std::string::npos) {
        name.erase(0, dot + 1);
      }
      return dbg->prepend("{} ", name) && x.debug(*dbg);
    }
    TENZIR_TODO();
  }
};

class operator_def {
public:
  virtual ~operator_def() = default;

  virtual auto
  make(ast::entity self, std::vector<ast::expression> args, context& ctx) const
    -> std::unique_ptr<operator_use>
    = 0;
};

using entity_def
  = variant<std::unique_ptr<function_def>, std::unique_ptr<operator_def>>;

class registry {
public:
  // TODO: The interface of this class is drastically simplified for now. It
  // must be changed eventually to properly enable modules and use an interned
  // representation of `entity_path`.
  void add(std::string name, entity_def fn) {
    auto inserted = defs_.emplace(std::move(name), std::move(fn)).second;
    TENZIR_ASSERT(inserted);
  }

  auto try_get(const entity_path& path) const -> const entity_def* {
    if (path.segments().size() != 1) {
      // TODO: We pretend here that only single-name paths exist.
      return nullptr;
    }
    auto it = defs_.find(path.segments()[0]);
    if (it == defs_.end()) {
      return nullptr;
    }
    return &it->second;
  }

  auto get(const entity_path& path) const -> const entity_def& {
    auto result = try_get(path);
    TENZIR_ASSERT(result);
    return *result;
  }

private:
  // TODO: Lifetime?
  detail::heterogeneous_string_hashmap<entity_def> defs_;
};

auto thread_local_registry() -> const registry*;

void set_thread_local_registry(const registry* reg);

template <class F>
auto with_thread_local_registry(const registry& reg, F&& f) {
  auto prev = thread_local_registry();
  set_thread_local_registry(&reg);
  std::forward<F>(f)();
  set_thread_local_registry(prev);
}

class context {
public:
  context(registry& reg, diagnostic_handler& dh) : reg_{reg}, dh_{dh} {
  }

  auto reg() -> registry& {
    return reg_;
  }

  auto dh() -> diagnostic_handler& {
    return dh_;
  }

private:
  registry& reg_;
  diagnostic_handler& dh_;
};

} // namespace tenzir::tql2
