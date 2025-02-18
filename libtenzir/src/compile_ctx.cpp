//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"

namespace tenzir {

auto compile_ctx::make_root(base_ctx ctx) -> root {
  return root{ctx};
}

auto compile_ctx::open_scope() -> scope {
  auto new_env = std::make_unique<env_t>(env());
  env_ = new_env.get();
  return scope{std::move(new_env), root_};
}

auto compile_ctx::get(std::string_view name) const -> std::optional<let_id> {
  if (not env_) {
    return std::nullopt;
  }
  auto it = env_->find(std::string{name});
  if (it == env_->end()) {
    return {};
  }
  return it->second;
}

auto compile_ctx::env() const -> env_t {
  if (not env_) {
    return {};
  }
  return *env_;
}

auto compile_ctx::without_env() const -> compile_ctx {
  return compile_ctx{root_, nullptr};
}

auto compile_ctx::scope::let(std::string name) & -> let_id {
  TENZIR_ASSERT(env_);
  root_.last_let_id_ += 1;
  auto id = let_id{root_.last_let_id_};
  auto inserted = env_->try_emplace(std::move(name), id).second;
  TENZIR_ASSERT(inserted);
  return id;
}

compile_ctx::scope::scope(std::unique_ptr<env_t> env, root& root)
  : env_{std::move(env)}, root_{root} {
  TENZIR_ASSERT(env_);
}

compile_ctx::root::operator compile_ctx() {
  return compile_ctx{*this, nullptr};
}

compile_ctx::root::operator base_ctx() const {
  return ctx_;
}

compile_ctx::compile_ctx(root& root, const env_t* env)
  : root_{root}, env_{env} {
}

} // namespace tenzir
