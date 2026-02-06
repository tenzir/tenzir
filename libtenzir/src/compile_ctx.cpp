//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"

#include <utility>

namespace tenzir {

auto compile_ctx::make_root(base_ctx ctx) -> root {
  return root{ctx};
}

auto compile_ctx::open_scope() -> scope {
  auto original_env = env_;
  auto new_env = std::make_unique<env_t>(env());
  env_ = new_env.get();
  return scope{std::move(new_env), this, original_env, root_};
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

compile_ctx::scope::~scope() {
  if (ctx_) {
    ctx_->env_ = original_env_;
  }
}

compile_ctx::scope::scope(scope&& other) noexcept
  : env_{std::move(other.env_)},
    ctx_{std::exchange(other.ctx_, nullptr)},
    original_env_{other.original_env_},
    root_{other.root_} {
}

auto compile_ctx::scope::operator=(scope&& other) noexcept -> scope& {
  if (this != &other) {
    // Restore original env if we were managing a scope
    if (ctx_) {
      ctx_->env_ = original_env_;
    }
    env_ = std::move(other.env_);
    ctx_ = std::exchange(other.ctx_, nullptr);
    original_env_ = other.original_env_;
    // Note: root_ is a reference, cannot be reassigned
  }
  return *this;
}

compile_ctx::scope::scope(std::unique_ptr<env_t> env, compile_ctx* ctx,
                          const env_t* original_env, root& root)
  : env_{std::move(env)}, ctx_{ctx}, original_env_{original_env}, root_{root} {
  TENZIR_ASSERT(env_);
  TENZIR_ASSERT(ctx_);
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
