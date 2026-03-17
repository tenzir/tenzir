//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/substitute_ctx.hpp"

namespace tenzir {

substitute_ctx::substitute_ctx(base_ctx ctx, const env_t* env)
  : ctx_{ctx}, env_{env} {
}

auto substitute_ctx::get(let_id id) const
  -> std::optional<ast::constant::kind> {
  if (not env_) {
    return std::nullopt;
  }
  auto it = env_->find(id);
  if (it == env_->end()) {
    return std::nullopt;
  }
  return it->second;
}

auto substitute_ctx::env() const
  -> std::unordered_map<let_id, ast::constant::kind> {
  if (not env_) {
    return {};
  }
  return *env_;
}

auto substitute_ctx::with_env(const env_t* env) const -> substitute_ctx {
  return substitute_ctx{ctx_, env};
}

} // namespace tenzir
