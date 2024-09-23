//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/source_map.hpp"

#include "tenzir/logger.hpp"

namespace tenzir {

void source_ref::reset() {
  if (origin_ == nullptr) {
    return;
  }
  auto lock = std::unique_lock{origin_->mutex_};
  auto removed = origin_->entries_.erase(id_);
  if (removed == 0) {
    TENZIR_ERROR("tried to remove already removed source map entry");
  }
  origin_ = nullptr;
}

auto source_map::add(source_entry info) -> source_ref {
  auto lock = std::unique_lock{mutex_};
  auto [it, inserted] = entries_.try_emplace(next_, std::move(info));
  next_ = source_id{next_.value + 1};
  TENZIR_ASSERT(inserted);
  return source_ref{*this, it->first};
}

auto source_map::get(source_id id) const -> const source_entry& {
  auto lock = std::unique_lock{mutex_};
  auto it = entries_.find(id);
  TENZIR_ASSERT(it != entries_.end());
  return it->second;
}

auto global_source_map() -> source_map& {
  static auto map = source_map{};
  return map;
}

} // namespace tenzir
