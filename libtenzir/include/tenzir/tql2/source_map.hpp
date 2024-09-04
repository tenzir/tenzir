//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/logger.hpp"

#include <mutex>
#include <string>
#include <unordered_map>

namespace tenzir {

class source_map;
class source_ref;
class source_owner;

class source_ref {
  friend class source_owner;
  friend class source_map;

public:
  auto operator<=>(const source_ref&) const = default;

  static const source_ref unknown;

private:
  source_ref(uint32_t id) : id_{id} {
  }

  uint32_t id_;
};

inline source_ref const source_ref::unknown = source_ref{0};

class source_owner {
public:
  ~source_owner();
  source_owner(const source_owner&) = delete;
  auto operator=(const source_owner&) -> source_owner& = delete;
  source_owner(source_owner&& other) noexcept : source_owner() {
    *this = std::move(other);
  }
  auto operator=(source_owner&& other) noexcept -> source_owner& {
    reset();
    std::swap(origin_, other.origin_);
    std::swap(id_, other.id_);
    return *this;
  }

  auto ref() const -> source_ref {
    return source_ref{id_};
  }

  void reset();

private:
  source_owner() : origin_{nullptr}, id_{0} {
  }

  source_owner(source_map& origin, uint32_t id) : origin_{&origin}, id_{id} {
  }

  source_map* origin_;
  uint32_t id_;
};

struct source_entry {
  std::string source;
  std::string path;
};

class source_map {
  friend class source_owner;

public:
  [[nodiscard]] auto add(source_entry info) -> source_owner;

  // TODO: We return by ref here. The caller makes sure that the owner lives?
  auto get(source_ref ref) const -> const source_entry& {
    auto lock = std::unique_lock{mutex_};
    auto it = entries_.find(ref.id_);
    TENZIR_ASSERT(it != entries_.end());
    return it->second;
  }

private:
  std::mutex mutex_;
  std::unordered_map<uint32_t, source_entry> entries_;
};

inline void source_owner::reset() {
  if (origin_ == nullptr) {
    return;
  }
  auto lock = std::unique_lock{origin_->mutex_};
  auto removed = origin_->entries_.erase(id_);
  if (removed == 0) {
    TENZIR_ERROR("tried to remove already removed source map entry");
  }
  id_ = 0;
  origin_ = nullptr;
}

// TODO: This should probably be attached to the actor context.
auto global_source_map() -> source_map&;

} // namespace tenzir
