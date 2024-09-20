//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/source_id.hpp"

namespace tenzir {

class source_ref {
public:
  ~source_ref();
  source_ref(const source_ref&) = delete;
  auto operator=(const source_ref&) -> source_ref& = delete;
  source_ref(source_ref&& other) noexcept : source_ref() {
    *this = std::move(other);
  }
  auto operator=(source_ref&& other) noexcept -> source_ref& {
    reset();
    std::swap(origin_, other.origin_);
    std::swap(id_, other.id_);
    return *this;
  }

  auto borrow() const -> source_id {
    return id_;
  }

  void reset();

private:
  friend class source_map;

  source_ref() : origin_{nullptr}, id_{source_id::unknown} {
  }

  source_ref(source_map& origin, source_id id)
    // TODO: Did we really overload `&` such that it doesn't work with
    // incomplete types?
    : origin_{std::addressof(origin)}, id_{id} {
  }

  source_map* origin_;
  source_id id_;
};

} // namespace tenzir
