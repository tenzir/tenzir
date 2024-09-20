//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/source_id.hpp"
#include "tenzir/tql2/source_ref.hpp"

#include <mutex>
#include <string>
#include <unordered_map>

namespace tenzir {

struct source_entry {
  std::string text;
  std::string path;
};

class source_map {
public:
  [[nodiscard]] auto add(source_entry info) -> source_ref;

  // TODO: We return by ref here. The caller makes sure that the owner lives?
  auto get(source_id id) const -> const source_entry&;

private:
  friend class source_ref;

  mutable std::mutex mutex_;
  // TODO: Maybe it's better to randomize this?
  source_id next_ = source_id{1};
  std::unordered_map<source_id, source_entry> entries_;
};

// TODO: This should probably be attached to the actor context.
auto global_source_map() -> source_map&;

} // namespace tenzir
