//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/global_concepts.hpp"

namespace vast::global_concepts {

namespace {

bool initialized = false;

auto get_impl() -> concepts_map& {
  static concepts_map data;
  return data;
}

} // namespace

auto init(concepts_map m) -> bool {
  if (initialized) [[likely]]
    return false;
  get_impl() = std::move(m);
  initialized = true;
  return true;
}

auto get() -> const concepts_map* {
  if (not initialized)
    return nullptr;
  return &get_impl();
}

} // namespace vast::global_concepts
