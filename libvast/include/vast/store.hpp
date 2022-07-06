//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

class passive_store {};

class active_store {};

struct default_passive_store_state {
  static constexpr auto name = "passive-store";

  system::store_actor::pointer self = {};
  system::filesystem_actor filesystem = {};
  system::accountant_actor accountant = {};
  std::unique_ptr<passive_store> store = {};
};

struct default_active_store_state {
  static constexpr auto name = "active-store";

  system::store_builder_actor::pointer self = {};
  system::filesystem_actor filesystem = {};
  system::accountant_actor accountant = {};
  std::unique_ptr<active_store> store = {};
};

system::store_actor::behavior_type default_passive_store(
  system::store_actor::stateful_pointer<default_passive_store_state> self,
  std::unique_ptr<passive_store> store, system::filesystem_actor filesystem,
  system::accountant_actor accountant);

system::store_builder_actor::behavior_type default_active_store(
  system::store_builder_actor::stateful_pointer<default_active_store_state> self,
  std::unique_ptr<active_store> store, system::filesystem_actor filesystem,
  system::accountant_actor accountant);

} // namespace vast
