//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/store.hpp"

#include "vast/error.hpp"

namespace vast {

system::store_actor::behavior_type default_passive_store(
  system::store_actor::stateful_pointer<default_passive_store_state> self,
  std::unique_ptr<passive_store> store, system::filesystem_actor filesystem,
  system::accountant_actor accountant) {
  // Configure our actor state.
  self->state.self = self;
  self->state.filesystem = std::move(filesystem);
  self->state.accountant = std::move(accountant);
  self->state.store = std::move(store);
  return {
    [](const query& query) -> caf::result<uint64_t> {
      // FIXME: Handle query lookup
      return ec::unimplemented;
    },
    [](atom::erase, const ids& selection) -> caf::result<uint64_t> {
      // FIXME: Handle erasure
      return ec::unimplemented;
    },
  };
}

system::store_builder_actor::behavior_type default_active_store(
  system::store_builder_actor::stateful_pointer<default_active_store_state> self,
  std::unique_ptr<active_store> store, system::filesystem_actor filesystem,
  system::accountant_actor accountant) {
  // Configure our actor state.
  self->state.self = self;
  self->state.filesystem = std::move(filesystem);
  self->state.accountant = std::move(accountant);
  self->state.store = std::move(store);
  return {
    [](const query& query) -> caf::result<uint64_t> {
      // FIXME: Handle query lookup
      return ec::unimplemented;
    },
    [](atom::erase, const ids& selection) -> caf::result<uint64_t> {
      // FIXME: Handle erasure
      return ec::unimplemented;
    },
    [](caf::stream<table_slice> stream)
      -> caf::inbound_stream_slot<table_slice> {
      return {};
    },
    [](atom::status, status_verbosity verbosity) -> caf::result<record> {
      return ec::unimplemented;
    },
  };
}

} // namespace vast
