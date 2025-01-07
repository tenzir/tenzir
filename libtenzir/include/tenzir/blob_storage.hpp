//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugin.hpp"

#include <caf/fwd.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

struct blob_storage_actor_traits {
  using signatures = caf::type_list<
    // Write into a file.
    auto(atom::write, std::string path, caf::typed_stream<chunk_ptr>)
      ->caf::result<void>,
    // Read from a file.
    auto(atom::read, std::string path)->caf::result<caf::typed_stream<chunk_ptr>>,
    // Move a file.
    auto(atom::move, std::string old_path, std::string new_path)
      ->caf::result<void>,
    // Delete a file.
    auto(atom::erase, std::string path)->caf::result<void>>;
};

using blob_storage_actor = caf::typed_actor<blob_storage_actor_traits>;

struct blob_storage_plugin : public virtual plugin {
  auto get_or_spawn_blob_storage(caf::actor_system& sys) -> blob_storage_actor {
    const auto guard = std::lock_guard{mtx_};
    if (auto handle = blob_storage_.lock()) {
      return handle;
    }
    auto handle = spawn_blob_storage(sys);
    blob_storage_ = handle;
    return handle;
  }

protected:
  virtual auto spawn_blob_storage(caf::actor_system& sys) -> blob_storage_actor
    = 0;

private:
  mutable std::mutex mtx_;
  detail::weak_handle<blob_storage_actor> blob_storage_;
};

} // namespace tenzir
