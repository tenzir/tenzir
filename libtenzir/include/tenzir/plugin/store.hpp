//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/plugin/base.hpp"
#include "tenzir/uuid.hpp"

#include <caf/expected.hpp>

#include <cstddef>
#include <memory>
#include <span>

namespace tenzir {

// -- store plugin ------------------------------------------------------------

/// A base class for plugins that add new store backends.
/// @note Consider using the simler `store_plugin` instead, which abstracts
/// the actor system logic away with a default implementation, which usually
/// suffices for most store backends.
class store_actor_plugin : public virtual plugin {
public:
  /// A store_builder actor and a chunk called the "header". The contents of
  /// the header will be persisted on disk, and should allow the plugin to
  /// retrieve the correct store actor when `make_store()` below is called.
  struct builder_and_header {
    store_builder_actor store_builder;
    chunk_ptr header;
  };

  /// Create a store builder actor that accepts incoming table slices.
  /// The store builder is required to keep a reference to itself alive
  /// as long as its input stream is live, and persist itself and exit as
  /// soon as the input stream terminates.
  /// @param fs The actor handle of a filesystem.
  /// @param id The partition id for which we want to create a store. Can be
  /// used as a unique key by the implementation.
  /// @param origin An origin identifier to embed into the store file.
  /// @returns A handle to the store builder actor to add events to, and a
  /// header that uniquely identifies this store for later use in `make_store`.
  [[nodiscard]] virtual auto
  make_store_builder(filesystem_actor fs, const uuid& id,
                     std::string origin) const
    -> caf::expected<builder_and_header>
    = 0;

  /// Create a store actor from the given header. Called when deserializing a
  /// partition that uses this partition as a store backend.
  /// @param fs The actor handle of a filesystem.
  /// @param header The store header as found in the partition flatbuffer.
  /// @returns A new store actor.
  [[nodiscard]] virtual auto
  make_store(filesystem_actor fs, std::span<const std::byte> header) const
    -> caf::expected<store_actor>
    = 0;
};

/// A base class for plugins that add new store backends.
class store_plugin : public virtual store_actor_plugin {
public:
  /// Create a store for passive partitions.
  [[nodiscard]] virtual auto make_passive_store() const
    -> caf::expected<std::unique_ptr<passive_store>>
    = 0;

  /// Create a store for active partitions.
  /// @param tenzir_config The tenzir node configuration.
  [[nodiscard]] virtual auto make_active_store() const
    -> caf::expected<std::unique_ptr<active_store>>
    = 0;

private:
  [[nodiscard]] auto make_store_builder(filesystem_actor fs, const uuid& id,
                                        std::string origin) const
    -> caf::expected<builder_and_header> final;

  [[nodiscard]] auto
  make_store(filesystem_actor fs, std::span<const std::byte> header) const
    -> caf::expected<store_actor> final;
};

} // namespace tenzir
