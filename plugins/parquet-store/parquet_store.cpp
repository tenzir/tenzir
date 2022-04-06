//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/chunk.hpp>
#include <vast/data.hpp>
#include <vast/error.hpp>
#include <vast/plugin.hpp>
#include <vast/query.hpp>

#include <arrow/io/file.h>
#include <caf/expected.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <parquet/stream_reader.h>

namespace vast::plugins::parquet_store {

struct store_builder_state {
  static constexpr const char* name = "active-parquet-store";
  uuid id_ = {};
  system::store_builder_actor::pointer self_ = {};
};

struct store_state {
  static constexpr const char* name = "passive-parquet-store";
  uuid id_ = {};
  system::store_actor::pointer self_ = {};
};

system::store_actor::behavior_type
store(system::store_actor::stateful_pointer<store_state> self, const uuid& id) {
  self->state.self_ = self;
  self->state.id_ = id;
  return {
    [self](const query& query) -> caf::result<uint64_t> {
      auto infile = arrow::io::ReadableFile::Open("test.parquet").ValueOrDie();
      parquet::StreamReader os{parquet::ParquetFileReader::Open(infile)};
      return ec::unimplemented;
    },
    [self](atom::erase, const ids& ids) -> caf::result<uint64_t> {
      return ec::unimplemented;
    },
  };
}

system::store_builder_actor::behavior_type store_builder(
  system::store_builder_actor::stateful_pointer<store_builder_state> self,
  const uuid& id) {
  self->state.self_ = self;
  self->state.id_ = id;
  return {
    [self](const query& query) -> caf::result<uint64_t> {
      return ec::unimplemented;
    },
    [self](atom::erase, const ids& ids) -> caf::result<uint64_t> {
      return ec::unimplemented;
    },
    [self](
      caf::stream<table_slice> in) -> caf::inbound_stream_slot<table_slice> {
      VAST_WARN("{} attached stream sink", *self);
      return {};
    },
    [self](atom::status,
           system::status_verbosity verbosity) -> caf::result<record> {
      return ec::unimplemented;
    },
  };
}

/// The plugin entrypoint for the aggregate transform plugin.
class plugin final : public store_plugin {
public:
  /// Initializes the aggregate plugin. This plugin has no general
  /// configuration, and is configured per instantiation as part of the
  /// transforms definition. We only check whether there's no unexpected
  /// configuration here.
  caf::error initialize(data options) override {
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    if (const auto* rec = caf::get_if<record>(&options))
      if (rec->empty())
        return caf::none;
    return caf::make_error(ec::invalid_configuration, //
                           "expected empty configuration under "
                           "vast.plugins.parquet-store");
  }

  [[nodiscard]] const char* name() const override {
    return "parquet-store";
  };

  /// Create a store builder actor that accepts incoming table slices.
  /// @param accountant The actor handle of the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param id The partition id for which we want to create a store. Can
  ///           be used as a unique key by the implementation.
  /// @returns A store_builder actor and a chunk called the "header". The
  ///          contents of the header will be persisted on disk, and should
  ///          allow the plugin to retrieve the correct store actor when
  ///          `make_store()` below is called.
  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(system::accountant_actor accountant,
                     system::filesystem_actor fs,
                     const vast::uuid& id) const override {
    auto actor_handle = fs.home_system().spawn(store_builder, id);
    auto header = chunk::copy(id);
    return builder_and_header{actor_handle, header};
  }

  /// Create a store actor from the given header. Called when deserializing a
  /// partition that uses this partition as a store backend.
  /// @param accountant The actor handle the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param header The store header as found in the partition flatbuffer.
  /// @returns A new store actor.
  [[nodiscard]] caf::expected<system::store_actor>
  make_store(system::accountant_actor accountant, system::filesystem_actor fs,
             std::span<const std::byte> header) const override {
    if (header.size() != uuid::num_bytes)
      return caf::make_error(ec::invalid_argument, "header must have size of "
                                                   "single uuid");
    auto id = uuid(std::span<const std::byte, uuid::num_bytes>(header.data(),
                                                               header.size()));
    return fs.home_system().spawn(store, id);
  }
};

} // namespace vast::plugins::parquet_store

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::parquet_store::plugin)
