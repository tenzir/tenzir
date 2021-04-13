//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/atoms.hpp"

#include <caf/replies_to.hpp>

#include <filesystem>

#define VAST_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(vast_actors, type)

// NOLINTNEXTLINE(cert-dcl58-cpp)
namespace std::filesystem {

// TODO: With CAF 0.18, drop this inspector that is—strictly speaking—undefined
// behavior and replace it with a specialization of caf::inspector_access.
template <class Inspector>
typename Inspector::result_type
inspect(Inspector& f, ::std::filesystem::path& x) {
  auto str = x.string();
  if constexpr (std::is_same_v<typename Inspector::result_type, void>) {
    f(str);
    if constexpr (Inspector::reads_state)
      x = {str};
    return;
  } else {
    auto result = f(str);
    if constexpr (Inspector::reads_state)
      x = {str};
    return result;
  }
}

} // namespace std::filesystem

namespace vast::system {

/// Helper utility that enables extending typed actor forward declarations
/// without including <caf/typed_actor.hpp>.
template <class... Fs>
struct typed_actor_fwd;

template <class... Fs>
struct typed_actor_fwd {
  template <class Handle>
  struct extend_with_helper;

  template <class... Gs>
  struct extend_with_helper<caf::typed_actor<Gs...>> {
    using type = typed_actor_fwd<Fs..., Gs...>;
  };

  template <class Handle>
  using extend_with = typename extend_with_helper<Handle>::type;

  using unwrap = caf::typed_actor<Fs...>;
};

/// The STREAM SINK actor interface.
/// @tparam Unit The stream unit.
/// @tparam Args... Additional parameters passed using
/// `caf::stream_source::add_outbound_path`.
template <class Unit, class... Args>
using stream_sink_actor = typename typed_actor_fwd<
  // Add a new source.
  typename caf::replies_to<caf::stream<Unit>, Args...>::template with< //
    caf::inbound_stream_slot<Unit>>>::unwrap;

/// The FLUSH LISTENER actor interface.
using flush_listener_actor = typed_actor_fwd<
  // Reacts to the requested flush message.
  caf::reacts_to<atom::flush>>::unwrap;

template <class T, class... Ts>
using receiver_actor = typename typed_actor_fwd<
  // Add a new source.
  typename caf::reacts_to<T, Ts...>>::unwrap;

/// The INDEX CLIENT actor interface.
using index_client_actor = typed_actor_fwd<
  // Receives done from the INDEX when the query finished.
  caf::reacts_to<atom::done>>
  // Receives ids from the INDEX for partial query hits.
  ::extend_with<receiver_actor<table_slice>>::unwrap;

/// The STATUS CLIENT actor interface.
using status_client_actor = typed_actor_fwd<
  // Reply to a status request from the NODE.
  caf::replies_to<atom::status, status_verbosity>::with< //
    caf::dictionary<caf::config_value>>>::unwrap;

using store_actor = typed_actor_fwd<
  // Handles a query for the given expression, optionally optimized by a set of
  // ids to pre-select the events to evaluate.
  caf::replies_to<atom::extract, expression, ids, receiver_actor<table_slice>,
                  bool>::with<atom::done>,
  // Erase the events with the given ids.
  caf::replies_to<atom::erase, ids>::with<atom::done>>::unwrap;

using store_builder_actor = typed_actor_fwd<>::extend_with<store_actor>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The PARTITION actor interface.
using partition_actor = typed_actor_fwd<
  // Evaluate the given expression and send the matching events to the receiver.
  caf::replies_to<expression, receiver_actor<table_slice>>::with<atom::done>>
  // Conform to the procol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// A set of relevant partition actors, and their uuids.
// TODO: Move this elsewhere.
using query_map = std::vector<std::pair<uuid, partition_actor>>;

/// The QUERY SUPERVISOR actor interface.
using query_supervisor_actor = typed_actor_fwd<
  /// Reacts to an expression and a set of relevant partitions by
  /// sending several `vast::ids` to the index_client_actor, followed
  /// by a final `atom::done`.
  caf::reacts_to<expression, query_map, index_client_actor>>::unwrap;

/// The EVALUATOR actor interface.
using evaluator_actor = typed_actor_fwd<
  // Evaluates the expression and responds with matching ids.
  caf::replies_to<atom::run>::with<ids>>::unwrap;

/// The INDEXER actor interface.
using indexer_actor = typed_actor_fwd<
  // Returns the ids for the given predicate.
  caf::replies_to<curried_predicate>::with<ids>,
  // Requests the INDEXER to shut down.
  caf::reacts_to<atom::shutdown>>::unwrap;

/// The ACTIVE INDEXER actor interface.
using active_indexer_actor = typed_actor_fwd<
  // Hooks into the table slice column stream.
  caf::replies_to<caf::stream<table_slice_column>>::with<
    caf::inbound_stream_slot<table_slice_column>>,
  // Finalizes the ACTIVE INDEXER into a chunk, which containes an INDEXER.
  caf::replies_to<atom::snapshot>::with<chunk_ptr>>
  // Conform the the INDEXER ACTOR interface.
  ::extend_with<indexer_actor>
  // Conform to the procol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The ACCOUNTANT actor interface.
using accountant_actor = typed_actor_fwd<
  // Update the configuration of the ACCOUNTANT.
  caf::replies_to<atom::config, accountant_config>::with< //
    atom::ok>,
  // Registers the sender with the ACCOUNTANT.
  caf::reacts_to<atom::announce, std::string>,
  // Record duration metric.
  caf::reacts_to<std::string, duration>,
  // Record time metric.
  caf::reacts_to<std::string, time>,
  // Record integer metric.
  caf::reacts_to<std::string, integer>,
  // Record count metric.
  caf::reacts_to<std::string, count>,
  // Record real metric.
  caf::reacts_to<std::string, real>,
  // Record a metrics report.
  caf::reacts_to<report>,
  // Record a performance report.
  caf::reacts_to<performance_report>,
  // The internal telemetry loop of the ACCOUNTANT.
  caf::reacts_to<atom::telemetry>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The QUERY SUPERVISOR MASTER actor interface.
using query_supervisor_master_actor = typed_actor_fwd<
  // Enlist the QUERY SUPERVISOR as an available worker.
  caf::reacts_to<atom::worker, query_supervisor_actor>>::unwrap;

/// The META INDEX actor interface.
using meta_index_actor = typed_actor_fwd<
  // Bulk import a set of partition synopses.
  caf::replies_to<atom::merge, std::shared_ptr<std::map<
                                 uuid, partition_synopsis>>>::with<atom::ok>,
  // Merge a single partition synopsis.
  caf::replies_to<atom::merge, uuid, std::shared_ptr<partition_synopsis>>::with< //
    atom::ok>,
  // Erase a single partition synopsis.
  caf::replies_to<atom::erase, uuid>::with<atom::ok>,
  // Evaluate the expression.
  caf::replies_to<expression>::with< //
    std::vector<uuid>>>::unwrap;

/// The INDEX actor interface.
using index_actor = typed_actor_fwd<
  // Triggered when the INDEX finished querying a PARTITION.
  caf::reacts_to<atom::done, uuid>,
  // Registers the INDEX with the ACCOUNTANT.
  caf::reacts_to<accountant_actor>,
  // Subscribes a FLUSH LISTENER to the INDEX.
  caf::reacts_to<atom::subscribe, atom::flush, flush_listener_actor>,
  // Evaluatates an expression.
  caf::reacts_to<expression>,
  // Queries PARTITION actors for a given query id.
  caf::reacts_to<uuid, uint32_t>,
  // Erases the given events from the INDEX, and returns their ids.
  caf::replies_to<atom::erase, uuid>::with<ids>>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the QUERY SUPERVISOR MASTER actor.
  ::extend_with<query_supervisor_master_actor>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The ARCHIVE actor interface.
using archive_actor = typed_actor_fwd<
  // Registers the ARCHIVE with the ACCOUNTANT.
  caf::reacts_to<accountant_actor>,
  // INTERNAL: Handles a query for the given ids, and sends the table slices
  // back to the client.
  caf::reacts_to<atom::internal>,
  // The internal telemetry loop of the ARCHIVE.
  caf::reacts_to<atom::telemetry>>
  // Conform to the protocol of the STORE actor.
  ::extend_with<store_actor>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The TYPE REGISTRY actor interface.
using type_registry_actor = typed_actor_fwd<
  // The internal telemetry loop of the TYPE REGISTRY.
  caf::reacts_to<atom::telemetry>,
  // Registers the given type.
  caf::reacts_to<atom::put, vast::type>,
  // Registers all types in the given schema.
  caf::reacts_to<atom::put, vast::schema>,
  // Retrieves all known types.
  caf::replies_to<atom::get>::with<type_set>,
  // Registers the given taxonomies.
  caf::reacts_to<atom::put, taxonomies>,
  // Retrieves the known taxonomies.
  caf::replies_to<atom::get, atom::taxonomies>::with< //
    taxonomies>,
  // Loads the taxonomies on disk.
  caf::replies_to<atom::load>::with< //
    atom::ok>,
  // Resolves an expression in terms of the known taxonomies.
  caf::replies_to<atom::resolve, expression>::with< //
    expression>,
  // Registers the TYPE REGISTRY with the ACCOUNTANT.
  caf::reacts_to<accountant_actor>>
  // Conform to the procotol of the STREAM SINK actor for table slices,
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The DISK MONITOR actor interface.
using disk_monitor_actor = typed_actor_fwd<
  // Checks the monitoring requirements.
  caf::reacts_to<atom::ping>,
  // Purge events as required for the monitoring requirements.
  caf::reacts_to<atom::erase>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface for file system I/O. The filesystem actor implementation
/// must interpret all operations that contain paths *relative* to its own
/// root directory.
using filesystem_actor = typed_actor_fwd<
  // Writes a chunk of data to a given path. Creates intermediate directories
  // if needed.
  caf::replies_to<atom::write, std::filesystem::path, chunk_ptr>::with< //
    atom::ok>,
  // Reads a chunk of data from a given path and returns the chunk.
  caf::replies_to<atom::read, std::filesystem::path>::with< //
    chunk_ptr>,
  // Memory-maps a file.
  caf::replies_to<atom::mmap, std::filesystem::path>::with< //
    chunk_ptr>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface of an ACTIVE PARTITION actor.
using active_partition_actor = typed_actor_fwd<
  caf::reacts_to<atom::subscribe, atom::flush, flush_listener_actor>,
  // Persists the active partition at the specified path.
  caf::replies_to<atom::persist, std::filesystem::path,
                  std::filesystem::path>::with< //
    std::shared_ptr<partition_synopsis>>,
  // INTERNAL: A repeatedly called continuation of the persist request.
  caf::reacts_to<atom::internal, atom::persist, atom::resume>>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the PARTITION actor.
  ::extend_with<partition_actor>::unwrap;

/// The interface of the EXPORTER actor.
using exporter_actor = typed_actor_fwd<
  // Request extraction of all events.
  caf::reacts_to<atom::extract>,
  // Request extraction of the given number of events.
  caf::reacts_to<atom::extract, uint64_t>,
  // Register the ACCOUNTANT actor.
  caf::reacts_to<accountant_actor>,
  // Register the INDEX actor.
  caf::reacts_to<index_actor>,
  // Register the SINK actor.
  caf::reacts_to<atom::sink, caf::actor>,
  // Execute previously registered query.
  caf::reacts_to<atom::run>,
  // Register a STATISTICS SUBSCRIBER actor.
  caf::reacts_to<atom::statistics, caf::actor>>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>
  // Conform to the protocol of the INDEX CLIENT actor.
  ::extend_with<index_client_actor>::unwrap;

/// The interface of a COMPONENT PLUGIN actor.
using component_plugin_actor = typed_actor_fwd<>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface of an ANALYZER PLUGIN actor.
using analyzer_plugin_actor = typed_actor_fwd<>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the COMPONENT PLUGIN actor.
  ::extend_with<component_plugin_actor>::unwrap;

/// The interface of an IMPORTER actor.
using importer_actor = typed_actor_fwd<
  // Register the ACCOUNTANT actor.
  caf::reacts_to<accountant_actor>,
  // Add a new sink.
  caf::replies_to<stream_sink_actor<table_slice>>::with< //
    caf::outbound_stream_slot<table_slice>>,
  // Register a FLUSH LISTENER actor.
  caf::reacts_to<atom::subscribe, atom::flush, flush_listener_actor>,
  // The internal telemetry loop of the IMPORTER.
  caf::reacts_to<atom::telemetry>>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the STREAM SINK actor for table slices with a
  // description.
  ::extend_with<stream_sink_actor<table_slice, std::string>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface of the NODE actor.
using node_actor = typed_actor_fwd<
  // Run an invocation in the node.
  caf::replies_to<atom::run, invocation>::with< //
    caf::message>,
  // INTERNAL: Spawn component plugins.
  caf::reacts_to<atom::internal, atom::spawn, atom::plugin>,
  // Run an invocation in the node that spawns an actor.
  caf::replies_to<atom::spawn, invocation>::with< //
    caf::actor>,
  // Add a component to the component registry.
  caf::replies_to<atom::put, caf::actor, std::string>::with< //
    atom::ok>,
  // Retrieve components by their type from the component registry.
  caf::replies_to<atom::get, atom::type, std::string>::with< //
    std::vector<caf::actor>>,
  // Retrieve a component by its label from the component registry.
  caf::replies_to<atom::get, atom::label, std::string>::with< //
    caf::actor>,
  // Retrieve components by their label from the component registry.
  caf::replies_to<atom::get, atom::label, std::vector<std::string>>::with< //
    std::vector<caf::actor>>,
  // Retrieve the version of the process running the NODE.
  caf::replies_to<atom::get, atom::version>::with< //
    std::string>,
  // Handle a signal.
  // TODO: Make this a signal_monitor_client_actor
  caf::reacts_to<atom::signal, int>>::unwrap;

} // namespace vast::system

// -- type announcements -------------------------------------------------------

CAF_BEGIN_TYPE_ID_BLOCK(vast_actors, caf::id_block::vast_atoms::end)

  VAST_ADD_TYPE_ID((std::filesystem::path))

  VAST_ADD_TYPE_ID((vast::system::accountant_actor))
  VAST_ADD_TYPE_ID((vast::system::active_indexer_actor))
  VAST_ADD_TYPE_ID((vast::system::active_partition_actor))
  VAST_ADD_TYPE_ID((vast::system::analyzer_plugin_actor))
  VAST_ADD_TYPE_ID((vast::system::archive_actor))
  VAST_ADD_TYPE_ID((vast::system::disk_monitor_actor))
  VAST_ADD_TYPE_ID((vast::system::evaluator_actor))
  VAST_ADD_TYPE_ID((vast::system::exporter_actor))
  VAST_ADD_TYPE_ID((vast::system::filesystem_actor))
  VAST_ADD_TYPE_ID((vast::system::flush_listener_actor))
  VAST_ADD_TYPE_ID((vast::system::importer_actor))
  VAST_ADD_TYPE_ID((vast::system::index_actor))
  VAST_ADD_TYPE_ID((vast::system::index_client_actor))
  VAST_ADD_TYPE_ID((vast::system::indexer_actor))
  VAST_ADD_TYPE_ID((vast::system::node_actor))
  VAST_ADD_TYPE_ID((vast::system::partition_actor))
  VAST_ADD_TYPE_ID((vast::system::query_map))
  VAST_ADD_TYPE_ID((vast::system::query_supervisor_actor))
  VAST_ADD_TYPE_ID((vast::system::query_supervisor_master_actor))
  VAST_ADD_TYPE_ID((vast::system::receiver_actor<vast::table_slice>) )
  VAST_ADD_TYPE_ID((vast::system::status_client_actor))
  VAST_ADD_TYPE_ID((vast::system::stream_sink_actor<vast::table_slice>) )
  VAST_ADD_TYPE_ID(
    (vast::system::stream_sink_actor<vast::table_slice, std::string>) )
  VAST_ADD_TYPE_ID((vast::system::type_registry_actor))

CAF_END_TYPE_ID_BLOCK(vast_actors)

// Used in the interface of the meta_index actor.
// We can't provide a meaningful implementation of `inspect()` for a shared_ptr,
// so so we add these as `UNSAFE_MESSAGE_TYPE` to assure caf that they will
// never be sent over the network.
#define vast_uuid_synopsis_map std::map<vast::uuid, vast::partition_synopsis>
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<vast_uuid_synopsis_map>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<vast::partition_synopsis>)
#undef vast_uuid_synopsis_map

#undef VAST_ADD_TYPE_ID
