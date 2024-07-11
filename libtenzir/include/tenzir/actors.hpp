//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http_api.hpp"

#include <caf/inspector_access.hpp>
#include <caf/io/fwd.hpp>

#include <filesystem>

#define TENZIR_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(tenzir_actors, type)

namespace tenzir {

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
  using unwrap_as_broker = caf::io::typed_broker<Fs...>;
};

/// The STREAM SINK actor interface.
/// @tparam Unit The stream unit.
/// @tparam Args... Additional parameters passed using
/// `caf::stream_source::add_outbound_path`.
template <class Unit, class... Args>
using stream_sink_actor = typename typed_actor_fwd<
  // Add a new source.
  auto(caf::stream<Unit>, Args...)
    ->caf::result<caf::inbound_stream_slot<Unit>>>::unwrap;

/// The FLUSH LISTENER actor interface.
using flush_listener_actor = typed_actor_fwd<
  // Reacts to the requested flush message.
  auto(atom::flush)->caf::result<void>>::unwrap;

/// The RECEIVER SINK actor interface.
/// This can be used to avoid defining an opaque alias for a single-handler
/// interface.
/// @tparam T The type of first parameter of the message handler the the actor
///           handle must implement.
/// @tparam Ts... The types of additional parameters for the message handler.
template <class T, class... Ts>
using receiver_actor = typename typed_actor_fwd<
  // Add a new source.
  auto(T, Ts...)->caf::result<void>>::unwrap;

/// The STATUS CLIENT actor interface.
using status_client_actor = typed_actor_fwd<
  // Reply to a status request from the NODE.
  auto(atom::status, status_verbosity, duration)->caf::result<record>>::unwrap;

/// The TERMINATION HANDLER actor interface.
using termination_handler_actor = typed_actor_fwd<
  // Receive a signal from the reflector.
  auto(atom::signal, int)->caf::result<void>>::unwrap;

/// The SIGNAL REFLECTOR actor interface.
using signal_reflector_actor = typed_actor_fwd<
  // Receive a signal from the listener.
  auto(atom::internal, atom::signal, int)->caf::result<void>,
  // Subscribe to one or more signals.
  auto(atom::subscribe)->caf::result<void>>::unwrap;

/// The STORE actor interface.
using store_actor = typed_actor_fwd<
  // Handles an extraction for the given expression.
  // TODO: It's a bit weird that the store plugin implementation needs to
  // implement query handling. It may be better to have an API that exposes
  // an mmapped view of the contained table slices; or to provide an opaque
  // callback that the store can use for that.
  auto(atom::query, query_context)->caf::result<uint64_t>,
  // TODO: Replace usage of `atom::erase` with `query::erase` in call sites.
  auto(atom::erase, ids)->caf::result<uint64_t>>::unwrap;

/// Passive store default implementation actor interface.
using default_passive_store_actor = typed_actor_fwd<
  // Proceed with a previously received `extract` query.
  auto(atom::internal, atom::extract, uuid)->caf::result<void>,
  // Proceed with a previously received `count` query.
  auto(atom::internal, atom::count, uuid)->caf::result<void>>
  // Based on the store_actor interface.
  ::extend_with<store_actor>::unwrap;

/// The STORE BUILDER actor interface.
using store_builder_actor
  = typed_actor_fwd<auto(atom::persist)->caf::result<resource>>
  // Conform to the protocol of the STORE actor.
  ::extend_with<store_actor>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// Active store default implementation actor interface.
using default_active_store_actor = typed_actor_fwd<
  // Proceed with a previously received `extract` query.
  auto(atom::internal, atom::extract, uuid)->caf::result<void>,
  // Proceed with a previously received `count` query.
  auto(atom::internal, atom::count, uuid)->caf::result<void>>
  // Based on the store_builder_actor interface.
  ::extend_with<store_builder_actor>::unwrap;

/// The PARTITION actor interface.
using partition_actor = typed_actor_fwd<
  // Evaluate the given expression and send the matching events to the receiver.
  auto(atom::query, query_context)->caf::result<uint64_t>,
  // Delete the whole partition from disk.
  auto(atom::erase)->caf::result<atom::done>>
  // Conform to the procol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The EVALUATOR actor interface.
using evaluator_actor = typed_actor_fwd<
  // Evaluates the expression and responds with matching ids.
  auto(atom::run)->caf::result<ids>>::unwrap;

/// The INDEXER actor interface.
using indexer_actor = typed_actor_fwd<
  // Returns the ids for the given predicate.
  auto(atom::evaluate, curried_predicate)->caf::result<ids>,
  // Requests the INDEXER to shut down.
  auto(atom::shutdown)->caf::result<void>>::unwrap;

/// The ACTIVE INDEXER actor interface.
using active_indexer_actor = typed_actor_fwd<
  // Hooks into the table slice stream.
  auto(caf::stream<table_slice>)
    ->caf::result<caf::inbound_stream_slot<table_slice>>,
  // Finalizes the ACTIVE INDEXER into a chunk, which containes an INDEXER.
  auto(atom::snapshot)->caf::result<chunk_ptr>>
  // Conform the the INDEXER ACTOR interface.
  ::extend_with<indexer_actor>
  // Conform to the procol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The PARTITION CREATION LISTENER actor interface.
using partition_creation_listener_actor = typed_actor_fwd<
  auto(atom::update, partition_synopsis_pair)->caf::result<void>,
  auto(atom::update, std::vector<partition_synopsis_pair>)
    ->caf::result<void>>::unwrap;

/// The CATALOG actor interface.
using catalog_actor = typed_actor_fwd<
  // Reinitialize the catalog from a set of partition synopses. Used at
  // startup, so the map is expected to be huge and we use a shared_ptr
  // to be sure it's not accidentally copied.
  auto(atom::merge,
       std::shared_ptr<std::unordered_map<uuid, partition_synopsis_ptr>>)
    ->caf::result<atom::ok>,
  // Merge a set of partition synopses.
  auto(atom::merge, std::vector<partition_synopsis_pair>)->caf::result<atom::ok>,
  // Get *ALL* partition synopses stored in the catalog, optionally filtered
  // with an expression to filter the candidate set.
  // Note that this returns a pointer into the catalog's internal data
  // structures, which is inherently unsafe to transfer between processes. The
  // data pointed to must not be mutated. Functionality that depends on this
  // should instead be moved inside of the catalog itself.
  auto(atom::get)->caf::result<std::vector<partition_synopsis_pair>>,
  auto(atom::get, expression)->caf::result<std::vector<partition_synopsis_pair>>,
  // Erase a single partition synopsis.
  auto(atom::erase, uuid)->caf::result<atom::ok>,
  // Atomatically replace a set of partititon synopses with another.
  auto(atom::replace, std::vector<uuid>, std::vector<partition_synopsis_pair>)
    ->caf::result<atom::ok>,
  // Return the candidate partitions per type for a query.
  auto(atom::candidates, tenzir::query_context)
    ->caf::result<catalog_lookup_result>,
  // Retrieves information about a partition with a given UUID.
  auto(atom::get, uuid)->caf::result<partition_info>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface of an IMPORTER actor.
using importer_actor = typed_actor_fwd<
  // Add a new sink.
  auto(stream_sink_actor<table_slice>)->caf::result<void>,
  // Register a FLUSH LISTENER actor.
  auto(atom::subscribe, atom::flush, flush_listener_actor)->caf::result<void>,
  // Register a subscriber for table slices.
  auto(atom::subscribe, receiver_actor<table_slice>, bool internal)
    ->caf::result<void>,
  // Push buffered slices downstream to make the data available.
  auto(atom::flush)->caf::result<void>,
  // Import a batch of data.
  auto(table_slice)->caf::result<void>>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the STREAM SINK actor for table slices with a
  // description.
  ::extend_with<stream_sink_actor<table_slice, std::string>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The INDEX actor interface.
using index_actor = typed_actor_fwd<
  // Triggered when the INDEX finished querying a PARTITION.
  auto(atom::done, uuid)->caf::result<void>,
  // Subscribes a FLUSH LISTENER to the INDEX.
  auto(atom::subscribe, atom::flush, flush_listener_actor)->caf::result<void>,
  // Subscribes a PARTITION CREATION LISTENER to the INDEX.
  auto(atom::subscribe, atom::create, partition_creation_listener_actor,
       send_initial_dbstate)
    ->caf::result<void>,
  // Evaluates a query, ie. sends matching events to the caller.
  auto(atom::evaluate, query_context)->caf::result<query_cursor>,
  // Resolves a query to its candidate partitions per type.
  // TODO: Expose the catalog as a system component so this
  // handler can go directly to the catalog.
  auto(atom::resolve, expression)->caf::result<catalog_lookup_result>,
  // Queries PARTITION actors for a given query id.
  auto(atom::query, uuid, uint32_t)->caf::result<void>,
  // Erases the given partition from the INDEX.
  auto(atom::erase, uuid)->caf::result<atom::done>,
  // Erases the given set of partitions from the INDEX.
  auto(atom::erase, std::vector<uuid>)->caf::result<atom::done>,
  // Applies the given pipelineation to the partition.
  // When keep_original_partition is yes: merges the transformed partitions with
  // the original ones and returns the new partition infos. When
  // keep_original_partition is no: does an in-place pipeline keeping the old
  // ids, and makes new partitions preserving them.
  auto(atom::apply, pipeline, std::vector<tenzir::partition_info>,
       keep_original_partition)
    ->caf::result<std::vector<partition_info>>,
  // Decomissions all active partitions, effectively flushing them to disk.
  auto(atom::flush)->caf::result<void>>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The DISK MONITOR actor interface.
using disk_monitor_actor = typed_actor_fwd<
  // Checks the monitoring requirements.
  auto(atom::ping)->caf::result<void>,
  // Purge events as required for the monitoring requirements.
  auto(atom::erase)->caf::result<void>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface for file system I/O. The filesystem actor implementation
/// must interpret all operations that contain paths *relative* to its own
/// root directory.
using filesystem_actor = typed_actor_fwd<
  // Writes a chunk of data to a given path. Creates intermediate directories
  // if needed.
  auto(atom::write, std::filesystem::path, chunk_ptr)->caf::result<atom::ok>,
  // Reads a chunk of data from a given path and returns the chunk.
  auto(atom::read, std::filesystem::path)->caf::result<chunk_ptr>,
  // Reads all files from the given directories and for each directory returns
  // its structure as a record. Directories are modeled as nested records and
  // their content as a 'blob'. Nonexisting paths are returned as empty records.
  auto(atom::read, atom::recursive, std::vector<std::filesystem::path>)
    ->caf::result<std::vector<record>>,
  // Moves a file on the fielsystem.
  auto(atom::move, std::filesystem::path, std::filesystem::path)
    ->caf::result<atom::done>,
  // Moves a file on the fielsystem.
  auto(atom::move,
       std::vector<std::pair<std::filesystem::path, std::filesystem::path>>)
    ->caf::result<atom::done>,
  // Memory-maps a file.
  auto(atom::mmap, std::filesystem::path)->caf::result<chunk_ptr>,
  // Deletes a file.
  auto(atom::erase, std::filesystem::path)->caf::result<atom::done>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface of a PARTITION TRANSFORMER actor.
using partition_transformer_actor = typed_actor_fwd<
  // Persist the transformed partitions and return the generated
  // partition synopses.
  auto(atom::persist)->caf::result<std::vector<partition_synopsis_pair>>,
  // INTERNAL: Continuation handler for `atom::done`.
  auto(atom::internal, atom::resume, atom::done)->caf::result<void>>
  // extract_query_context API
  ::extend_with<receiver_actor<table_slice>>
  // Receive a completion signal for the input stream.
  ::extend_with<receiver_actor<atom::done>>::unwrap;

/// The interface of an ACTIVE PARTITION actor.
using active_partition_actor = typed_actor_fwd<
  auto(atom::subscribe, atom::flush, flush_listener_actor)->caf::result<void>,
  // Persists the active partition at the specified path.
  auto(atom::persist, std::filesystem::path, std::filesystem::path)
    ->caf::result<partition_synopsis_ptr>,
  // INTERNAL: A repeatedly called continuation of the persist request.
  auto(atom::internal, atom::persist, atom::resume)->caf::result<void>>
  // Conform to the protocol of the STREAM SINK actor for table slices.
  ::extend_with<stream_sink_actor<table_slice>>
  // Conform to the protocol of the PARTITION actor.
  ::extend_with<partition_actor>::unwrap;

/// The interface of a REST HANDLER actor.
using rest_handler_actor = typed_actor_fwd<
  // Receive an incoming HTTP request.
  auto(atom::http_request, uint64_t, tenzir::record)
    ->caf::result<rest_response>>::unwrap;

/// The interface of a COMPONENT PLUGIN actor.
using component_plugin_actor = typed_actor_fwd<
  // Conform to the protocol of the STATUS CLIENT actor.
  >::extend_with<status_client_actor>::unwrap;

/// The interface of a SOURCE actor.
using source_actor = typed_actor_fwd<
  // Retrieve the currently used module of the SOURCE.
  auto(atom::get, atom::module)->caf::result<module>,
  // Update the currently used module of the SOURCE.
  auto(atom::put, module)->caf::result<void>,
  // Update the expression used for filtering data in the SOURCE.
  auto(atom::normalize, expression)->caf::result<void>,
  // Set up a new stream sink for the generated data.
  auto(stream_sink_actor<table_slice, std::string>)->caf::result<void>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface of a DATAGRAM SOURCE actor.
using datagram_source_actor = typed_actor_fwd<
  // Reacts to datagram messages.
  auto(caf::io::new_datagram_msg)->caf::result<void>>
  // Conform to the protocol of the SOURCE actor.
  ::extend_with<source_actor>::unwrap_as_broker;

using exec_node_sink_actor = typed_actor_fwd<
  // Push events.
  auto(atom::push, table_slice events)->caf::result<void>,
  // Push bytes.
  auto(atom::push, chunk_ptr bytes)->caf::result<void>>::unwrap;

/// The interface of a EXEC NODE actor.
using exec_node_actor = typed_actor_fwd<
  // Resume the internal event loop.
  auto(atom::internal, atom::run)->caf::result<void>,
  // Start an execution node. Returns after the operator has yielded for the
  // first time.
  auto(atom::start, std::vector<caf::actor> all_previous)->caf::result<void>,
  // Pause the execution node. No-op if it was already paused.
  auto(atom::pause)->caf::result<void>,
  // Resume the execution node. No-op if it was not paused.
  auto(atom::resume)->caf::result<void>,
  // Emit a diagnostic through the exec node.
  auto(diagnostic diag)->caf::result<void>,
  // Uodate demand.
  auto(atom::pull, exec_node_sink_actor sink, uint64_t batch_size)
    ->caf::result<void>>
  // Source.
  ::extend_with<exec_node_sink_actor>::unwrap;

/// The interface of the METRICS RECEIVER actor.
using metrics_receiver_actor = typed_actor_fwd<
  // Register a custom metric type for the metrics of an operator.
  auto(uint64_t op_index, uint64_t metric_index, type)->caf::result<void>,
  // Receive custom metrics of an operator.
  auto(uint64_t op_index, uint64_t metric_index, record)->caf::result<void>,
  // Receive the standard execution node metrics.
  auto(operator_metric)->caf::result<void>>::unwrap;

/// The interface of the NODE actor.
using node_actor = typed_actor_fwd<
  // Execute a REST endpoint on this node.
  // Note that nodes connected via CAF trust each other completely,
  // so this skips all authorization and access control mechanisms
  // that come with HTTP(s).
  auto(atom::proxy, http_request_description, std::string)
    ->caf::result<rest_response>,
  // Retrieve components by their label from the component registry.
  auto(atom::get, atom::label, std::vector<std::string>)
    ->caf::result<std::vector<caf::actor>>,
  // Retrieve the version of the process running the NODE.
  auto(atom::get, atom::version)->caf::result<record>,
  // Spawn a set of execution nodes for a given pipeline. Does not start the
  // execution nodes.
  auto(atom::spawn, operator_box, operator_type, receiver_actor<diagnostic>,
       metrics_receiver_actor, int index, bool is_hidden)
    ->caf::result<exec_node_actor>>::unwrap;

/// The interface of a PIPELINE EXECUTOR actor.
using pipeline_executor_actor = typed_actor_fwd<
  // Execute a pipeline, returning the result asynchronously. This must be
  // called at most once per executor.
  auto(atom::start)->caf::result<void>,
  // Pause the pipeline execution. No-op if it was already paused. Must not be
  // called before the pipeline was started.
  auto(atom::pause)->caf::result<void>,
  // Resume the pipeline execution. No-op if it was not paused.
  auto(atom::resume)->caf::result<void>>::unwrap;

/// The interface of a PACKAGE LISTENER actor.
// Listeners are notified by the package manager in the following order:
//  1. context_manager component
//  2. pipeline_manager component
//  3. other subscribers (tbd)
using package_listener_actor = typed_actor_fwd<
  // Add a new package.
  auto(atom::package, atom::add, package)->caf::result<void>,
  // Remove all pipelines from a package.
  auto(atom::package, atom::remove, std::string)->caf::result<void>,
  // Send the list of packages that were found on disk
  // during startup. Listeners should use this information
  // to purge left-over state from packages that were
  // removed in the meantime.
  auto(atom::package, atom::start, std::vector<std::string>)
    ->caf::result<void>>::unwrap;

using terminator_actor = typed_actor_fwd<
  // Shut down the given actors.
  auto(atom::shutdown, std::vector<caf::actor>)->caf::result<atom::done>>::unwrap;

using connector_actor = typed_actor_fwd<
  // Retrieve the handle to a remote node actor.
  auto(atom::connect, connect_request)->caf::result<node_actor>>::unwrap;

} // namespace tenzir

// -- type announcements -------------------------------------------------------

CAF_BEGIN_TYPE_ID_BLOCK(tenzir_actors, caf::id_block::tenzir_atoms::end)

  TENZIR_ADD_TYPE_ID((std::filesystem::path))
  TENZIR_ADD_TYPE_ID(
    (std::vector<std::pair<std::filesystem::path, std::filesystem::path>>))
  TENZIR_ADD_TYPE_ID(
    (std::vector<
      std::tuple<tenzir::exec_node_actor, tenzir::operator_type, std::string>>))

  TENZIR_ADD_TYPE_ID((tenzir::active_indexer_actor))
  TENZIR_ADD_TYPE_ID((tenzir::active_partition_actor))
  TENZIR_ADD_TYPE_ID((tenzir::catalog_actor))
  TENZIR_ADD_TYPE_ID((tenzir::default_active_store_actor))
  TENZIR_ADD_TYPE_ID((tenzir::default_passive_store_actor))
  TENZIR_ADD_TYPE_ID((tenzir::disk_monitor_actor))
  TENZIR_ADD_TYPE_ID((tenzir::evaluator_actor))
  TENZIR_ADD_TYPE_ID((tenzir::exec_node_actor))
  TENZIR_ADD_TYPE_ID((tenzir::exec_node_sink_actor))
  TENZIR_ADD_TYPE_ID((tenzir::filesystem_actor))
  TENZIR_ADD_TYPE_ID((tenzir::flush_listener_actor))
  TENZIR_ADD_TYPE_ID((tenzir::importer_actor))
  TENZIR_ADD_TYPE_ID((tenzir::index_actor))
  TENZIR_ADD_TYPE_ID((tenzir::indexer_actor))
  TENZIR_ADD_TYPE_ID((tenzir::metrics_receiver_actor))
  TENZIR_ADD_TYPE_ID((tenzir::node_actor))
  TENZIR_ADD_TYPE_ID((tenzir::partition_actor))
  TENZIR_ADD_TYPE_ID((tenzir::partition_creation_listener_actor))
  TENZIR_ADD_TYPE_ID((tenzir::receiver_actor<tenzir::atom::done>))
  TENZIR_ADD_TYPE_ID((tenzir::receiver_actor<tenzir::diagnostic>))
  TENZIR_ADD_TYPE_ID((tenzir::receiver_actor<tenzir::table_slice>))
  TENZIR_ADD_TYPE_ID((tenzir::rest_handler_actor))
  TENZIR_ADD_TYPE_ID((tenzir::status_client_actor))
  TENZIR_ADD_TYPE_ID((tenzir::stream_sink_actor<tenzir::table_slice>))
  TENZIR_ADD_TYPE_ID(
    (tenzir::stream_sink_actor<tenzir::table_slice, std::string>))

CAF_END_TYPE_ID_BLOCK(tenzir_actors)

// Used in the interface of the catalog actor.
// We can't provide a meaningful implementation of `inspect()` for a shared_ptr,
// so so we add these as `UNSAFE_MESSAGE_TYPE` to assure caf that they will
// never be sent over the network.
#define tenzir_uuid_synopsis_map                                               \
  std::unordered_map<tenzir::uuid, tenzir::partition_synopsis_ptr>
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::shared_ptr<tenzir_uuid_synopsis_map>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::partition_synopsis_ptr)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::partition_synopsis_pair)
#undef tenzir_uuid_synopsis_map

#undef TENZIR_ADD_TYPE_ID
