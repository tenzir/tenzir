/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/config.hpp"

#include <caf/atom.hpp>
#include <caf/config.hpp>
#include <caf/fwd.hpp>
#include <caf/intrusive_cow_ptr.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/replies_to.hpp>
#include <caf/type_id.hpp>

#include <cstdint>
#include <vector>

// -- define helper macros -----------------------------------------------------

#define VAST_CAF_ATOM_ALIAS(name)                                              \
  using name = caf::name##_atom;                                               \
  [[maybe_unused]] constexpr inline auto name##_v = caf::name##_atom_v;

#define VAST_ADD_ATOM(name, text)                                              \
  CAF_ADD_ATOM(vast_atoms, vast::atom, name, text)

#define VAST_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(vast_types, type)

// -- vast::atom ---------------------------------------------------------------

namespace vast::atom {

// Inherited from CAF.
VAST_CAF_ATOM_ALIAS(add)
VAST_CAF_ATOM_ALIAS(connect)
VAST_CAF_ATOM_ALIAS(flush)
VAST_CAF_ATOM_ALIAS(get)
VAST_CAF_ATOM_ALIAS(ok)
VAST_CAF_ATOM_ALIAS(put)
VAST_CAF_ATOM_ALIAS(join)
VAST_CAF_ATOM_ALIAS(leave)
VAST_CAF_ATOM_ALIAS(spawn)
VAST_CAF_ATOM_ALIAS(subscribe)

} // namespace vast::atom

CAF_BEGIN_TYPE_ID_BLOCK(vast_atoms, caf::first_custom_type_id)

  // Generic atoms.
  VAST_ADD_ATOM(accept, "accept")
  VAST_ADD_ATOM(announce, "announce")
  VAST_ADD_ATOM(batch, "batch")
  VAST_ADD_ATOM(config, "config")
  VAST_ADD_ATOM(continuous, "continuous")
  VAST_ADD_ATOM(cpu, "cpu")
  VAST_ADD_ATOM(data, "data")
  VAST_ADD_ATOM(disable, "disable")
  VAST_ADD_ATOM(disconnect, "disconnect")
  VAST_ADD_ATOM(done, "done")
  VAST_ADD_ATOM(election, "election")
  VAST_ADD_ATOM(empty, "empty")
  VAST_ADD_ATOM(enable, "enable")
  VAST_ADD_ATOM(erase, "erase")
  VAST_ADD_ATOM(exists, "exists")
  VAST_ADD_ATOM(extract, "extract")
  VAST_ADD_ATOM(filesystem, "filesystem")
  VAST_ADD_ATOM(heap, "heap")
  VAST_ADD_ATOM(heartbeat, "heartbeat")
  VAST_ADD_ATOM(historical, "historical")
  VAST_ADD_ATOM(id, "id")
  VAST_ADD_ATOM(key, "key")
  VAST_ADD_ATOM(label, "label")
  VAST_ADD_ATOM(limit, "limit")
  VAST_ADD_ATOM(link, "link")
  VAST_ADD_ATOM(list, "list")
  VAST_ADD_ATOM(load, "load")
  VAST_ADD_ATOM(mmap, "mmap")
  VAST_ADD_ATOM(peer, "peer")
  VAST_ADD_ATOM(persist, "persist")
  VAST_ADD_ATOM(ping, "ping")
  VAST_ADD_ATOM(pong, "pong")
  VAST_ADD_ATOM(progress, "progress")
  VAST_ADD_ATOM(prompt, "prompt")
  VAST_ADD_ATOM(provision, "provision")
  VAST_ADD_ATOM(publish, "publish")
  VAST_ADD_ATOM(query, "query")
  VAST_ADD_ATOM(read, "read")
  VAST_ADD_ATOM(replace, "replace")
  VAST_ADD_ATOM(replicate, "replicate")
  VAST_ADD_ATOM(request, "request")
  VAST_ADD_ATOM(resolve, "resolve")
  VAST_ADD_ATOM(response, "response")
  VAST_ADD_ATOM(resume, "resume")
  VAST_ADD_ATOM(run, "run")
  VAST_ADD_ATOM(schema, "schema")
  VAST_ADD_ATOM(seed, "seed")
  VAST_ADD_ATOM(set, "set")
  VAST_ADD_ATOM(shutdown, "shutdown")
  VAST_ADD_ATOM(signal, "signal")
  VAST_ADD_ATOM(snapshot, "snapshot")
  VAST_ADD_ATOM(start, "start")
  VAST_ADD_ATOM(state, "state")
  VAST_ADD_ATOM(statistics, "statistics")
  VAST_ADD_ATOM(status, "status")
  VAST_ADD_ATOM(stop, "stop")
  VAST_ADD_ATOM(store, "store")
  VAST_ADD_ATOM(submit, "submit")
  VAST_ADD_ATOM(taxonomies, "taxonomies")
  VAST_ADD_ATOM(telemetry, "telemetry")
  VAST_ADD_ATOM(try_put, "tryPut")
  VAST_ADD_ATOM(unload, "unload")
  VAST_ADD_ATOM(value, "value")
  VAST_ADD_ATOM(version, "version")
  VAST_ADD_ATOM(wakeup, "wakeup")
  VAST_ADD_ATOM(write, "write")

  // Actor role atoms.
  VAST_ADD_ATOM(accountant, "accountant")
  VAST_ADD_ATOM(archive, "archive")
  VAST_ADD_ATOM(candidate, "candidate")
  VAST_ADD_ATOM(eraser, "eraser")
  VAST_ADD_ATOM(exporter, "exporter")
  VAST_ADD_ATOM(follower, "follower")
  VAST_ADD_ATOM(identifier, "identifier")
  VAST_ADD_ATOM(importer, "importer")
  VAST_ADD_ATOM(index, "index")
  VAST_ADD_ATOM(leader, "leader")
  VAST_ADD_ATOM(receiver, "receiver")
  VAST_ADD_ATOM(search, "search")
  VAST_ADD_ATOM(sink, "sink")
  VAST_ADD_ATOM(source, "source")
  VAST_ADD_ATOM(subscriber, "subscriber")
  VAST_ADD_ATOM(supervisor, "supervisor")
  VAST_ADD_ATOM(tracker, "tracker")
  VAST_ADD_ATOM(worker, "worker")

  // Attribute atoms.
  VAST_ADD_ATOM(field, "field")
  VAST_ADD_ATOM(timestamp, "timestamp")
  VAST_ADD_ATOM(type, "type")

CAF_END_TYPE_ID_BLOCK(vast_atoms)

#if VAST_ENABLE_ARROW

// -- arrow --------------------------------------------------------------------

// Note that this can also be achieved by including <arrow/type_fwd.h>, but
// that header is a fair bit more expensive than forward declaring just the
// types we need forward-declared here. If this ever diverges between Arrow
// versions, consider switching to including that file.

namespace arrow {

class Array;
class ArrayBuilder;
class DataType;
class MemoryPool;
class RecordBatch;
class Schema;

} // namespace arrow

#endif // VAST_ENABLE_ARROW

// -- caf ----------------------------------------------------------------------

// These are missing from <caf/fwd.hpp>.

namespace caf {

template <class In>
class inbound_stream_slot;

} // namespace caf

// -- vast::fbs ----------------------------------------------------------------

namespace vast::fbs {

struct FlatTableSlice;
struct TableSlice;

} // namespace vast::fbs

namespace vast::fbs::table_slice::msgpack {

struct v0;

} // namespace vast::fbs::table_slice::msgpack

namespace vast::fbs::table_slice::arrow {

struct v0;

} // namespace vast::fbs::table_slice::arrow

// -- vast ---------------------------------------------------------------------

namespace vast {

class abstract_type;
class address;
class arrow_table_slice_builder;
class bitmap;
class chunk;
class column_index;
class command;
class data;
class ewah_bitstream;
class expression;
class json;
class meta_index;
class msgpack_table_slice_builder;
class path;
class pattern;
class plugin;
class plugin_ptr;
class schema;
class segment;
class segment_builder;
class segment_store;
class store;
class subnet;
class synopsis;
class table_slice;
class table_slice_builder;
class table_slice_column;
class type;
class uuid;
class value_index;

struct address_type;
struct alias_type;
struct attribute_extractor;
struct bool_type;
struct concept_;
struct conjunction;
struct count_type;
struct curried_predicate;
struct data_extractor;
struct disjunction;
struct duration_type;
struct enumeration_type;
struct field_extractor;
struct flow;
struct integer_type;
struct invocation;
struct list_type;
struct map_type;
struct model;
struct negation;
struct none_type;
struct offset;
struct partition_synopsis;
struct pattern_type;
struct predicate;
struct qualified_record_field;
struct real_type;
struct record_type;
struct status;
struct string_type;
struct subnet_type;
struct taxonomies;
struct time_type;
struct type_extractor;
struct type_set;

enum arithmetic_operator : uint8_t;
enum bool_operator : uint8_t;
enum relational_operator : uint8_t;

enum class ec : uint8_t;
enum class query_options : uint32_t;
enum class table_slice_encoding : uint8_t;

template <class>
class arrow_table_slice;

template <class>
class msgpack_table_slice;

template <class>
class scope_linked;

void intrusive_ptr_add_ref(const table_slice_builder*);
void intrusive_ptr_release(const table_slice_builder*);

using chunk_ptr = caf::intrusive_ptr<chunk>;
using column_index_ptr = std::unique_ptr<column_index>;
using ids = bitmap; // temporary; until we have a real type for 'ids'
using synopsis_ptr = std::unique_ptr<synopsis>;
using table_slice_builder_ptr = caf::intrusive_ptr<table_slice_builder>;
using value_index_ptr = std::unique_ptr<value_index>;

/// A duration in time with nanosecond resolution.
using duration = caf::timespan;

/// An absolute point in time with nanosecond resolution. It is capable to
/// represent +/- 292 years around the UNIX epoch.
using time = caf::timestamp;

/// Signed integer type.
using integer = int64_t;

/// Unsigned integer type.
using count = uint64_t;

/// Floating point type.
using real = double;

/// Enumeration type.
using enumeration = uint8_t;

} // namespace vast

// -- vast::format -------------------------------------------------------------

namespace vast::format {

class writer;

using writer_ptr = std::unique_ptr<writer>;

} // namespace vast::format

// -- vast::system -------------------------------------------------------------

namespace vast::system {

class application;
class configuration;
class default_application;
class export_command;
class node_command;
class pcap_writer_command;
class remote_command;
class sink_command;
class start_command;

struct accountant_config;
struct active_partition_state;
struct passive_partition_state;
struct index_state;
struct index_statistics;
struct layout_statistics;
struct component_map;
struct component_map_entry;
struct component_state;
struct component_state_map;
struct data_point;
struct measurement;
struct node_state;
struct performance_sample;
struct query_status;
struct query_status;
struct spawn_arguments;

enum class status_verbosity;

using node_actor = caf::stateful_actor<node_state>;
using performance_report = std::vector<performance_sample>;
using report = std::vector<data_point>;

} // namespace vast::system

// -- typed actors -------------------------------------------------------------

namespace vast::system {

/// Helper utility that enables extending typed actor forwartd declarations
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

/// A flush listener actor listens for flushes.
using flush_listener_actor = typed_actor_fwd<
  // Reacts to the requested flush message.
  caf::reacts_to<atom::flush>>::unwrap;

/// The ARCHIVE CLIENT actor interface.
using archive_client_actor = typed_actor_fwd<
  // An ARCHIVE CLIENT receives table slices from the ARCHIVE for partial
  // query hits.
  caf::reacts_to<table_slice>,
  // An ARCHIVE CLIENT receives (done, error) when the query finished.
  caf::reacts_to<atom::done, caf::error>>::unwrap;

/// The PARTITION CLIENT actor interface.
using partition_client_actor = typed_actor_fwd<
  // The client sends an expression to the partition and receives several sets
  // of ids followed by a final `atom::done` which as sent as response to the
  // expression. This interface provides the callback for the middle part of
  // this sequence.
  caf::reacts_to<ids>>::unwrap;

/// The INDEX CLIENT actor interface.
using index_client_actor = typed_actor_fwd<
  // Receives done from the INDEX when the query finished.
  caf::reacts_to<atom::done>>
  // Receives ids from the INDEX for partial query hits.
  ::extend_with<partition_client_actor>::unwrap;

/// The PARTITION actor interface.
using partition_actor = typed_actor_fwd<
  // Evaluate the given expression, returning the relevant evaluation triples.
  // TODO: Passing the `partition_client_actor` here is an historical artifact,
  // a cleaner API would be to just return the evaluated `vast::ids`.
  caf::replies_to<expression, partition_client_actor>::with<atom::done>>::unwrap;

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
  // Re-evaluates the expression and relays new hits to the PARTITION CLIENT.
  caf::replies_to<partition_client_actor>::with<atom::done>>::unwrap;

/// The STATUS CLIENT actor interface.
using status_client_actor = typed_actor_fwd<
  // Reply to a status request from the NODE.
  caf::replies_to<atom::status, status_verbosity>::with< //
    caf::dictionary<caf::config_value>>>::unwrap;

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
  ::extend_with<indexer_actor>::unwrap;

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

/// The INDEX actor interface.
using index_actor = typed_actor_fwd<
  // Triggered when the INDEX finished querying a PARTITION.
  caf::reacts_to<atom::done, uuid>,
  // Hooks into the table slice stream.
  caf::replies_to<caf::stream<table_slice>>::with<
    caf::inbound_stream_slot<table_slice>>,
  // Registers the ARCHIVE with the ACCOUNTANT.
  caf::reacts_to<accountant_actor>,
  // Subscribes a FLUSH LISTENER to the INDEX.
  caf::reacts_to<atom::subscribe, atom::flush, flush_listener_actor>,
  // Evaluatates an expression.
  caf::reacts_to<expression>,
  // Queries PARTITION actors for a given query id.
  caf::reacts_to<uuid, uint32_t>,
  // Replaces the SYNOPSIS of the PARTITION witht he given partition id.
  caf::reacts_to<atom::replace, uuid, std::shared_ptr<partition_synopsis>>,
  // Erases the given events from the INDEX, and returns their ids.
  caf::replies_to<atom::erase, uuid>::with<ids>>
  // Conform to the protocol of the QUERY SUPERVISOR MASTER actor.
  ::extend_with<query_supervisor_master_actor>
  // Conform to the procol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

using archive_actor = typed_actor_fwd<
  // Hook into the table slice stream.
  caf::replies_to<caf::stream<table_slice>>::with< //
    caf::inbound_stream_slot<table_slice>>,
  // Register an exporter actor.
  // TODO: This should probably take an archive_client_actor.
  caf::reacts_to<atom::exporter, caf::actor>,
  // Registers the ARCHIVE with the ACCOUNTANT.
  caf::reacts_to<accountant_actor>,
  // Starts handling a query for the given ids.
  // TODO: This forwards to the second handler; this should probably be
  // removed, as it is not type safe.
  caf::reacts_to<ids>,
  // Starts handling a query for the given ids.
  caf::reacts_to<ids, archive_client_actor>,
  // Handles a query for the given ids, and sends the table slices back to the
  // ARCHIVE CLIENT.
  caf::reacts_to<ids, archive_client_actor, uint64_t>,
  // The internal telemetry loop of the ARCHIVE.
  caf::reacts_to<atom::telemetry>,
  // Erase the events with the given ids.
  caf::replies_to<atom::erase, ids>::with< //
    atom::done>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The TYPE REGISTRY actor interface.
using type_registry_actor = typed_actor_fwd<
  // The internal telemetry loop of the TYPE REGISTRY.
  caf::reacts_to<atom::telemetry>,
  // Hooks into the table slice stream.
  caf::replies_to<caf::stream<table_slice>>::with< //
    caf::inbound_stream_slot<table_slice>>,
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
  caf::replies_to<atom::write, path, chunk_ptr>::with< //
    atom::ok>,
  // Reads a chunk of data from a given path and returns the chunk.
  caf::replies_to<atom::read, path>::with< //
    chunk_ptr>,
  // Memory-maps a file.
  caf::replies_to<atom::mmap, path>::with< //
    chunk_ptr>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

/// The interface of an ACTIVE PARTITION actor.
using active_partition_actor = typed_actor_fwd<
  // Hooks into the table slice stream.
  caf::replies_to<caf::stream<table_slice>>::with< //
    caf::inbound_stream_slot<table_slice>>,
  // Persists the active partition at the specified path.
  caf::replies_to<atom::persist, path, index_actor>::with< //
    atom::ok>,
  // A repeatedly called continuation of the persist request.
  caf::reacts_to<atom::persist, atom::resume>>
  // Conform to the protocol of the PARTITION.
  ::extend_with<partition_actor>::unwrap;

using exporter_actor = typed_actor_fwd<
  // Request extraction of all events.
  caf::reacts_to<atom::extract>,
  // Request extraction of the given number of events.
  caf::reacts_to<atom::extract, uint64_t>,
  // Register the ACCOUNTANT actor.
  caf::reacts_to<accountant_actor>,
  // Register the ARCHIVE actor.
  caf::reacts_to<archive_actor>,
  // Register the INDEX actor.
  caf::reacts_to<index_actor>,
  // Register the SINK actor.
  caf::reacts_to<atom::sink, caf::actor>,
  // Register a list of IMPORTER actors.
  caf::reacts_to<atom::importer, std::vector<caf::actor>>,
  // Execute previously registered query.
  caf::reacts_to<atom::run>,
  // Register a STATISTICS SUBSCRIBER actor.
  caf::reacts_to<atom::statistics, caf::actor>,
  // Hook into the table slice stream.
  // TODO: This should probably be modeled as a IMPORTER CLIENT actor.
  caf::replies_to<caf::stream<table_slice>>::with< //
    caf::inbound_stream_slot<table_slice>>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>
  // Conform to the protocol of the ARCHIVE CLIENT actor.
  ::extend_with<archive_client_actor>
  // Conform to the protocol of the INDEX CLIENT actor.
  ::extend_with<index_client_actor>::unwrap;

} // namespace vast::system

// -- type announcements -------------------------------------------------------

CAF_BEGIN_TYPE_ID_BLOCK(vast_types, caf::id_block::vast_atoms_last_type_id + 1)

  VAST_ADD_TYPE_ID((vast::attribute_extractor))
  VAST_ADD_TYPE_ID((vast::bitmap))
  VAST_ADD_TYPE_ID((vast::conjunction))
  VAST_ADD_TYPE_ID((vast::curried_predicate))
  VAST_ADD_TYPE_ID((vast::data))
  VAST_ADD_TYPE_ID((vast::data_extractor))
  VAST_ADD_TYPE_ID((vast::disjunction))
  VAST_ADD_TYPE_ID((vast::ec))
  VAST_ADD_TYPE_ID((vast::expression))
  VAST_ADD_TYPE_ID((vast::field_extractor))
  VAST_ADD_TYPE_ID((vast::invocation))
  VAST_ADD_TYPE_ID((vast::negation))
  VAST_ADD_TYPE_ID((vast::path))
  VAST_ADD_TYPE_ID((vast::predicate))
  VAST_ADD_TYPE_ID((vast::query_options))
  VAST_ADD_TYPE_ID((vast::relational_operator))
  VAST_ADD_TYPE_ID((vast::schema))
  VAST_ADD_TYPE_ID((vast::table_slice))
  VAST_ADD_TYPE_ID((vast::type))
  VAST_ADD_TYPE_ID((vast::type_extractor))
  VAST_ADD_TYPE_ID((vast::type_set))
  VAST_ADD_TYPE_ID((vast::uuid))

  VAST_ADD_TYPE_ID((vast::system::accountant_actor))
  VAST_ADD_TYPE_ID((vast::system::active_indexer_actor))
  VAST_ADD_TYPE_ID((vast::system::active_partition_actor))
  VAST_ADD_TYPE_ID((vast::system::archive_actor))
  VAST_ADD_TYPE_ID((vast::system::archive_client_actor))
  VAST_ADD_TYPE_ID((vast::system::disk_monitor_actor))
  VAST_ADD_TYPE_ID((vast::system::evaluator_actor))
  VAST_ADD_TYPE_ID((vast::system::exporter_actor))
  VAST_ADD_TYPE_ID((vast::system::filesystem_actor))
  VAST_ADD_TYPE_ID((vast::system::flush_listener_actor))
  VAST_ADD_TYPE_ID((vast::system::index_actor))
  VAST_ADD_TYPE_ID((vast::system::index_client_actor))
  VAST_ADD_TYPE_ID((vast::system::indexer_actor))
  VAST_ADD_TYPE_ID((vast::system::partition_actor))
  VAST_ADD_TYPE_ID((vast::system::partition_client_actor))
  VAST_ADD_TYPE_ID((vast::system::performance_report))
  VAST_ADD_TYPE_ID((vast::system::query_map))
  VAST_ADD_TYPE_ID((vast::system::query_status))
  VAST_ADD_TYPE_ID((vast::system::query_supervisor_actor))
  VAST_ADD_TYPE_ID((vast::system::query_supervisor_master_actor))
  VAST_ADD_TYPE_ID((vast::system::report))
  VAST_ADD_TYPE_ID((vast::system::status_client_actor))
  VAST_ADD_TYPE_ID((vast::system::status_verbosity))
  VAST_ADD_TYPE_ID((vast::system::type_registry_actor))

  VAST_ADD_TYPE_ID((std::vector<uint32_t>) )
  VAST_ADD_TYPE_ID((std::vector<vast::table_slice>) )

  VAST_ADD_TYPE_ID((caf::stream<vast::table_slice>) )

CAF_END_TYPE_ID_BLOCK(vast_types)

  // -- undefine helper macros ---------------------------------------------------

#undef VAST_CAF_ATOM_ALIAS
#undef VAST_ADD_ATOM
#undef VAST_ADD_TYPE_ID
