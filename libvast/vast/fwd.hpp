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
#include <caf/type_id.hpp>

#include <vector>

namespace vast {

// -- classes ------------------------------------------------------------------

class abstract_type;
class address;
class arrow_table_slice;
class arrow_table_slice_builder;
class bitmap;
class caf_table_slice;
class caf_table_slice_builder;
class chunk;
class column_index;
class command;
class data;
class ewah_bitstream;
class expression;
class json;
class meta_index;
class msgpack_table_slice;
class msgpack_table_slice_builder;
class path;
class pattern;
class port;
class schema;
class segment;
class segment_builder;
class segment_store;
class store;
class subnet;
class synopsis;
class table_slice;
class table_slice_builder;
class type;
class uuid;
class value_index;

namespace system {

class application;
class configuration;
class default_application;
class export_command;
class node_command;
class pcap_writer_command;
class remote_command;
class sink_command;
class start_command;

} // namespace system

// -- structs ------------------------------------------------------------------

struct address_type;
struct alias_type;
struct attribute_extractor;
struct bool_type;
struct conjunction;
struct count_type;
struct curried_predicate;
struct data_extractor;
struct disjunction;
struct duration_type;
struct enumeration_type;
struct flow;
struct integer_type;
struct invocation;
struct field_extractor;
struct map_type;
struct negation;
struct none_type;
struct offset;
struct pattern_type;
struct port_type;
struct predicate;
struct real_type;
struct record_type;
struct string_type;
struct status;
struct subnet_type;
struct time_type;
struct type_extractor;
struct list_type;

namespace system {

namespace v2 {

struct active_partition_state;
struct passive_partition_state;
struct index_state;
struct index_statistics;
struct layout_statistics;

} // namespace v2

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
struct type_set;

} // namespace system

// -- enums --------------------------------------------------------------------

enum arithmetic_operator : uint8_t;
enum bool_operator : uint8_t;
enum relational_operator : uint8_t;

// -- enum classes -------------------------------------------------------------

enum class ec : uint8_t;
enum class query_options : uint32_t;
enum class status_verbosity;

// -- aliases ------------------------------------------------------------------

namespace system {

using node_actor = caf::stateful_actor<node_state>;
using performance_report = std::vector<performance_sample>;
using report = std::vector<data_point>;

} // namespace system

// -- templates ----------------------------------------------------------------

template <class>
class scope_linked;

// -- free functions -----------------------------------------------------------

void intrusive_ptr_add_ref(const table_slice*);
void intrusive_ptr_release(const table_slice*);
table_slice* intrusive_cow_ptr_unshare(table_slice*&);

void intrusive_ptr_add_ref(const table_slice_builder*);
void intrusive_ptr_release(const table_slice_builder*);

// -- smart pointers -----------------------------------------------------------

using chunk_ptr = caf::intrusive_ptr<chunk>;
using column_index_ptr = std::unique_ptr<column_index>;
using synopsis_ptr = caf::intrusive_ptr<synopsis>;
using table_slice_builder_ptr = caf::intrusive_ptr<table_slice_builder>;
using table_slice_ptr = caf::intrusive_cow_ptr<table_slice>;
using value_index_ptr = std::unique_ptr<value_index>;

// -- miscellaneous ------------------------------------------------------------

using ids = bitmap; // temporary; until we have a real type for 'ids'

namespace atom {

#define VAST_CAF_ATOM_ALIAS(name)                                              \
  using name = caf::name##_atom;                                               \
  [[maybe_unused]] constexpr inline auto name##_v = caf::name##_atom_v;

// Inherited from CAF
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

#undef VAST_CAF_ATOM_ALIAS

} // namespace atom

} // namespace vast

#define VAST_ADD_ATOM(name, text) CAF_ADD_ATOM(vast, vast::atom, name, text)
#define VAST_ADD_TYPE_ID(type) CAF_ADD_TYPE_ID(vast, type)

CAF_BEGIN_TYPE_ID_BLOCK(vast, caf::first_custom_type_id)

  // -- generic atoms ------------------------------------------------------------

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
  VAST_ADD_ATOM(replicate, "replicate")
  VAST_ADD_ATOM(request, "request")
  VAST_ADD_ATOM(response, "response")
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
  VAST_ADD_ATOM(telemetry, "telemetry")
  VAST_ADD_ATOM(try_put, "tryPut")
  VAST_ADD_ATOM(unload, "unload")
  VAST_ADD_ATOM(value, "value")
  VAST_ADD_ATOM(write, "write")

  // -- actor role atoms ---------------------------------------------------------

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

  // -- attribute atoms ----------------------------------------------------------

  VAST_ADD_ATOM(timestamp, "timestamp")
  VAST_ADD_ATOM(type, "type")

  // -- type announcements -------------------------------------------------------

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
  VAST_ADD_TYPE_ID((vast::status_verbosity))
  VAST_ADD_TYPE_ID((vast::table_slice_ptr))
  VAST_ADD_TYPE_ID((vast::type))
  VAST_ADD_TYPE_ID((vast::type_extractor))
  VAST_ADD_TYPE_ID((vast::uuid))

  VAST_ADD_TYPE_ID((vast::system::performance_report))
  VAST_ADD_TYPE_ID((vast::system::query_status))
  VAST_ADD_TYPE_ID((vast::system::report))
  VAST_ADD_TYPE_ID((vast::system::type_set))

  VAST_ADD_TYPE_ID((std::vector<uint32_t>) )
  VAST_ADD_TYPE_ID((std::vector<vast::table_slice_ptr>) )

  VAST_ADD_TYPE_ID((caf::stream<vast::table_slice_ptr>) )

CAF_END_TYPE_ID_BLOCK(vast)

#undef VAST_ADD_ATOM
#undef VAST_ADD_TYPE_ID
