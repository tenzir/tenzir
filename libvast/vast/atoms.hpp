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

#define VAST_CAF_ATOM_ALIAS(name)                                              \
  using name = caf::name##_atom;                                               \
  [[maybe_unused]] constexpr inline auto name##_v = caf::name##_atom_v;

#define VAST_ADD_ATOM(name, text)                                              \
  CAF_ADD_ATOM(vast_atoms, vast::atom, name, text)

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

CAF_BEGIN_TYPE_ID_BLOCK(vast_atoms, caf::id_block::vast_types::end)

  // Generic atoms.
  VAST_ADD_ATOM(accept, "accept")
  VAST_ADD_ATOM(announce, "announce")
  VAST_ADD_ATOM(batch, "batch")
  VAST_ADD_ATOM(config, "config")
  VAST_ADD_ATOM(continuous, "continuous")
  VAST_ADD_ATOM(cpu, "cpu")
  VAST_ADD_ATOM(count, "count")
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
  VAST_ADD_ATOM(intel, "intel")
  VAST_ADD_ATOM(internal, "internal")
  VAST_ADD_ATOM(key, "key")
  VAST_ADD_ATOM(label, "label")
  VAST_ADD_ATOM(limit, "limit")
  VAST_ADD_ATOM(link, "link")
  VAST_ADD_ATOM(list, "list")
  VAST_ADD_ATOM(load, "load")
  VAST_ADD_ATOM(merge, "merge")
  VAST_ADD_ATOM(mmap, "mmap")
  VAST_ADD_ATOM(peer, "peer")
  VAST_ADD_ATOM(persist, "persist")
  VAST_ADD_ATOM(ping, "ping")
  VAST_ADD_ATOM(plugin, "plugin")
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

#undef VAST_CAF_ATOM_ALIAS
#undef VAST_ADD_ATOM
