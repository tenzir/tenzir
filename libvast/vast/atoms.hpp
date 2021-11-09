//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

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
VAST_CAF_ATOM_ALIAS(spawn)
VAST_CAF_ATOM_ALIAS(subscribe)

} // namespace vast::atom

CAF_BEGIN_TYPE_ID_BLOCK(vast_atoms, caf::id_block::vast_types::end)

  // Generic atoms.
  VAST_ADD_ATOM(announce, "announce")
  VAST_ADD_ATOM(apply, "apply")
  VAST_ADD_ATOM(candidates, "candidates")
  VAST_ADD_ATOM(config, "config")
  VAST_ADD_ATOM(done, "done")
  VAST_ADD_ATOM(erase, "erase")
  VAST_ADD_ATOM(extract, "extract")
  VAST_ADD_ATOM(internal, "internal")
  VAST_ADD_ATOM(label, "label")
  VAST_ADD_ATOM(limit, "limit")
  VAST_ADD_ATOM(list, "list")
  VAST_ADD_ATOM(load, "load")
  VAST_ADD_ATOM(merge, "merge")
  VAST_ADD_ATOM(mmap, "mmap")
  VAST_ADD_ATOM(persist, "persist")
  VAST_ADD_ATOM(ping, "ping")
  VAST_ADD_ATOM(plugin, "plugin")
  VAST_ADD_ATOM(provision, "provision")
  VAST_ADD_ATOM(read, "read")
  VAST_ADD_ATOM(reserve, "reserve")
  VAST_ADD_ATOM(resolve, "resolve")
  VAST_ADD_ATOM(resume, "resume")
  VAST_ADD_ATOM(replace, "replace")
  VAST_ADD_ATOM(run, "run")
  VAST_ADD_ATOM(schema, "schema")
  VAST_ADD_ATOM(shutdown, "shutdown")
  VAST_ADD_ATOM(signal, "signal")
  VAST_ADD_ATOM(snapshot, "snapshot")
  VAST_ADD_ATOM(start, "start")
  VAST_ADD_ATOM(statistics, "statistics")
  VAST_ADD_ATOM(status, "status")
  VAST_ADD_ATOM(stop, "stop")
  VAST_ADD_ATOM(supervise, "supervise")
  VAST_ADD_ATOM(taxonomies, "taxonomies")
  VAST_ADD_ATOM(telemetry, "telemetry")
  VAST_ADD_ATOM(version, "version")
  VAST_ADD_ATOM(wakeup, "wakeup")
  VAST_ADD_ATOM(write, "write")

  // Actor role atoms.
  VAST_ADD_ATOM(importer, "importer")
  VAST_ADD_ATOM(sink, "sink")
  VAST_ADD_ATOM(worker, "worker")

  // Attribute atoms.
  VAST_ADD_ATOM(type, "type")

CAF_END_TYPE_ID_BLOCK(vast_atoms)

#undef VAST_CAF_ATOM_ALIAS
#undef VAST_ADD_ATOM
