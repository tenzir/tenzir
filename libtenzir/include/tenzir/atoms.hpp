//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#define TENZIR_CAF_ATOM_ALIAS(name)                                            \
  using name = caf::name##_atom;                                               \
  [[maybe_unused]] constexpr inline auto name##_v = caf::name##_atom_v;

#define TENZIR_ADD_ATOM(name, text)                                            \
  CAF_ADD_ATOM(tenzir_atoms, tenzir::atom, name, text)

// -- tenzir::atom ---------------------------------------------------------------

namespace tenzir::atom {

// Inherited from CAF.
TENZIR_CAF_ATOM_ALIAS(add)
TENZIR_CAF_ATOM_ALIAS(connect)
TENZIR_CAF_ATOM_ALIAS(flush)
TENZIR_CAF_ATOM_ALIAS(get)
TENZIR_CAF_ATOM_ALIAS(ok)
TENZIR_CAF_ATOM_ALIAS(publish)
TENZIR_CAF_ATOM_ALIAS(put)
TENZIR_CAF_ATOM_ALIAS(spawn)
TENZIR_CAF_ATOM_ALIAS(subscribe)

} // namespace tenzir::atom

CAF_BEGIN_TYPE_ID_BLOCK(tenzir_atoms, caf::id_block::tenzir_types::end)

  // Generic atoms.
  TENZIR_ADD_ATOM(accept, "accept")
  TENZIR_ADD_ATOM(announce, "announce")
  TENZIR_ADD_ATOM(apply, "apply")
  TENZIR_ADD_ATOM(candidates, "candidates")
  TENZIR_ADD_ATOM(config, "config")
  TENZIR_ADD_ATOM(count, "count")
  TENZIR_ADD_ATOM(create, "create")
  TENZIR_ADD_ATOM(done, "done")
  TENZIR_ADD_ATOM(erase, "erase")
  TENZIR_ADD_ATOM(evaluate, "evaluate")
  TENZIR_ADD_ATOM(extract, "extract")
  TENZIR_ADD_ATOM(http_request, "hrequest")
  TENZIR_ADD_ATOM(internal, "internal")
  TENZIR_ADD_ATOM(label, "label")
  TENZIR_ADD_ATOM(limit, "limit")
  TENZIR_ADD_ATOM(list, "list")
  TENZIR_ADD_ATOM(load, "load")
  TENZIR_ADD_ATOM(merge, "merge")
  TENZIR_ADD_ATOM(metrics, "metrics")
  TENZIR_ADD_ATOM(mmap, "mmap")
  TENZIR_ADD_ATOM(module, "module")
  TENZIR_ADD_ATOM(move, "move")
  TENZIR_ADD_ATOM(next, "next")
  TENZIR_ADD_ATOM(normalize, "normalize")
  TENZIR_ADD_ATOM(package, "package")
  TENZIR_ADD_ATOM(pause, "pause")
  TENZIR_ADD_ATOM(persist, "persist")
  TENZIR_ADD_ATOM(ping, "ping")
  TENZIR_ADD_ATOM(plugin, "plugin")
  TENZIR_ADD_ATOM(provision, "provision")
  TENZIR_ADD_ATOM(proxy, "proxy")
  TENZIR_ADD_ATOM(pull, "pull")
  TENZIR_ADD_ATOM(push, "push")
  TENZIR_ADD_ATOM(query, "query")
  TENZIR_ADD_ATOM(read, "read")
  TENZIR_ADD_ATOM(rebuild, "rebuild")
  TENZIR_ADD_ATOM(recursive, "recursive")
  TENZIR_ADD_ATOM(replace, "replace")
  TENZIR_ADD_ATOM(request, "request")
  TENZIR_ADD_ATOM(reserve, "reserve")
  TENZIR_ADD_ATOM(reset, "reset")
  TENZIR_ADD_ATOM(resolve, "resolve")
  TENZIR_ADD_ATOM(resume, "resume")
  TENZIR_ADD_ATOM(remove, "remove")
  TENZIR_ADD_ATOM(run, "run")
  TENZIR_ADD_ATOM(schedule, "schedule")
  TENZIR_ADD_ATOM(set, "set")
  TENZIR_ADD_ATOM(shutdown, "shutdown")
  TENZIR_ADD_ATOM(signal, "signal")
  TENZIR_ADD_ATOM(snapshot, "snapshot")
  TENZIR_ADD_ATOM(start, "start")
  TENZIR_ADD_ATOM(statistics, "statistics")
  TENZIR_ADD_ATOM(status, "status")
  TENZIR_ADD_ATOM(stop, "stop")
  TENZIR_ADD_ATOM(supervise, "supervise")
  TENZIR_ADD_ATOM(taxonomies, "taxonomies")
  TENZIR_ADD_ATOM(telemetry, "telemetry")
  TENZIR_ADD_ATOM(timeout, "timeout")
  TENZIR_ADD_ATOM(update, "update")
  TENZIR_ADD_ATOM(version, "version")
  TENZIR_ADD_ATOM(wakeup, "wakeup")
  TENZIR_ADD_ATOM(write, "write")

  // Actor role atoms.
  TENZIR_ADD_ATOM(importer, "importer")
  TENZIR_ADD_ATOM(sink, "sink")
  TENZIR_ADD_ATOM(worker, "worker")

  // Attribute atoms.
  TENZIR_ADD_ATOM(type, "type")

CAF_END_TYPE_ID_BLOCK(tenzir_atoms)

#undef TENZIR_CAF_ATOM_ALIAS
#undef TENZIR_ADD_ATOM
