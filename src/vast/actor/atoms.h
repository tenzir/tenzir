#ifndef VAST_ACTOR_ATOMS_H
#define VAST_ACTOR_ATOMS_H

namespace vast {

// Generic
using accept_atom = caf::atom_constant<caf::atom("accept")>;
using add_atom = caf::atom_constant<caf::atom("add")>;
using batch_atom = caf::atom_constant<caf::atom("batch")>;
using data_atom = caf::atom_constant<caf::atom("data")>;
using delete_atom = caf::atom_constant<caf::atom("delete")>;
using done_atom = caf::atom_constant<caf::atom("done")>;
using empty_atom = caf::atom_constant<caf::atom("empty")>;
using extract_atom = caf::atom_constant<caf::atom("extract")>;
using flush_atom = caf::atom_constant<caf::atom("flush")>;
using id_atom = caf::atom_constant<caf::atom("id")>;
using key_atom = caf::atom_constant<caf::atom("key")>;
using limit_atom = caf::atom_constant<caf::atom("limit")>;
using link_atom = caf::atom_constant<caf::atom("link")>;
using load_atom = caf::atom_constant<caf::atom("load")>;
using ping_atom = caf::atom_constant<caf::atom("ping")>;
using pong_atom = caf::atom_constant<caf::atom("pong")>;
using progress_atom = caf::atom_constant<caf::atom("progress")>;
using prompt_atom = caf::atom_constant<caf::atom("prompt")>;
using query_atom = caf::atom_constant<caf::atom("query")>;
using read_atom = caf::atom_constant<caf::atom("read")>;
using request_atom = caf::atom_constant<caf::atom("request")>;
using response_atom = caf::atom_constant<caf::atom("response")>;
using run_atom = caf::atom_constant<caf::atom("run")>;
using set_atom = caf::atom_constant<caf::atom("set")>;
using signal_atom = caf::atom_constant<caf::atom("signal")>;
using start_atom = caf::atom_constant<caf::atom("start")>;
using stop_atom = caf::atom_constant<caf::atom("stop")>;
using store_atom = caf::atom_constant<caf::atom("store")>;
using submit_atom = caf::atom_constant<caf::atom("submit")>;
using value_atom = caf::atom_constant<caf::atom("value")>;
using write_atom = caf::atom_constant<caf::atom("write")>;

// Roles
using accountant_atom = caf::atom_constant<caf::atom("accountant")>;
using archive_atom = caf::atom_constant<caf::atom("archive")>;
using identifier_atom = caf::atom_constant<caf::atom("identifier")>;
using index_atom = caf::atom_constant<caf::atom("index")>;
using receiver_atom = caf::atom_constant<caf::atom("receiver")>;
using sink_atom = caf::atom_constant<caf::atom("sink")>;
using source_atom = caf::atom_constant<caf::atom("source")>;
using subscriber_atom = caf::atom_constant<caf::atom("subscriber")>;
using supervisor_atom = caf::atom_constant<caf::atom("supervisor")>;
using search_atom = caf::atom_constant<caf::atom("search")>;
using tracker_atom = caf::atom_constant<caf::atom("tracker")>;
using worker_atom = caf::atom_constant<caf::atom("worker")>;
using workers_atom = caf::atom_constant<caf::atom("workers")>;

// Profiler
using cpu_atom = caf::atom_constant<caf::atom("cpu")>;
using heap_atom = caf::atom_constant<caf::atom("heap")>;
using perftools_atom = caf::atom_constant<caf::atom("perftools")>;
using rusage_atom = caf::atom_constant<caf::atom("rusage")>;

} // namespace vast

#endif
