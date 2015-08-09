#ifndef VAST_ACTOR_ATOMS_H
#define VAST_ACTOR_ATOMS_H

#include <caf/atom.hpp>

namespace vast {

// Inherited from CAF
using caf::delete_atom;
using caf::flush_atom;
using caf::get_atom;
using caf::ok_atom;
using caf::put_atom;
using caf::join_atom;
using caf::leave_atom;
using caf::sys_atom;

// Generic
using accept_atom = caf::atom_constant<caf::atom("accept")>;
using add_atom = caf::atom_constant<caf::atom("add")>;
using batch_atom = caf::atom_constant<caf::atom("batch")>;
using connect_atom = caf::atom_constant<caf::atom("connect")>;
using continuous_atom = caf::atom_constant<caf::atom("continuous")>;
using data_atom = caf::atom_constant<caf::atom("data")>;
using disable_atom = caf::atom_constant<caf::atom("disable")>;
using disconnect_atom = caf::atom_constant<caf::atom("disconnect")>;
using done_atom = caf::atom_constant<caf::atom("done")>;
using empty_atom = caf::atom_constant<caf::atom("empty")>;
using enable_atom = caf::atom_constant<caf::atom("enable")>;
using exists_atom = caf::atom_constant<caf::atom("exists")>;
using extract_atom = caf::atom_constant<caf::atom("extract")>;
using historical_atom = caf::atom_constant<caf::atom("historical")>;
using id_atom = caf::atom_constant<caf::atom("id")>;
using key_atom = caf::atom_constant<caf::atom("key")>;
using limit_atom = caf::atom_constant<caf::atom("limit")>;
using link_atom = caf::atom_constant<caf::atom("link")>;
using list_atom = caf::atom_constant<caf::atom("list")>;
using load_atom = caf::atom_constant<caf::atom("load")>;
using overload_atom = caf::atom_constant<caf::atom("overload")>;
using peer_atom = caf::atom_constant<caf::atom("peer")>;
using ping_atom = caf::atom_constant<caf::atom("ping")>;
using pong_atom = caf::atom_constant<caf::atom("pong")>;
using progress_atom = caf::atom_constant<caf::atom("progress")>;
using prompt_atom = caf::atom_constant<caf::atom("prompt")>;
using publish_atom = caf::atom_constant<caf::atom("publish")>;
using query_atom = caf::atom_constant<caf::atom("query")>;
using read_atom = caf::atom_constant<caf::atom("read")>;
using request_atom = caf::atom_constant<caf::atom("request")>;
using response_atom = caf::atom_constant<caf::atom("response")>;
using run_atom = caf::atom_constant<caf::atom("run")>;
using schema_atom = caf::atom_constant<caf::atom("schema")>;
using set_atom = caf::atom_constant<caf::atom("set")>;
using signal_atom = caf::atom_constant<caf::atom("signal")>;
using spawn_atom = caf::atom_constant<caf::atom("spawn")>;
using start_atom = caf::atom_constant<caf::atom("start")>;
using stop_atom = caf::atom_constant<caf::atom("stop")>;
using store_atom = caf::atom_constant<caf::atom("store")>;
using submit_atom = caf::atom_constant<caf::atom("submit")>;
using subscribe_atom = caf::atom_constant<caf::atom("subscribe")>;
using underload_atom = caf::atom_constant<caf::atom("underload")>;
using value_atom = caf::atom_constant<caf::atom("value")>;
using write_atom = caf::atom_constant<caf::atom("write")>;

// Actor roles
using actor_atom = caf::atom_constant<caf::atom("actor")>;
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
using upstream_atom = caf::atom_constant<caf::atom("upstream")>;
using worker_atom = caf::atom_constant<caf::atom("worker")>;
using workers_atom = caf::atom_constant<caf::atom("workers")>;

} // namespace vast

#endif
