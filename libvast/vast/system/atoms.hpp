#ifndef VAST_ACTOR_ATOMS_HPP
#define VAST_ACTOR_ATOMS_HPP

#include <caf/atom.hpp>

namespace vast {

using caf::atom;
using caf::atom_constant;

// Inherited from CAF
using caf::add_atom;
using caf::delete_atom;
using caf::flush_atom;
using caf::get_atom;
using caf::ok_atom;
using caf::put_atom;
using caf::join_atom;
using caf::leave_atom;

// Generic
using accept_atom = atom_constant<atom("accept")>;
using announce_atom = atom_constant<atom("announce")>;
using batch_atom = atom_constant<atom("batch")>;
using connect_atom = atom_constant<atom("connect")>;
using continuous_atom = atom_constant<atom("continuous")>;
using data_atom = atom_constant<atom("data")>;
using disable_atom = atom_constant<atom("disable")>;
using disconnect_atom = atom_constant<atom("disconnect")>;
using done_atom = atom_constant<atom("done")>;
using empty_atom = atom_constant<atom("empty")>;
using enable_atom = atom_constant<atom("enable")>;
using exists_atom = atom_constant<atom("exists")>;
using extract_atom = atom_constant<atom("extract")>;
using historical_atom = atom_constant<atom("historical")>;
using id_atom = atom_constant<atom("id")>;
using key_atom = atom_constant<atom("key")>;
using limit_atom = atom_constant<atom("limit")>;
using link_atom = atom_constant<atom("link")>;
using list_atom = atom_constant<atom("list")>;
using load_atom = atom_constant<atom("load")>;
using overload_atom = atom_constant<atom("overload")>;
using peer_atom = atom_constant<atom("peer")>;
using persist_atom = atom_constant<atom("persist")>;
using ping_atom = atom_constant<atom("ping")>;
using pong_atom = atom_constant<atom("pong")>;
using progress_atom = atom_constant<atom("progress")>;
using prompt_atom = atom_constant<atom("prompt")>;
using publish_atom = atom_constant<atom("publish")>;
using query_atom = atom_constant<atom("query")>;
using read_atom = atom_constant<atom("read")>;
using replicate_atom = atom_constant<atom("replicate")>;
using request_atom = atom_constant<atom("request")>;
using response_atom = atom_constant<atom("response")>;
using run_atom = atom_constant<atom("run")>;
using schema_atom = atom_constant<atom("schema")>;
using set_atom = atom_constant<atom("set")>;
using signal_atom = atom_constant<atom("signal")>;
using spawn_atom = atom_constant<atom("spawn")>;
using start_atom = atom_constant<atom("start")>;
using state_atom = atom_constant<atom("state")>;
using stop_atom = atom_constant<atom("stop")>;
using store_atom = atom_constant<atom("store")>;
using submit_atom = atom_constant<atom("submit")>;
using subscribe_atom = atom_constant<atom("subscribe")>;
using underload_atom = atom_constant<atom("underload")>;
using value_atom = atom_constant<atom("value")>;
using write_atom = atom_constant<atom("write")>;

// Actor roles
using actor_atom = atom_constant<atom("actor")>;
using accountant_atom = atom_constant<atom("accountant")>;
using candidate_atom = atom_constant<atom("candidate")>;
using controller_atom = atom_constant<atom("controller")>;
using deflector_atom = atom_constant<atom("deflector")>;
using identifier_atom = atom_constant<atom("identifier")>;
using index_atom = atom_constant<atom("index")>;
using follower_atom = atom_constant<atom("follower")>;
using leader_atom = atom_constant<atom("leader")>;
using receiver_atom = atom_constant<atom("receiver")>;
using sink_atom = atom_constant<atom("sink")>;
using source_atom = atom_constant<atom("source")>;
using subscriber_atom = atom_constant<atom("subscriber")>;
using supervisor_atom = atom_constant<atom("supervisor")>;
using search_atom = atom_constant<atom("search")>;
using tracker_atom = atom_constant<atom("tracker")>;
using upstream_atom = atom_constant<atom("upstream")>;
using worker_atom = atom_constant<atom("worker")>;
using workers_atom = atom_constant<atom("workers")>;

} // namespace vast

#endif
