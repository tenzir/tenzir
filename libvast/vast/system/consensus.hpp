#ifndef VAST_SYSTEM_CONSENSUS_HPP
#define VAST_SYSTEM_CONSENSUS_HPP

#include <chrono>
#include <cstdint>
#include <deque>
#include <fstream>
#include <random>
#include <vector>
#include <unordered_map>

#include <caf/stateful_actor.hpp>

#include "vast/expected.hpp"
#include "vast/filesystem.hpp"
#include "vast/optional.hpp"

#include "vast/detail/mmapbuf.hpp"

namespace vast {
namespace system {

/// The Raft consensus algorithm.
///
/// This implementation of Raft treats state machine and consensus module as
/// independent pieces. The consensus module is passive and only reacts to
/// requests from the state machine. It does not initiate communication to the
/// state machine.
namespace raft {

/// The clock type for timeouts.
using clock = std::chrono::steady_clock;

/// The election timeout.
constexpr auto election_timeout = std::chrono::milliseconds(500);

/// The timeout when sending requests to other peers.
constexpr auto request_timeout = election_timeout * 4;

/// The heartbeat period.
constexpr auto heartbeat_period = election_timeout / 2;

/// A type to uniquely represent a server in the system. An ID of 0 is invalid.
using server_id = uint64_t;

/// A monotonically increasing type to represent a term.
using term_type = uint64_t;

/// A monotonically increasing type to represent the offset into the log.
using index_type = uint64_t;

// -- log ---------------------------------------------------------------------

/// An entry in the replicated log.
struct log_entry {
  term_type term = 0;
  index_type index = 0;
  std::vector<char> data;
};

template <class Inspector>
auto inspect(Inspector& f, log_entry& e) {
  return f(e.term, e.index, e.data);
};

/// A sequence of log entries accessed through monotonically increasing
/// indexes. The first entry has index 1. Index 0 is invalid. Mutable
/// operations do not return before they have been made persistent.
class log {
public:
  /// Constructs a log and attempts to read persistent state from the
  /// filesystem.
  /// @param dir The directory where the log stores persistent state.
  log(path dir);

  /// Retrieves the first log entry.
  /// @pre `!empty()`
  log_entry& first();

  /// Retrieves the first index in the log.
  index_type first_index() const;

  /// Retrieves the last log entry.
  /// @pre `!empty()`
  log_entry& last();

  /// Retrieves the last index in the log.
  index_type last_index() const;

  /// Truncates all entries *before* a given index.
  index_type truncate_before(index_type index);

  /// Truncates all entries *after* a given index.
  index_type truncate_after(index_type index);

  /// Accesses a log entry at a given index.
  log_entry& at(index_type i);

  /// Appends entries to the log.
  expected<void> append(std::vector<log_entry> xs);

  /// Checks whether the log is empty.
  bool empty() const;

  /// Returns the number of bytes the serialized entries in the log occupy.
  friend uint64_t bytes(log& l);

private:
  expected<void> persist_meta_data();

  expected<void> persist_entries();

  std::deque<log_entry> entries_;
  index_type start_ = 1;
  std::ofstream meta_file_;
  std::ofstream entries_file_;
  path dir_;
};

/// A snapshot covering log entries indices in *[1, L]* where *L* is the last
/// included index.
struct snapshot_header {
  uint32_t version = 1;
  index_type last_included_index;
  term_type last_included_term;
};

template <class Inspector>
auto inspect(Inspector& f, snapshot_header& ss) {
  return f(ss.version, ss.last_included_index, ss.last_included_term);
};

/// Statistics clients can request.
struct statistics {
  uint64_t log_entries;
  uint64_t log_bytes;
};

template <class Inspector>
auto inspect(Inspector& f, statistics& stats) {
  return f(stats.log_bytes, stats.log_entries);
};

// -- RPC/message types -------------------------------------------------------

/// The **RequestVote** RPC. Sent by candidates to gather votes.
struct request_vote {
  struct request {
    server_id candidate_id;
    term_type term;
    index_type last_log_index;
    term_type last_log_term;
  };

  struct response {
    term_type term;
    bool vote_granted;
  };
};

template <class Inspector>
auto inspect(Inspector& f, request_vote::request& r) {
  return f(r.candidate_id, r.term, r.last_log_index, r.last_log_term);
};

template <class Inspector>
auto inspect(Inspector& f, request_vote::response& r) {
  return f(r.term, r.vote_granted);
};

/// The **AppendEntries** RPC. Sent by leader to replicate log entries; also
/// used as heartbeat.
struct append_entries {
  struct request {
    term_type term;
    server_id leader_id;
    index_type prev_log_index;
    term_type prev_log_term;
    std::vector<log_entry> entries;
    index_type commit_index;
  };

  struct response {
    term_type term;
    index_type last_log_index;
    bool success;
  };
};

template <class Inspector>
auto inspect(Inspector& f, append_entries::request& r) {
  return f(r.term, r.leader_id, r.prev_log_index, r.prev_log_term,
           r.entries, r.commit_index);
};

template <class Inspector>
auto inspect(Inspector& f, append_entries::response& r) {
  return f(r.term, r.success);
};

/// The **InstallSnapshot** RPC.
struct install_snapshot {
  struct request {
    term_type term;
    server_id leader_id;
    index_type last_snapshot_index;
    uint64_t byte_offset;
    std::vector<char> data;
    bool done;
  };

  struct response {
    term_type term;
    uint64_t bytes_stored;
  };
};

template <class Inspector>
auto inspect(Inspector& f, install_snapshot::request& r) {
  return f(r.term, r.leader_id, r.last_snapshot_index, r.byte_offset, r.data,
           r.done);
};

template <class Inspector>
auto inspect(Inspector& f, install_snapshot::response& r) {
  return f(r.term, r.bytes_stored);
};

// -- actor state -------------------------------------------------------------

/// The state a server maintains for each peer.
struct peer_state {
  /// The peer ID.
  server_id id = 0;

  /// Index of the next entry to send.
  index_type next_index = 0;

  /// Index of the highest entry known to be replicated.
  index_type match_index = 0;

  /// Indicates whether we have a vote from this peer.
  bool have_vote = false;

  /// The index of the last log entry in the last snapshot.
  index_type last_snapshot_index = 0;

  /// A handle to the snapshot file in the form of a memory-mapped streambuffer.
  std::unique_ptr<detail::mmapbuf> snapshot;

  /// The actor handle to the peer.
  caf::actor peer;
};

/// The state for the consensus module.
struct server_state {
  // -- persistent state ------------------------------------------------------

  /// The unique ID of this server.
  server_id id = 0;

  /// A monotonically increasing number to denote the current term.
  term_type current_term = 0;

  /// Candidate that received our vote in current term.
  server_id voted_for = 0;

  /// The sequence of log entries applied to the state machine.
  std::unique_ptr<raft::log> log;

  // -- volatile state --------------------------------------------------------

  /// Log index of last committed entry.
  index_type commit_index = 0;

  /// The index of the last entry in the last snapshot.
  index_type last_snapshot_index = 0;

  /// The term of the last entry in the last snapshot.
  term_type last_snapshot_term = 0;

  /// The snapshot file when writing the snapshot to disk.
  std::ofstream snapshot;

  // -- volatile implementation details ---------------------------------------

  // The different states of a server.
  caf::behavior following;
  caf::behavior candidating;
  caf::behavior leading;

  // A handle to the leader, deduced from (accepted) AppendEntries messages.
  caf::actor leader;

  // A handle to the state machine such that it can asynchronously receive
  // entries from peers.
  caf::actor state_machine;

  // All known peers.
  std::vector<peer_state> peers;

  // Flag that indicates whether we've kicked of the heartbeat loop.
  bool heartbeat_inflight = false;

  // The point in time when a follower should hold an election.
  clock::time_point election_time = clock::time_point::max();

  // A PRNG to generate random election timeouts.
  std::mt19937 prng;

  /// The directory where to keep persistent state.
  path dir;

  // Name of this actor (for logging purposes).
  const char* name = "raft";
};

/// Spawns a consensus module.
/// @param self The actor handle.
/// @param dir The directory where to store persistent state.
caf::behavior consensus(caf::stateful_actor<server_state>* self, path dir);

} // namespace raft
} // namespace system
} // namespace vast

#endif
