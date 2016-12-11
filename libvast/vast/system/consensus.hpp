#ifndef VAST_SYSTEM_CONSENSUS_HPP
#define VAST_SYSTEM_CONSENSUS_HPP

#include <chrono>
#include <cstdint>
#include <deque>
#include <random>
#include <vector>
#include <unordered_map>

#include <caf/stateful_actor.hpp>

#include "vast/optional.hpp"

namespace vast {
namespace system {

/// The Raft consensus algorithm.
namespace raft {

/// The clock type for timeouts.
using clock = std::chrono::steady_clock;

/// The election timeout.
constexpr auto election_timeout = std::chrono::milliseconds(500);

/// The heartbeat period.
constexpr auto heartbeat_period = election_timeout / 2;

/// A type to uniquely represent a server in the system. An ID of 0 is invalid.
using server_id = uint64_t;

/// A monotonically increasing type to represent a term.
using term_type = uint64_t;

/// A monotonically increasing type to represent the offset into the log.
using index_type = uint64_t;

/// A server configuration.
using configuration = std::unordered_map<std::string, std::string>;

// -- log types ---------------------------------------------------------------

/// An entry in the replicated log.
struct log_entry {
  term_type term = 0;
  index_type index = 0;
  caf::message data;
};

template <class Inspector>
auto inspect(Inspector& f, log_entry& e) {
  return f(e.term, e.index, e.data);
};

// TODO: move to namespace detail
/// The interface to a Raft log, i.e., a sequence of log entries accessed
/// through indexes as defined in Raft. In particular, the first entry has
/// index 1. Index 0 is invalid.
class log_type {
public:
  using container_type = std::deque<log_entry>;
  using size_type = index_type;
  using value_type = log_entry;

  /// Retrieves the last index in the log.
  /// @returns The last log index.
  index_type last_index() const;

  /// Truncate all entries *after* a given index.
  /// @param index The index to truncate after.
  /// @returns The number of entries truncated.
  size_type truncate_after(size_type index);

  // -- container API --

  using iterator = container_type::iterator;
  using const_iterator = container_type::iterator;

  iterator begin();
  iterator end();

  /// Retrieves the number of entries in the log.
  /// @returns The number of the log entries.
  size_type size() const;

  /// Checks whether the log has no entries.
  /// @returns `true` iff the log has no entries.
  bool empty() const;

  /// Access the first entry.
  /// @returns A reference to the first entry in the log.
  /// @pre `!empty()`
  log_entry& front();

  /// Access the last entry.
  /// @returns A reference to the first last in the log.
  /// @pre `!empty()`
  log_entry& back();

  /// Accesses a log entry at a given index.
  /// @param i The index of the entry.
  /// @pre i > 0
  log_entry& operator[](size_type i);

  /// Appends an entry to the log.
  /// @param x The entry to append to the log.
  void push_back(const value_type& x);

private:
  container_type container_;
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
    std::vector<uint8_t> bytes;
    bool done;
  };

  struct response {
    term_type term;
    uint64_t bytes_stored;
  };
};

template <class Inspector>
auto inspect(Inspector& f, install_snapshot::request& r) {
  return f(r.term, r.leader_id, r.last_snapshot_index, r.byte_offset, r.bytes,
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

  /// The actor handle ro the peer.
  caf::actor peer;
};

/// The state for the consensus module.
struct server_state {
  // -- persistent state on all servers ---------------------------------------

  /// The unique ID of this server.
  server_id id = 0;

  /// A monotonically increasing number to denote the current term.
  term_type current_term = 0;

  /// Candidate that received our vote in current term.
  server_id voted_for = 0;

  /// The sequence of log entries applied to the state machine.
  log_type log;

  // -- volatile state on all servers -----------------------------------------

  /// Log index of last committed entry.
  index_type commit_index = 0;

  /// Log index of last entry applied to state machine.
  //index_type last_applied = 0;

  // -- volatile implementation details ---------------------------------------

  // The different states of a server.
  caf::behavior following;
  caf::behavior candidating;
  caf::behavior leading;

  // A handle to the leader, deduced from (accepted) AppendEntries messages.
  caf::actor leader;

  // All known peers.
  std::vector<peer_state> peers;

  // Flag that indicates whether we've kicked of the heartbeat loop.
  bool heartbeat_inflight = false;

  // The point in time when a follower should hold an election.
  clock::time_point election_time = clock::time_point::max();

  // A PRNG to generate random election timeouts.
  std::mt19937 prng;

  // Outstanding replication requests that have not yet been answered. Sorted
  // by log index and cleared as soon as commitIndex catches up.
  std::deque<std::pair<index_type, caf::response_promise>> pending;

  // Name of this actor (for logging purposes).
  const char* name = "raft";
};

/// Spawns a consensus module.
/// @param self The actor handle.
/// @param config The server configuration.
caf::behavior consensus(caf::stateful_actor<server_state>* self,
                        configuration config);

} // namespace raft
} // namespace system
} // namespace vast

#endif
