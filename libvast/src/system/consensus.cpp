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

#include <caf/all.hpp>

#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/si_literals.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/consensus.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/string.hpp"

using namespace caf;

namespace vast {
namespace system {
namespace raft {

log::log(caf::actor_system& sys, path dir) : dir_{std::move(dir)}, sys_(sys) {
  auto meta_filename = dir_ / "meta";
  auto entries_filename = dir_ / "entries";
  if (exists(dir_)) {
    if (exists(meta_filename))
      if (!load(sys_, meta_filename, start_))
        die("failed to load raft log meta data");
    if (exists(entries_filename)) {
      std::ifstream entries{entries_filename.str(), std::ios::binary};
      while (entries.peek() != std::ifstream::traits_type::eof()) {
        std::vector<log_entry> xs;
        if (!load(sys_, entries, xs))
          die("failed to load raft log entries");
        std::move(xs.begin(), xs.end(), std::back_inserter(entries_));
      }
    }
  } else {
    if (!mkdir(dir_))
      die("failed to create raft log directory");
  }
}

log_entry& log::first() {
  VAST_ASSERT(!empty());
  return entries_.front();
}

index_type log::first_index() const {
  return start_;
}

log_entry& log::last() {
  VAST_ASSERT(!empty());
  return entries_.back();
}

index_type log::last_index() const {
  return start_ + entries_.size() - 1;
}

index_type log::truncate_before(index_type index) {
  if (index <= start_)
    return 0; // already truncated
  auto n = std::min(index_type{entries_.size()}, index - start_);
  if (n > 0) {
    entries_.erase(entries_.begin(), entries_.begin() + n);
    start_ += n;
    // Persists meta data and entries.
    if (!persist_meta_data())
      die("failed to persist log meta data");
    if (!persist_entries())
      die("failed to persist log entries");
  }
  return n;
}

index_type log::truncate_after(index_type index) {
  VAST_ASSERT(index >= start_);
  if (index > last_index())
    return 0;
  auto old_size = entries_.size();
  auto new_size = index - start_ + 1;
  VAST_ASSERT(new_size <= old_size);
  if (new_size < old_size) {
    entries_.resize(new_size);
    if (!persist_entries())
      die("failed to persist log entries");
  }
  return old_size - new_size;
}

log_entry& log::at(index_type i) {
  VAST_ASSERT(!empty());
  VAST_ASSERT(i >= start_ && i - start_ < entries_.size());
  return entries_[i - start_];
}

expected<void> log::append(std::vector<log_entry> xs) {
  // Allocate persistent state on first entry.
  if (!entries_file_.is_open()) {
    auto entries_filename = dir_ / "entries";
    auto flags = std::ios::app | std::ios::binary | std::ios::ate;
    entries_file_.open(entries_filename.str(), flags);
    if (!entries_file_)
      return make_error(ec::filesystem_error, "failed to open log entry file");
    if (!exists(dir_ / "meta")) {
      auto res = persist_meta_data();
      if (!res)
        return res;
    }
  }
  // Serialize the entries...
  auto res = save(sys_, entries_file_, xs);
  if (!res)
    return res;
  // ...and make them persistent...
  entries_file_.flush();
  if (!entries_file_)
    return make_error(ec::filesystem_error, "bad log entry file");
  // ...before keeping 'em.
  std::move(xs.begin(), xs.end(), std::back_inserter(entries_));
  return {};
}

bool log::empty() const {
  return entries_.empty();
}

uint64_t bytes(log& l) {
  auto pos = l.entries_file_.tellp();
  if (pos == -1)
    return 0;
  return static_cast<uint64_t>(pos);
}

expected<void> log::persist_meta_data() {
  return save(sys_, dir_ / "meta", start_);
}

expected<void> log::persist_entries() {
  entries_file_.close();
  return save(sys_, dir_ / "entries", entries_);
}

namespace {

template <class Actor>
bool is_follower(Actor* self) {
  return self->has_behavior() && self->current_behavior().as_behavior_impl()
    == self->state.following.as_behavior_impl();
}

template <class Actor>
bool is_candidate(Actor* self) {
  return self->has_behavior() && self->current_behavior().as_behavior_impl()
    == self->state.candidating.as_behavior_impl();
}

template <class Actor>
bool is_leader(Actor* self) {
  return self->has_behavior() && self->current_behavior().as_behavior_impl()
    == self->state.leading.as_behavior_impl();
}

template <class Actor>
term_type last_log_term(Actor* self) {
  if (self->state.log->empty())
    return self->state.last_snapshot_term;
  return self->state.log->last().term;
}

// prints the server's role (for logging purposes)
template <class Actor>
std::string role(Actor* self) {
  std::string result;
  if (is_follower(self))
    result = "follower";
  else if (is_candidate(self))
    result = "candidate";
  else if (is_leader(self))
    result = "leader";
  else
    result = "server";
  result += '#';
  result += std::to_string(self->state.id);
  return result;
}

template <class Actor>
expected<void> save_state(Actor* self) {
  auto res = save(self->system(), self->state.dir / "state", self->state.id,
                  self->state.current_term, self->state.voted_for);
  if (res)
    VAST_DEBUG(role(self), "saved persistent state: id =",
               self->state.id << ", current term =",
               self->state.current_term << ", voted for =",
               self->state.voted_for);
  return res;
}

template <class Actor>
expected<void> load_state(Actor* self) {
  auto res = load(self->system(), self->state.dir / "state", self->state.id,
                  self->state.current_term, self->state.voted_for);
  if (res)
    VAST_DEBUG(role(self), "loaded persistent state: id =",
               self->state.id << ", current term",
               self->state.current_term << ", voted for",
               self->state.voted_for);
  return res;
}

// Retrieves the peer state from a response handler.
template <class Actor>
peer_state* current_peer(Actor* self) {
  auto f = [=](auto& x) { return x.peer->address() == self->current_sender(); };
  auto p = std::find_if(self->state.peers.begin(), self->state.peers.end(), f);
  if (p == self->state.peers.end()) {
    VAST_WARNING(role(self), "ignores response from dead peer");
    return nullptr;
  }
  return &*p;
}

// Picks a election timeout uniformly at random from [T, T * 2], where T is the
// configured election timeout.
template <class Actor>
auto random_timeout(Actor* self) {
  using timeout_type = decltype(election_timeout);
  using timeout_rep = timeout_type::rep;
  using unif = std::uniform_int_distribution<timeout_rep>;
  unif dist{election_timeout.count(), election_timeout.count() * 2};
  return timeout_type{dist(self->state.prng)};
}

template <class Actor>
void reset_election_time(Actor* self) {
  auto timeout = random_timeout(self);
  VAST_DEBUG(role(self), "will start election in", timeout);
  self->state.election_time = clock::now() + timeout;
  self->delayed_send(self, timeout, election_atom::value);
}

// Saves a state machine snapshot that represents all the applied state up to a
// given index.
template <class Actor>
result<index_type> save_snapshot(Actor* self, index_type index,
                                 const std::vector<char>& snapshot) {
  VAST_DEBUG(role(self), "creates snapshot of indices [1,", index << ']');
  VAST_ASSERT(index > 0);
  if (index == self->state.last_snapshot_index)
    return make_error(ec::unspecified, "ignores request to take redundant "
                      "snapshot at index", index);
  if (index < self->state.log->first_index())
    return make_error(ec::unspecified, "ignores request to take snapshot at "
                      "index", index, "that is included in prior snapshot "
                      "at index", self->state.log->first_index());
  if (index > self->state.log->last_index())
    return make_error(ec::unspecified, "cannot take snapshot at index", index,
                      "that is larger than largest index",
                      self->state.log->last_index());
  if (index > self->state.commit_index)
    return make_error(ec::unspecified, "cannot take snapshot of uncommitted "
                      "index", index);
  // Request to snapshot is now guaranteed to fall within the window of our log.
  VAST_ASSERT(index >= self->state.log->first_index()
              && index <= self->state.log->last_index());
  // If we have the snapshot stream open, then it must have been opened while
  // we were receiving InstallSnapshot messages. We don't support both
  // operations at the same time.
  if (self->state.snapshot.is_open())
    return make_error(ec::unspecified, "snapshot delivery in progress");
  // Write snapshot to file.
  snapshot_header hdr;
  hdr.last_included_index = index;
  hdr.last_included_term = self->state.log->at(index).term;
  auto result = save(self->system(), self->state.dir / "snapshot",
                     hdr, snapshot);
  if (!result)
    return result.error();
  VAST_DEBUG(role(self), "completed snapshotting, last included term =",
             hdr.last_included_term << ", index =", hdr.last_included_index);
  VAST_ASSERT(self->state.log->first_index() <= hdr.last_included_index);
  // Update (volatile) server state.
  self->state.last_snapshot_index = hdr.last_included_index;
  self->state.last_snapshot_term = hdr.last_included_term;
  // Truncate now no longer needed entries.
  auto n = self->state.log->truncate_before(index + 1);
  VAST_IGNORE_UNUSED(n);
  VAST_DEBUG(role(self), "truncated", n, "log entries");
  return index;
}

// Loads a snapshot header into memory and adapts the server state accordingly.
template <class Actor>
expected<void> load_snapshot_header(Actor* self) {
  VAST_DEBUG(role(self), "loads snapshot header");
  snapshot_header hdr;
  // Read snapshot header from filesystem.
  auto result = load(self->system(), self->state.dir / "snapshot", hdr);
  if (!result)
    return result.error();
  if (hdr.version != 1)
    return make_error(ec::version_error, "needed version 1, got", hdr.version);
  if (hdr.last_included_index < self->state.last_snapshot_index)
    return make_error(ec::unspecified, "stale snapshot");
  // Update actor state.
  self->state.last_snapshot_index = hdr.last_included_index;
  self->state.last_snapshot_term = hdr.last_included_term;
  self->state.commit_index = std::max(self->state.last_snapshot_index,
                                      self->state.commit_index);
  VAST_DEBUG(role(self), "sets commitIndex to", self->state.commit_index);
  // Discard the existing log entirely if (1) the log is fully covered by the
  // last snapshot, or (2) the last snapshot entry comes after the first log
  // entry and both entries have different terms.
  if (self->state.log->last_index() < self->state.last_snapshot_index
      || (self->state.log->first_index() <= self->state.last_snapshot_index
          && self->state.log->at(self->state.last_snapshot_index).term
             != self->state.last_snapshot_term)) {
    VAST_DEBUG(role(self), "discards entire log");
    self->state.log->truncate_before(self->state.last_snapshot_index + 1);
    self->state.log->truncate_after(self->state.last_snapshot_index);
  }
  return {};
}

// Loads snapshot contents.
template <class Actor>
expected<std::vector<char>> load_snapshot_data(Actor* self) {
  VAST_DEBUG(role(self), "loads snapshot data");
  snapshot_header hdr;
  std::vector<char> data;
  auto result = load(self->system(), self->state.dir / "snapshot", hdr, data);
  if (!result)
    return result.error();
  if (hdr.version != 1)
    return make_error(ec::version_error, "needed version 1, got", hdr.version);
  return data;
}

// Sends a range of entries to the state machine.
template <class Actor>
void deliver(Actor* self, index_type from, index_type to) {
  VAST_ASSERT(from != 0 && to != 0);
  if (!self->state.state_machine)
    return;
  if (from < self->state.log->first_index()) {
    VAST_ASSERT(self->state.last_snapshot_index > 0);
    auto snapshot = load_snapshot_data(self);
    if (!snapshot) {
      VAST_ERROR(role(self), "failed to load snapshot data",
                 self->system().render(snapshot.error()));
      self->quit(snapshot.error());
      return;
    }
    VAST_DEBUG(role(self), "delivers snapshot at index",
               self->state.last_snapshot_index);
    auto msg = make_message(snapshot_atom::value,
                            self->state.last_snapshot_index,
                            std::move(*snapshot));
    self->send(self->state.state_machine, self->state.last_snapshot_index, msg);
    from = self->state.last_snapshot_index + 1;
  }
  VAST_DEBUG(role(self), "sends entries", from, "to", to);
  for (auto i = from; i <= to; ++i) {
    auto& entry = self->state.log->at(i);
    if (entry.data.empty()) {
      VAST_DEBUG(role(self), "skips delivery of no-op entry", i);
    } else {
      caf::binary_deserializer bd{self->system(), entry.data};
      caf::message msg;
      bd >> msg;
      VAST_DEBUG(role(self), "delivers entry", i, deep_to_string(msg));
      self->send(self->state.state_machine, i, msg);
    }
  }
}

// Adjusts the leader's commit index.
template <class Actor>
void advance_commit_index(Actor* self) {
  VAST_ASSERT(is_leader(self));
  auto last_index = self->state.log->last_index();
  // Without peers, we can adjust the commit index directly.
  if (self->state.peers.empty()) {
    VAST_DEBUG(role(self), "advances commitIndex", self->state.commit_index,
               "->", last_index);
    deliver(self, self->state.commit_index + 1, last_index);
    self->state.commit_index = last_index;
    return;
  }
  // Compute the new commit index based through a majority vote.
  auto n = self->state.peers.size() + 1;
  std::vector<index_type> xs;
  xs.reserve(n);
  xs.emplace_back(last_index);
  for (auto& state : self->state.peers)
    xs.emplace_back(state.match_index);
  std::sort(xs.begin(), xs.end());
  VAST_DEBUG(role(self), "takes quorum min of [", detail::join(xs, ", "), ']');
  auto index = xs[(n - 1) / 2];
  // Check whether the new index makes sense to accept.
  if (index <= self->state.commit_index) {
    VAST_DEBUG(role(self), "didn't advance commitIndex",
               self->state.commit_index << ", quorum min =", index);
    return;
  }
  VAST_ASSERT(index >= self->state.log->first_index());
  if (self->state.log->at(index).term != self->state.current_term)
    return;
  VAST_DEBUG(role(self), "advances commitIndex", self->state.commit_index,
             "->", index);
  VAST_ASSERT(index <= last_index);
  deliver(self, self->state.commit_index + 1, index);
  self->state.commit_index = index;
}

template <class Actor>
expected<void> become_follower(Actor* self, term_type term) {
  if (!is_follower(self))
    VAST_DEBUG(role(self), "becomes follower in term", term);
  VAST_ASSERT(term >= self->state.current_term);
  if (term > self->state.current_term) {
    self->state.current_term = term;
    self->state.leader = {};
    self->state.voted_for = 0;
    auto result = save_state(self);
    if (!result)
      return result;
  }
  self->become(self->state.following);
  if (self->state.election_time == clock::time_point::max())
    reset_election_time(self);
  return {};
}

template <class Actor>
void become_leader(Actor* self) {
  VAST_DEBUG(role(self), "becomes leader in term", self->state.current_term);
  self->become(self->state.leading);
  self->state.leader = self;
  self->state.election_time = clock::time_point::max();
  // Reset follower state.
  for (auto& peer : self->state.peers) {
    peer.next_index = self->state.log->last_index() + 1;
    peer.match_index = 0;
    peer.last_snapshot_index = 0;
  }
  // (A no-op entry has an index of 0 and no data in our implementation.)
  log_entry entry;
  entry.term = self->state.current_term;
  auto res = self->state.log->append({std::move(entry)});
  if (!res) {
    VAST_ERROR(role(self), "failed to append no-op entry:",
               self->system().render(res.error()));
    self->quit(res.error());
    return;
  }
  advance_commit_index(self);
  // Kick off leader heartbeat loop.
  if (!self->state.peers.empty() && !self->state.heartbeat_inflight) {
    VAST_DEBUG(role(self), "kicks off heartbeat");
    self->send(self, heartbeat_atom::value);
    self->state.heartbeat_inflight = true;
  }
}

template <class Actor>
expected<void> become_candidate(Actor* self) {
  VAST_ASSERT(!is_leader(self));
  if (self->state.leader)
    VAST_DEBUG(role(self), "becomes candidate in term",
               self->state.current_term + 1, "(leader timeout)");
  else if (is_candidate(self))
    VAST_DEBUG(role(self), "becomes candidate in term",
               self->state.current_term + 1, "(election timeout)");
  else
    VAST_DEBUG(role(self), "becomes candidate in term",
               self->state.current_term + 1);
  self->become(self->state.candidating);
  ++self->state.current_term;
  self->state.leader = {};
  self->state.voted_for = self->state.id; // vote for ourself
  auto result = save_state(self);
  if (!result)
    return result;
  if (self->state.peers.empty()) {
    VAST_DEBUG(role(self), "has no peers, advancing to leader immediately");
    become_leader(self);
    return {};
  }
  reset_election_time(self);
  // Request votes from all peers.
  request_vote::request req;
  req.candidate_id = self->state.id;
  req.term = self->state.current_term;
  req.last_log_index = self->state.log->last_index();
  req.last_log_term = last_log_term(self);
  auto req_term = req.term;
  VAST_DEBUG(role(self), "broadcasts RequestVote request: term =",
             self->state.current_term << ", last log index =",
             req.last_log_index << ", last log term =", req.last_log_term);
  auto msg = make_message(req);
  for (auto& peer : self->state.peers) {
    auto peer_id = peer.id;
    if (peer.peer) {
      self->request(peer.peer, election_timeout * 2, msg).then(
        [=](const request_vote::response& resp) {
          VAST_DEBUG(role(self), "got RequestVote response from peer",
                     peer_id << ": term =", resp.term);
          if (!is_candidate(self))
            return;
          if (self->state.current_term != req_term || !is_candidate(self)) {
            VAST_DEBUG(role(self), "discards vote with stale term");
            return;
          }
          if (resp.term > self->state.current_term) {
            VAST_DEBUG(role(self), "got vote from newer term, stepping down");
            become_follower(self, resp.term);
          } else if (!resp.vote_granted) {
            VAST_DEBUG(role(self), "got vote denied");
          } else {
            // Become leader if we have the majority of votes.
            auto count = size_t{2}; // Our and the peer's vote.
            for (auto& state : self->state.peers)
              if (state.id == peer_id)
                state.have_vote = true;
              else if (state.have_vote)
                ++count;
            auto n = self->state.peers.size() + 1;
            VAST_DEBUG(role(self), "got vote granted,", count, "out of", n);
            if (count > n / 2)
              become_leader(self);
          }
        }
      );
    }
  }
  return {};
}

template <class Actor>
auto handle_request_vote(Actor* self, request_vote::request& req) {
  VAST_DEBUG(role(self), "got RequestVote request: term =",
             req.term << ", candidate =",
             req.candidate_id << ", last log index =",
             req.last_log_index << ", last log term =", req.last_log_term);
  request_vote::response resp;
  // From §5.1 in the Raft paper: "If a server receives a request with a stale
  // term number, it rejects it."
  if (req.term < self->state.current_term) {
    VAST_DEBUG(role(self), "rejects RequestVote with stale term");
    resp.term = self->state.current_term;
    resp.vote_granted = false;
    return resp;
  }
  // If someone else has a higher term, we subdue. Whether we grant our vote
  // depends on the subsequent conditions.
  if (req.term > self->state.current_term)
    become_follower(self, req.term);
  // From §5.4.1 in the Raft paper: "[..] the voter denies its vote if its own
  // log is more up-to-date than that of the candidate. [..]. Raft determines
  // which of two logs is more up-to-date by comparing the index and term of
  // the last entries in the logs. If the logs have last entries with different
  // terms, then the log with the later term is more up-to-date."
  auto last_log_index = self->state.log->last_index();
  auto less_up_to_date = req.last_log_term > last_log_term(self)
                         || (req.last_log_term == last_log_term(self)
                             && req.last_log_index >= last_log_index);
  // From §5.2 in the Raft paper: "Each server will vote for at most one
  // candidate in a given term, on a first-come-first-serve basis [..]."
  if (self->state.voted_for == 0 && less_up_to_date) {
    VAST_DEBUG(role(self), "grants vote");
    become_follower(self, req.term);
    reset_election_time(self);
    self->state.voted_for = req.candidate_id;
    auto result = save_state(self);
    if (!result) {
      VAST_ERROR(role(self), self->system().render(result.error()));
      self->quit(result.error());
      return resp;
    }
  }
  resp.term = self->state.current_term;
  resp.vote_granted = self->state.voted_for == req.candidate_id;
  return resp;
}

// Constructs an InstallSnapshot message.
template <class Actor>
expected<install_snapshot::request>
make_install_snapshot(Actor* self, peer_state& peer) {
  using namespace binary_byte_literals;
  VAST_ASSERT(is_leader(self));
  install_snapshot::request req;
  req.term = self->state.current_term;
  req.leader_id = self->state.id;
  // If we don't have a handle to the snapshot already, open it.
  if (!peer.snapshot) {
    auto filename = self->state.dir / "snapshot";
    peer.snapshot = std::make_unique<detail::mmapbuf>(filename.str());
    peer.last_snapshot_index = self->state.last_snapshot_index;
  }
  auto available = peer.snapshot->in_avail();
  VAST_ASSERT(available > 0);
  req.last_snapshot_index = peer.last_snapshot_index;
  req.byte_offset = peer.snapshot->size() - available;
  // Construct at most chunks of 1 MB.
  auto remaining_bytes = detail::narrow_cast<unsigned long long>(available);
  size_t msg_size = std::min(1_MiB, remaining_bytes);
  req.data.resize(msg_size);
  VAST_DEBUG(role(self), "fills snapshot chunk with", req.data.size(), "bytes");
  auto got = peer.snapshot->sgetn(req.data.data(), req.data.size());
  if (got != static_cast<std::streamsize>(req.data.size()))
    return make_error(ec::filesystem_error, "incomplete chunk read");
  req.done = peer.snapshot->in_avail() == 0;
  return req;
}

// Sends an InstallSnapshot message to a peer.
template <class Actor>
auto send_install_snapshot(Actor* self, peer_state& peer) {
  VAST_ASSERT(peer.peer);
  auto req = make_install_snapshot(self, peer);
  if (!req) {
    VAST_ERROR(role(self), self->system().render(req.error()));
    self->quit(req.error());
    return;
  }
  auto peer_id = peer.id;
  auto req_term = req->term;
  self->request(peer.peer, request_timeout, std::move(*req)).then(
    [=](const install_snapshot::response& resp) {
      VAST_IGNORE_UNUSED(peer_id);
      VAST_DEBUG(role(self), "got InstallSnapshot response from peer", peer_id,
                 ": term =", resp.term << ", bytes stored =",
                 resp.bytes_stored);
      if (req_term != self->state.current_term) {
        VAST_DEBUG(role(self), "ignores stale response");
        return;
      }
      VAST_ASSERT(is_leader(self));
      if (resp.term > self->state.current_term) {
        VAST_DEBUG(role(self), "steps down (reponse with higher term)");
        become_follower(self, resp.term);
        return;
      }
      VAST_ASSERT(resp.term == self->state.current_term);
      if (auto p = current_peer(self)) {
        if (p->snapshot->in_avail() == 0) {
          VAST_DEBUG(role(self), "completed sending snapshot to peer", p->id,
                     "(index", p->last_snapshot_index << ')');
          p->next_index = p->last_snapshot_index + 1;
          p->match_index = p->last_snapshot_index;
          advance_commit_index(self);
          p->snapshot.reset();
          p->last_snapshot_index = 0;
        }
      }
    }
  );
}

template <class Actor>
auto handle_install_snapshot(Actor* self, install_snapshot::request& req) {
  VAST_DEBUG(role(self), "got InstallSnapshot request: leader =",
             req.leader_id << ", term =", req.term << ", bytes =",
             req.data.size());
  install_snapshot::response resp;
  resp.term = self->state.current_term;
  resp.bytes_stored = 0;
  if (req.term < self->state.current_term) {
    VAST_DEBUG(role(self), "rejects request: stale term");
    return resp;
  }
  if (req.term > self->state.current_term)
    resp.term = req.term;
  auto grd = caf::detail::make_scope_guard([&] { reset_election_time(self); });
  become_follower(self, req.term);
  if (self->state.leader != self->current_sender())
    self->state.leader = actor_cast<actor>(self->current_sender());
  // Prepare for writing a snapshot unless we're already in the middle of
  // receiving snapshot chunks.
  if (!self->state.snapshot.is_open()) {
    auto filename = self->state.dir / "snapshot";
    self->state.snapshot.open(filename.str());
    if (!self->state.snapshot) {
      VAST_ERROR(role(self), "failed to open snapshot writer");
      return resp;
    }
  }
  auto bytes_written = static_cast<uint64_t>(self->state.snapshot.tellp());
  resp.bytes_stored = bytes_written;
  // Ensure that the chunk is in sequence.
  if (req.byte_offset < bytes_written) {
    VAST_WARNING(role(self), "ignores stale snapshot chunk, got offset",
                 req.byte_offset, "but have", bytes_written);
    return resp;
  }
  if (req.byte_offset > bytes_written) {
    VAST_WARNING(role(self), "ignores discontinous snapshot chunk, got offset",
                 req.byte_offset, "but have", bytes_written);
    return resp;
  }
  // Append the raw bytes and compute the new position.
  auto sb = self->state.snapshot.rdbuf();
  auto put = sb->sputn(req.data.data(), req.data.size());
  // Terminate if we could not append the entire chunk.
  if (put != static_cast<std::streamsize>(req.data.size()))
    self->quit(make_error(ec::filesystem_error, "incomplete chunk write"));
  resp.bytes_stored += put;
  if (self->state.snapshot.bad())
    self->quit(make_error(ec::filesystem_error, "bad snapshot file"));
  // If this was the last chunk, load the snapshot.
  if (req.done) {
    self->state.snapshot.close();
    auto res = load_snapshot_header(self);
    if (!res) {
      VAST_ERROR(role(self), "failed to apply remote snapshot:",
                 self->system().render(res.error()));
      self->quit(res.error());
      return resp;
    }
    VAST_DEBUG(role(self), "completed loading of remote snapshot with index",
               self->state.last_snapshot_index, "and term",
               self->state.last_snapshot_term);
    if (self->state.state_machine) {
      auto snapshot = load_snapshot_data(self);
      if (!snapshot) {
        VAST_ERROR(role(self), "failed to load snapshot:",
                   self->system().render(snapshot.error()));
        self->quit(snapshot.error());
        return resp;
      }
      VAST_DEBUG(role(self), "delivers snapshot");
      self->send(self->state.state_machine,
                 self->state.last_snapshot_index,
                 make_message(snapshot_atom::value,
                              self->state.last_snapshot_index,
                              std::move(*snapshot)));
    }
  }
  return resp;
}

template <class Actor>
auto send_append_entries(Actor* self, peer_state& peer) {
  // Find the previous index for this peer.
  auto prev_log_index = peer.next_index - 1;
  VAST_ASSERT(prev_log_index <= self->state.log->last_index());
  // If we cannot provide the log the peer needs, we send a snapshot.
  if (peer.next_index < self->state.log->first_index()) {
    VAST_DEBUG(role(self), "sends snapshot, server", peer.id, "needs index",
               peer.next_index, "but log starts at",
               self->state.log->first_index());
    send_install_snapshot(self, peer);
    return;
  }
  // Find the previous term for this peer.
  index_type prev_log_term;
  if (prev_log_index >= self->state.log->first_index()) {
    prev_log_term = self->state.log->at(prev_log_index).term;
  } else if (prev_log_index == 0) {
    prev_log_term = 0;
  } else if (prev_log_index == self->state.last_snapshot_index) {
    prev_log_term = self->state.last_snapshot_term;
  } else {
    VAST_DEBUG(role(self), "sends snapshot, can't find previous log term "
               "for server", peer.id);
    send_install_snapshot(self, peer);
    return;
  }
  // Assemble an AppendEntries request.
  append_entries::request req;
  req.term = self->state.current_term;
  req.leader_id = self->state.id;
  req.commit_index = self->state.commit_index;
  req.prev_log_index = prev_log_index;
  req.prev_log_term = prev_log_term;
  // Add log entries [peer next index, local last log index].
  for (auto i = peer.next_index; i <= self->state.log->last_index(); ++i)
    req.entries.push_back(self->state.log->at(i));
  auto req_term = req.term;
  auto num_entries = req.entries.size();
  auto peer_id = peer.id;
  VAST_IGNORE_UNUSED(peer_id);
  VAST_DEBUG(role(self), "sends AppendEntries request to peer", peer_id,
             "with", num_entries, "entries");
  // Send request away and process response.
  self->request(peer.peer, request_timeout, req).then(
    [=](const append_entries::response& resp) {
      VAST_DEBUG(role(self), "got AppendEntries response: peer =",
                 peer_id << ", term =", resp.term << ", success =",
                 (resp.success ? 'T' : 'F'));
      if (req_term != self->state.current_term) {
        VAST_DEBUG(role(self), "ignores stale response");
        return;
      }
      VAST_ASSERT(is_leader(self));
      if (resp.term > self->state.current_term) {
        VAST_DEBUG(role(self), "steps down (reponse with higher term)");
        become_follower(self, resp.term);
        return;
      }
      VAST_ASSERT(resp.term == self->state.current_term);
      if (auto p = current_peer(self)) {
        if (resp.success) {
          if (p->match_index > prev_log_index + num_entries) {
            VAST_WARNING(role(self), "got nonmonotonic matchIndex with a term");
          } else {
            p->match_index = prev_log_index + num_entries;
            advance_commit_index(self);
          }
          p->next_index = p->match_index + 1;
        } else {
          if (p->next_index > 1)
            --p->next_index;
          if (p->next_index > resp.last_log_index + 1)
            p->next_index = resp.last_log_index + 1;
        }
        VAST_DEBUG(role(self), "now has peer's next index at", p->next_index);
      }
    }
  );
}

template <class Actor>
result<append_entries::response>
handle_append_entries(Actor* self, append_entries::request& req) {
  VAST_DEBUG(role(self), "got AppendEntries request: entries =",
             req.entries.size() << ", term =", req.term << ", prev log index =",
             req.prev_log_index << ", prev log term =", req.prev_log_term);
  // Construct a response.
  append_entries::response resp;
  resp.term = self->state.current_term;
  resp.success = false;
  resp.last_log_index = self->state.log->last_index();
  if (req.term < self->state.current_term) {
    VAST_DEBUG(role(self), "rejects request: stale term");
    return resp;
  }
  if (req.term > self->state.current_term) {
    VAST_DEBUG(role(self), "got request with higher term", req.term,
               "than own term", self->state.current_term);
    resp.term = req.term;
  }
  auto grd = caf::detail::make_scope_guard([&] { reset_election_time(self); });
  become_follower(self, req.term);
  // We can only append contiguous entries.
  if (req.prev_log_index > self->state.log->last_index()) {
    VAST_DEBUG(role(self), "rejects request: not contiguous ("
               << req.prev_log_index, ">", self->state.log->last_index()
               << ')');
    return resp;
  }
  // Ensure term compatibility with previous entry (and thereby inductively
  // with all prior entries as well).
  if (req.prev_log_index >= self->state.log->first_index()
      && req.prev_log_term != self->state.log->at(req.prev_log_index).term) {
    VAST_DEBUG(role(self), "rejects request: terms disagree");
    return resp;
  }
  VAST_DEBUG(role(self), "accepts request, leader =", req.leader_id);
  resp.success = true;
  if (self->state.leader != self->current_sender())
    self->state.leader = actor_cast<actor>(self->current_sender());
  // Apply entries to local log.
  auto index = req.prev_log_index;
  std::vector<log_entry> xs;
  for (auto& entry : req.entries) {
    ++index;
    if (index <= self->state.log->last_index()) {
      if (entry.term == self->state.log->at(index).term)
        continue;
      VAST_ASSERT(self->state.commit_index < index);
      auto n = self->state.log->truncate_after(index - 1);
      if (n > 0)
        VAST_DEBUG(role(self), "truncated", n, "entries after index",
                   index - 1);
    }
    xs.push_back(std::move(entry));
  }
  if (!xs.empty()) {
    // Append new entries to log.
    auto n = xs.size();
    auto res = self->state.log->append(std::move(xs));
    if (!res) {
      VAST_IGNORE_UNUSED(n);
      VAST_ERROR(role(self), "failed to append", n, "entries to log");
      return res.error();
    }
    VAST_DEBUG(role(self), "appended", n, "entries to log");
  }
  resp.last_log_index = self->state.log->last_index();
  if (self->state.commit_index < req.commit_index) {
    deliver(self, self->state.commit_index + 1, req.commit_index);
    VAST_DEBUG(role(self), "adjusts commitIndex", self->state.commit_index,
               "->", req.commit_index);
    self->state.commit_index = req.commit_index;
  }
  return resp;
}

} // namespace <anonymous>

behavior consensus(stateful_actor<server_state>* self, path dir) {
  self->state.dir = std::move(dir);
  self->state.prng.seed(std::random_device{}());
  if (exists(self->state.dir)) {
    auto res = load_state(self);
    if (!res) {
      VAST_ERROR(role(self), "failed to load state:",
                 self->system().render(res.error()));
      self->quit(res.error());
      return {};
    }
  } else {
    // Generate unique server ID; can be overriden in startup phase.
    std::uniform_int_distribution<server_id> unif{1};
    auto id = unif(self->state.prng);
    VAST_DEBUG(role(self), "generated random server ID", id);
    self->state.id = id;
  }
  // We monitor all other servers; when one goes down, we disable it until
  // comes back.
  self->set_down_handler(
    [=](const down_msg& msg) {
      auto a = actor_cast<actor>(msg.source);
      auto i = std::find_if(self->state.peers.begin(),
                            self->state.peers.end(),
                            [&](auto& state) { return state.peer == a; });
      VAST_ASSERT(i != self->state.peers.end());
      VAST_DEBUG(role(self), "got DOWN from peer#" << i->id);
      i->peer = {};
    }
  );
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      VAST_DEBUG(role(self), "got request to terminate");
      if (auto res = save_state(self); !res) {
        VAST_ERROR(role(self), "failed to persist state:",
                   self->system().render(res.error()));
        self->quit(res.error());
      } else {
        self->quit(msg.reason);
      }
    }
  );
  // -- common behavior ------------------------------------------------------
  auto common = message_handler{
    [=](election_atom) {
      if (clock::now() >= self->state.election_time)
        become_candidate(self);
    },
    [=](statistics_atom) -> result<statistics> {
      statistics stats;
      auto& l = *self->state.log;
      stats.log_entries = l.empty() ? 0 : l.last_index() - l.first_index();
      stats.log_bytes = bytes(l);
      return stats;
    },
    [=](snapshot_atom, index_type index, const std::vector<char>& snapshot) {
      // We keep at least one entry in the log.
      // if (self->state.commit_index <= 1)
      //   return make_error(ec::unspecified,
      //                     "not enough commited entries to snapshot");
      return save_snapshot(self, index, snapshot);
    },
    [=](peer_atom, const actor& peer, server_id peer_id) {
      VAST_DEBUG(role(self), "re-activates peer", peer_id);
      VAST_ASSERT(peer_id != 0);
      auto i = std::find_if(self->state.peers.begin(),
                            self->state.peers.end(),
                            [&](auto& p) { return p.id == peer_id; });
      VAST_ASSERT(i != self->state.peers.end()); // currently no config changes
      VAST_ASSERT(!i->peer); // must have been deactivated via DOWN message.
      i->peer = peer;
      if (is_leader(self) && !self->state.heartbeat_inflight) {
        VAST_DEBUG(role(self), "kicks off heartbeat");
        self->send(self, heartbeat_atom::value);
        self->state.heartbeat_inflight = true;
      }
    },
    // When the state machine initializes, it will obtain the latest state
    // through this handler.
    [=](subscribe_atom, const actor& state_machine) {
      VAST_DEBUG(role(self), "got subscribe request from", state_machine);
      self->state.state_machine = state_machine;
      if (self->state.commit_index > 0)
        deliver(self, 1, self->state.commit_index);
    },
  };
  // -- follower & candidate --------------------------------------------------
  self->state.following = self->state.candidating = message_handler{
    [=](append_entries::request& req) {
      return handle_append_entries(self, req);
    },
    [=](request_vote::request& req) {
      return handle_request_vote(self, req);
    },
    [=](install_snapshot::request& req) {
      return handle_install_snapshot(self, req);
    },
    // Non-leaders forward replication requests to the leader when possible.
    // TODO: instead of delegating the request, we could return with an error
    // and the actual leader to the user in order to avoid permanent routing of
    // messages through this instance (which may cause performance issues).
    [=](replicate_atom, const message& command) {
      auto rp = self->make_response_promise();
      if (!self->state.leader)
        rp.deliver(make_error(ec::unspecified, "no leader available"));
      else
        rp.delegate(self->state.leader, replicate_atom::value, command);
    }
  }.or_else(common);
  // -- leader ---------------------------------------------------------------
  self->state.leading = message_handler{
    [=](heartbeat_atom) {
      self->state.heartbeat_inflight = false;
      if (self->state.peers.empty()) {
        VAST_DEBUG(role(self), "cancels heartbeat loop (no peers)");
        return;
      }
      for (auto& peer : self->state.peers)
        if (peer.peer)
          send_append_entries(self, peer);
      self->delayed_send(self, heartbeat_period, heartbeat_atom::value);
      self->state.heartbeat_inflight = true;
    },
    [=](replicate_atom, const message& command) -> result<ok_atom> {
      auto log_index = self->state.log->last_index() + 1;
      VAST_DEBUG(role(self), "replicates new entry with index", log_index);
      VAST_ASSERT(log_index > self->state.commit_index);
      // Create new log entry.
      std::vector<log_entry> entry(1);
      entry[0].term = self->state.current_term;
      entry[0].index = log_index;
      caf::binary_serializer bs{self->system(), entry[0].data};
      bs << command;
      // Append to log and wait for commit via AppendEntries.
      auto res = self->state.log->append(std::move(entry));
      if (!res) {
        VAST_ERROR(role(self), "failed to append new entry:",
                   self->system().render(res.error()));
        return res.error();
      }
      // Without peers, we can commit the entry immediately.
      if (self->state.peers.empty())
        advance_commit_index(self);
      return ok_atom::value;
    }
  }.or_else(common);
  // -- startup --------------------------------------------------------------
  return {
    [=](id_atom, server_id id) {
      VAST_DEBUG(role(self), "sets server ID to", id);
      self->state.id = id;
    },
    [=](seed_atom, uint64_t value) {
      self->state.prng.seed(value);
    },
    [=](peer_atom, const actor& peer, server_id peer_id) {
      VAST_ASSERT(peer_id != 0);
      VAST_DEBUG(role(self), "adds new peer", peer_id);
      if (peer_id == self->state.id) {
        VAST_ERROR(role(self), "peer cannot have same server ID");
        return;
      }
      auto pred = [&](auto& x) { return x.peer == peer || x.id == peer_id; };
      auto i = std::find_if(self->state.peers.begin(), self->state.peers.end(),
                            pred);
      VAST_ASSERT(i == self->state.peers.end());
      self->monitor(peer);
      peer_state state;
      state.id = peer_id;
      state.peer = peer;
      self->state.peers.push_back(std::move(state));
    },
    [=](run_atom) {
      self->become(self->state.following);
      VAST_DEBUG(role(self), "starts in term", self->state.current_term);
      if (self->state.voted_for != 0)
        VAST_DEBUG(role(self), "previously voted for server",
                   self->state.voted_for);
      // Load the persistent log into memory.
      self->state.log = std::make_unique<log>(self->system(),
                                              self->state.dir / "log");
      if (self->state.log->empty())
        VAST_DEBUG(role(self), "initialized new log");
      else
        VAST_DEBUG(role(self), "initialized log in range ["
                   << self->state.log->first_index() << ','
                   << self->state.log->last_index() << ']');
      // Read a snapshot from disk.
      if (exists(self->state.dir / "snapshot")) {
        auto res = load_snapshot_header(self);
        if (!res) {
          self->quit(res.error());
          return;
        }
        VAST_DEBUG(role(self), "found existing snapshot up to "
                   "index", self->state.last_snapshot_index, "and term",
                   self->state.last_snapshot_term);
      }
      // Start acting.
      if (self->state.peers.empty()) {
        ++self->state.current_term;
        self->state.voted_for = self->state.id;
        become_leader(self);
      } else {
        become_follower(self, self->state.current_term);
      }
    }
  };
}

} // namespace raft
} // namespace system
} // namespace vast
