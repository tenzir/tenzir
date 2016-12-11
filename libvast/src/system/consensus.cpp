#include <caf/all.hpp>

#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/consensus.hpp"

using namespace caf;

namespace vast {
namespace system {
namespace raft {

index_type log_type::last_index() const {
  return container_.size();
}

log_type::size_type log_type::truncate_after(size_type index) {
  VAST_ASSERT(index <= last_index());
  auto n = container_.size();
  container_.resize(index);
  return n - index;
}

log_type::iterator log_type::begin() {
  return container_.begin();
}

log_type::iterator log_type::end() {
  return container_.end();
}

log_type::size_type log_type::size() const {
  return container_.size();
}

bool log_type::empty() const {
  return container_.empty();
}

log_entry& log_type::front() {
  VAST_ASSERT(!empty());
  return container_.front();
}

log_entry& log_type::back() {
  VAST_ASSERT(!empty());
  return container_.back();
}

log_entry& log_type::operator[](size_type i) {
  VAST_ASSERT(!empty());
  VAST_ASSERT(i > 0 && i - 1 < size());
  return container_[i - 1];
}

void log_type::push_back(const value_type& x) {
  container_.push_back(x);
}

namespace {

template <class Actor>
expected<void> apply_configuration(Actor* self, const configuration& config) {
  auto i = config.find("id");
  if (i == config.end())
    return make_error(ec::unspecified, "missing server ID");
  if (!parsers::u64(i->second, self->state.id))
    return make_error(ec::parse_error, "failed to parse server ID");
  if (self->state.id == 0)
    return make_error(ec::unspecified, "invalid server ID of 0");
  return {};
}

template <class Actor>
bool is_follower(Actor* self) {
  return self->current_behavior().as_behavior_impl()
    == self->state.following.as_behavior_impl();
}

template <class Actor>
bool is_candidate(Actor* self) {
  return self->current_behavior().as_behavior_impl()
    == self->state.candidating.as_behavior_impl();
}

template <class Actor>
bool is_leader(Actor* self) {
  return self->current_behavior().as_behavior_impl()
    == self->state.leading.as_behavior_impl();
}

template <class Actor>
term_type last_log_term(Actor* self) {
  if (self->state.log.empty())
    return 0;
  return self->state.log.back().term;
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
    VAST_ASSERT(!"not possible");
  result += '#';
  result += std::to_string(self->state.id);
  return result;
}

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
  // Pick a election timeout uniformly at random from [T, T * 2], where T is
  // the configured election timeout.
  auto timeout = random_timeout(self);
  VAST_DEBUG(role(self), "will start election in", timeout);
  self->state.election_time = clock::now() + timeout;
  self->delayed_send(self, timeout, election_atom::value);
}

template <class Actor>
void become_follower(Actor* self, term_type term) {
  VAST_DEBUG(role(self), "becomes follower in term", term, "(old term:",
             self->state.current_term << ')');
  VAST_ASSERT(term >= self->state.current_term);
  if (term > self->state.current_term) {
    self->state.current_term = term;
    self->state.leader = {};
    self->state.voted_for = 0;
    // TODO: persist log meta data (currentTerm and votedFor).
  }
  self->become(self->state.following);
  if (self->state.election_time == clock::time_point::max())
    reset_election_time(self);
}

template <class Actor>
void become_leader(Actor* self) {
  VAST_ASSERT(is_candidate(self));
  VAST_DEBUG(role(self),
             "becomes leader (term", self->state.current_term << ')');
  self->become(self->state.leading);
  self->state.leader = self;
  self->state.election_time = clock::time_point::max();
  // Initialize follower state.
  for (auto& p : self->state.peers) {
    p.next_index = self->state.log.last_index() + 1;
    p.match_index = 0;
  }
  // TODO: set out own match_index = self->state.log.last_index();
  if (!self->state.peers.empty() && !self->state.heartbeat_inflight) {
    VAST_DEBUG(role(self), "kicks off heartbeat");
    self->send(self, heartbeat_atom::value);
    self->state.heartbeat_inflight = true;
  }
}

template <class Actor>
void become_candidate(Actor* self) {
  VAST_ASSERT(!is_leader(self));
  if (self->state.leader)
    VAST_DEBUG(role(self), "becomes candidate for term",
               self->state.current_term + 1, "(leader timeout)");
  else if (is_candidate(self))
    VAST_DEBUG(role(self), "becomes candidate for term",
               self->state.current_term + 1, "(election timeout)");
  else
    VAST_DEBUG(role(self), "becomes candidate for term",
               self->state.current_term + 1);
  self->become(self->state.candidating);
  ++self->state.current_term;
  self->state.leader = {};
  self->state.voted_for = self->state.id; // vote for ourself
  // TODO: persist log meta data (currentTerm and votedFor).
  if (self->state.peers.empty()) {
    VAST_DEBUG(role(self), "has no peers, advancing to leader immediately");
    become_leader(self);
    return;
  }
  reset_election_time(self);
  // Request votes from all peers.
  request_vote::request req;
  req.candidate_id = self->state.id;
  req.term = self->state.current_term;
  req.last_log_index = self->state.log.last_index();
  req.last_log_term = last_log_term(self);
  auto msg = make_message(req);
  for (auto& p : self->state.peers) {
    auto peer_id = p.id;
    if (p.peer) {
      self->request(p.peer, election_timeout * 2, msg).then(
        [=](const request_vote::response& resp) {
          VAST_DEBUG(role(self), "got RequestVote response (peer",
                     peer_id << ", term", resp.term << ')');
          if (!is_candidate(self))
            return;
          if (self->state.current_term != req.term || !is_candidate(self)) {
            VAST_DEBUG(role(self), "discards vote from term", resp.term);
            return;
          }
          if (resp.term > self->state.current_term) {
            VAST_DEBUG(role(self), "got vote from newer term, stepping down");
            become_follower(self, resp.term);
          } else if (!resp.vote_granted) {
            VAST_DEBUG(role(self), "got vote denied by", peer_id);
          } else {
            // Become leader if we have the majority of votes.
            auto count = size_t{2}; // our and the peer's vote
            for (auto& state : self->state.peers)
              if (state.id == peer_id)
                state.have_vote = true;
              else if (state.have_vote)
                ++count;
            auto n = self->state.peers.size() + 1;
            VAST_DEBUG(role(self), "got vote granted by", peer_id,
                       '(' << count, "out of", n << ')');
            if (count > n / 2)
              become_leader(self);
          }
        }
      );
    }
  }
}

template <class Actor>
void advance_commit_index(Actor* self) {
  VAST_ASSERT(is_leader(self));
  // Compute the new commit index based through a majority vote.
  auto n = self->state.peers.size() + 1;
  std::vector<index_type> xs;
  xs.reserve(n);
  xs.emplace_back(self->state.log.last_index()); // TODO: check
  for (auto& state : self->state.peers)
    xs.emplace_back(state.match_index);
  std::sort(xs.begin(), xs.end());
  VAST_DEBUG(role(self), "takes quorum min of [", detail::join(xs, ", "), ']');
  auto index = xs[(n - 1) / 2];
  // Check whether the new index makes sense to accept.
  if (index <= self->state.commit_index) {
    VAST_DEBUG(role(self), "didn't advance commitIndex",
               self->state.commit_index, "(quorum min =", index << ')');
    return;
  }
  VAST_ASSERT(index >= 1);
  if (self->state.log[index].term != self->state.current_term) {
    VAST_DEBUG(role(self), "didn't advance commitIndex (quorum from old term",
               self->state.log[index].term, "current:",
               self->state.current_term << ')');
    return;
  }
  VAST_DEBUG(role(self), "advances commitIndex to", index);
  self->state.commit_index = index;
  // Answer pending requests.
  auto r = size_t{0};
  while (!self->state.pending.empty()) {
    auto& pair = self->state.pending.front();
    if (pair.first > index)
      break;
    pair.second.deliver(ok_atom::value, pair.first);
    self->state.pending.pop_front();
    ++r;
  }
  VAST_DEBUG(role(self), "delivered", r, "pending responses");
}

template <class Actor>
auto handle_request_vote(Actor* self, const request_vote::request& req) {
  VAST_DEBUG(role(self), "got RequestVote request (term",
             req.term << ", candidate", req.candidate_id << ", lastLogIndex",
             req.last_log_index << ", lastLogTerm", req.last_log_term << ')');
  request_vote::response resp;
  // From §5.1 in the Raft paper: "If a server receives a request with a stale
  // term number, it rejects it."
  if (req.term < self->state.current_term) {
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
  auto last_log_index = self->state.log.last_index();
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
    // TODO: persist log meta data (currentTerm and votedFor).
  }
  resp.term = self->state.current_term;
  resp.vote_granted = self->state.voted_for == req.candidate_id;
  return resp;
}

template <class Actor>
auto handle_append_entries(Actor* self, const append_entries::request& req) {
  VAST_DEBUG(role(self), "got AppendEntries request with", req.entries.size(),
             "entries");
  // Construct a response.
  append_entries::response resp;
  resp.term = self->state.current_term;
  resp.success = false;
  if (req.term < self->state.current_term) {
    VAST_DEBUG(role(self), "rejects request: stale term");
    return resp;
  }
  if (req.term > self->state.current_term)
    resp.term = req.term;
  become_follower(self, req.term);
  // We make sure to reset the election timer upon returning from this
  // function. Otherwise the potentially time-consuming operations below may
  // shorten the timer below its minimum value, resulting in premature firing.
  auto grd = caf::detail::make_scope_guard([&] { reset_election_time(self); });
  // We can only append contiguous entries.
  if (req.prev_log_index > self->state.log.last_index()) {
    VAST_DEBUG(role(self), "rejects request: not contiguous");
    return resp;
  }
  // Ensure term compatibility with previous entry (and thereby inductively
  // with all prior entries as well).
  if (req.prev_log_index >= 1
      && req.prev_log_term != self->state.log[req.prev_log_index].term) {
    VAST_DEBUG(role(self), "rejects request: terms disagree");
    return resp;
  }
  VAST_DEBUG(role(self), "accepts request (leader =", req.leader_id << ')');
  resp.success = true;
  if (self->state.leader != self->current_sender())
    self->state.leader = actor_cast<actor>(self->current_sender());
  // Apply entries to local log.
  auto index = req.prev_log_index;
  for (auto& entry : req.entries) {
    ++index;
    if (index <= self->state.log.last_index()) {
      if (entry.term == self->state.log[index].term)
        continue;
      VAST_ASSERT(self->state.commit_index < index);
      self->state.log.truncate_after(index - 1);
    }
    self->state.log.push_back(entry); // TODO: persist
  }
  if (self->state.commit_index < req.commit_index) {
    VAST_DEBUG(role(self), "adjusts commitIndex", self->state.commit_index,
               "->", req.commit_index);
    self->state.commit_index = req.commit_index;
  }
  return resp;
}

} // namespace <anonymous>

behavior consensus(stateful_actor<server_state>* self, configuration config) {
  // Parse and apply configuration.
  auto result = apply_configuration(self, config);
  if (!result)
    self->quit(result.error());
  // Start by kicking off the election loop timeout.
  self->state.prng.seed(std::random_device{}());
  auto initial_timeout = random_timeout(self);
  VAST_DEBUG("follower#" << self->state.id,
             "will hold first election in", initial_timeout);
  self->state.election_time = clock::now() + initial_timeout;
  self->delayed_send(self, initial_timeout, election_atom::value);
  // Temporarily disable a disconnected peer.
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
  // -- common behavior ------------------------------------------------------
  auto common = message_handler{
    [=](election_atom) {
      if (clock::now() >= self->state.election_time)
        become_candidate(self);
    },
    [=](peer_atom, actor const& peer, server_id peer_id) {
      if (peer_id == 0) {
        VAST_WARNING(role(self), "ignores peer with invalid ID", peer_id);
        return;
      }
      auto pred = [&](auto& state) {
        return state.peer == peer || state.id == peer_id;
      };
      auto i = std::find_if(self->state.peers.begin(), self->state.peers.end(),
                            pred);
      if (i == self->state.peers.end()) {
        VAST_DEBUG(role(self), "adds new peer", peer_id);
        self->monitor(peer);
        peer_state state;
        state.id = peer_id;
        state.peer = peer;
        self->state.peers.push_back(state);
        if (is_leader(self) && !self->state.heartbeat_inflight) {
          VAST_DEBUG(role(self), "kicks off heartbeat");
          self->send(self, heartbeat_atom::value);
          self->state.heartbeat_inflight = true;
        }
      } else if (i->id == peer_id) {
        if (!i->peer) {
          VAST_DEBUG(role(self), "re-activates peer", peer_id);
          i->peer = peer;
          // TODO: re-initialize state?
        } else {
          VAST_WARNING(role(self), "ignores request to add known peer", i->id);
        }
      } else {
          VAST_WARNING(role(self), "ignores request to add peer (ID mismatch:",
                       "got", peer_id << ", need", i->id << ')');
      }
    },
    [=](shutdown_atom) {
      VAST_DEBUG(role(self), "got request to terminate");
      self->quit(exit_reason::user_shutdown);
    }
  };
  auto replicate_through_leader = [=](replicate_atom, const message& msg) {
    auto rp = self->make_response_promise();
    if (!self->state.leader)
      rp.deliver(make_error(ec::unspecified, "no leader available"));
    else
      rp.delegate(self->state.leader, replicate_atom::value, msg);
  };
  // -- follower --------------------------------------------------------------
  self->state.following = message_handler{
    [=](const append_entries::request& req) {
      return handle_append_entries(self, req);
    },
    [=](const request_vote::request& req) {
      return handle_request_vote(self, req);
    },
    // All operations go through the leader.
    replicate_through_leader
  }.or_else(common);
  // -- candidate -------------------------------------------------------------
  self->state.candidating = message_handler{
    [=](const append_entries::request& req) {
      return handle_append_entries(self, req);
    },
    [=](const request_vote::request& req) {
      return handle_request_vote(self, req);
    },
    // All operations go through the leader.
    replicate_through_leader
  }.or_else(common);
  // -- leader ---------------------------------------------------------------
  self->state.leading = message_handler{
    [=](heartbeat_atom) {
      self->state.heartbeat_inflight = false;
      if (self->state.peers.empty()) {
        VAST_DEBUG(role(self), "cancels heartbeat loop (no peers)");
        return;
      }
      // Assemble an AppendEntries request.
      append_entries::request req;
      req.term = self->state.current_term;
      req.leader_id = self->state.id;
      req.commit_index = self->state.commit_index;
      for (auto& p : self->state.peers) {
        if (p.peer) {
          // Compute prevLogIndex and prevLogTerm.
          auto prev_log_index = p.next_index - 1;
          VAST_ASSERT(prev_log_index <= self->state.log.last_index());
          index_type prev_log_term;
          if (prev_log_index == 0) {
            prev_log_term = 0;
          } else {
            VAST_ASSERT(prev_log_index >= 1);
            prev_log_term = self->state.log[prev_log_index].term;
          }
          // Fill in computed values.
          req.prev_log_index = prev_log_index;
          req.prev_log_term = prev_log_term;
          // Fill in relevant entries.
          req.entries.clear();
          for (auto i = p.next_index; i < self->state.log.last_index() + 1; ++i)
            req.entries.push_back(self->state.log[i]);
          auto num_entries = req.entries.size();
          auto peer_id = p.id;
          VAST_DEBUG(role(self), "sends AppendEntries request peer to",
                     peer_id, "with", num_entries, "entries");
          // Send request away and process response.
          self->request(p.peer, election_timeout * 4, req).then(
            [=](const append_entries::response& resp) {
              VAST_DEBUG(role(self), "got AppendEntries response from peer",
                         peer_id, "for term", resp.term << ':',
                         (resp.success ? "success" : "no success"));
              if (!is_leader(self)) {
                VAST_DEBUG(role(self), "ignores stale response (stepped down)");
                return;
              }
              if (resp.term > self->state.current_term) {
                VAST_DEBUG(role(self), "steps down (reponse with higher term)");
                become_follower(self, resp.term);
                return;
              }
              VAST_ASSERT(resp.term == self->state.current_term);
              auto pred = [=](auto& state) {
                return state.peer->address() == self->current_sender();
              };
              auto ps = std::find_if(self->state.peers.begin(),
                                     self->state.peers.end(),
                                     pred);
              if (ps == self->state.peers.end()) {
                VAST_WARNING(role(self), "ignores response from dead peer");
                return;
              }
              if (resp.success) {
                if (ps->match_index > prev_log_index + num_entries) {
                  VAST_WARNING(role(self),
                               "got nonmonotonic matchIndex with a term");
                } else {
                  ps->match_index = prev_log_index + num_entries;
                  // TODO: don't call for *every* response, but only when it
                  // makes sense.
                  advance_commit_index(self);
                }
                ps->next_index = ps->match_index + 1;
              } else {
                if (ps->next_index > 1)
                  --ps->next_index;
              }
            }
          );
        }
      }
      self->delayed_send(self, heartbeat_period, heartbeat_atom::value);
      self->state.heartbeat_inflight = true;
    },
    [=](replicate_atom, const message& msg) {
      VAST_DEBUG(role(self), "replicates new entry");
      auto rp = self->make_response_promise();
      // Create new log entry.
      log_entry entry;
      entry.term = self->state.current_term;
      entry.data = msg;
      // Append to log and wait for commit.
      self->state.log.push_back(entry);
      auto log_index = self->state.log.last_index();
      VAST_ASSERT(log_index > self->state.commit_index);
      if (self->state.peers.empty()) {
        VAST_DEBUG(role(self), "immediately commits entry", log_index);
        ++self->state.commit_index;
        VAST_ASSERT(log_index == self->state.commit_index);
        rp.deliver(ok_atom::value, log_index);
      } else {
        VAST_DEBUG(role(self), "asks followers to confirm entry", log_index);
        self->state.pending.emplace_back(log_index, rp);
      }
    }
  }.or_else(common);
  return self->state.following;
}

} // namespace raft
} // namespace system
} // namespace vast
