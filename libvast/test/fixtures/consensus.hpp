#ifndef FIXTURES_CONSENSUS_HPP
#define FIXTURES_CONSENSUS_HPP

#include "vast/system/atoms.hpp"
#include "vast/system/consensus.hpp"

#include "fixtures/actor_system.hpp"

namespace fixtures {

struct consensus : actor_system {
  consensus() {
    using namespace vast::system;
    auto leader_config = raft::configuration{{"id", "1"}};
    auto follower1_config = raft::configuration{{"id", "2"}};
    auto follower2_config = raft::configuration{{"id", "3"}};
    server1 = self->spawn(raft::consensus, leader_config);
    server2 = self->spawn(raft::consensus, follower1_config);
    server3 = self->spawn(raft::consensus, follower2_config);
    // Peers usually connect to each other through a configuration file, which
    // contains the TCP endpoints of the respective peers. For now, we do the
    // setup manually.
    self->send(server1, peer_atom::value, server2, raft::server_id{2});
    self->send(server1, peer_atom::value, server3, raft::server_id{3});
    self->send(server1, peer_atom::value, server3, raft::server_id{3}); // dup
    self->send(server2, peer_atom::value, server1, raft::server_id{1});
    self->send(server2, peer_atom::value, server3, raft::server_id{3});
    self->send(server3, peer_atom::value, server1, raft::server_id{2});
    self->send(server3, peer_atom::value, server2, raft::server_id{3});
  }

  void shutdown() {
    using namespace vast::system;
    self->send(server1, shutdown_atom::value);
    self->send(server2, shutdown_atom::value);
    self->send(server3, shutdown_atom::value);
  }

  caf::actor server1;
  caf::actor server2;
  caf::actor server3;
  // Timeout for requests against the consensus module.
  std::chrono::seconds timeout = std::chrono::seconds(3);
};

} // namespace fixtures

#endif
