#ifndef FIXTURES_CONSENSUS_HPP
#define FIXTURES_CONSENSUS_HPP

#include "vast/system/atoms.hpp"
#include "vast/system/consensus.hpp"

#include "fixtures/actor_system.hpp"

namespace fixtures {

struct consensus : actor_system {
  consensus() {
    launch();
  }

  void launch() {
    using namespace vast::system;
    server1 = self->spawn(raft::consensus, directory / "server1");
    server2 = self->spawn(raft::consensus, directory / "server2");
    server3 = self->spawn(raft::consensus, directory / "server3");
    self->send(server1, id_atom::value, raft::server_id{1});
    self->send(server2, id_atom::value, raft::server_id{2});
    self->send(server3, id_atom::value, raft::server_id{3});
    // Make it deterministic.
    self->send(server1, seed_atom::value, uint64_t{42});
    self->send(server2, seed_atom::value, uint64_t{43});
    self->send(server3, seed_atom::value, uint64_t{44});
    // Setup peers.
    self->send(server1, peer_atom::value, server2, raft::server_id{2});
    self->send(server1, peer_atom::value, server3, raft::server_id{3});
    self->send(server2, peer_atom::value, server1, raft::server_id{1});
    self->send(server2, peer_atom::value, server3, raft::server_id{3});
    self->send(server3, peer_atom::value, server1, raft::server_id{2});
    self->send(server3, peer_atom::value, server2, raft::server_id{3});
    self->send(server1, run_atom::value);
    self->send(server2, run_atom::value);
    self->send(server3, run_atom::value);
    MESSAGE("sleeping until leader got elected");
    std::this_thread::sleep_for(raft::election_timeout * 2);
  }

  void shutdown() {
    using namespace caf;
    using namespace vast::system;
    self->monitor(server1);
    self->send_exit(server1, exit_reason::user_shutdown);
    self->receive(
      [](const down_msg&) { /* nop */ },
      error_handler()
    );
    self->monitor(server2);
    self->send_exit(server2, exit_reason::user_shutdown);
    self->receive(
      [](const down_msg&) { /* nop */ },
      error_handler()
    );
    self->monitor(server3);
    self->send_exit(server3, exit_reason::user_shutdown);
    self->receive(
      [](const down_msg&) { /* nop */ },
      error_handler()
    );
  }

  caf::actor server1;
  caf::actor server2;
  caf::actor server3;
  // Timeout for requests against the consensus module.
  std::chrono::seconds timeout = std::chrono::seconds(3);
};

} // namespace fixtures

#endif
