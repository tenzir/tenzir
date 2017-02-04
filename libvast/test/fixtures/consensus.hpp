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

  ~consensus() {
    shutdown();
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
    self->send(server3, peer_atom::value, server1, raft::server_id{1});
    self->send(server3, peer_atom::value, server2, raft::server_id{2});
    self->send(server1, run_atom::value);
    self->send(server2, run_atom::value);
    self->send(server3, run_atom::value);
    self->send(server1, subscribe_atom::value, self);
    self->send(server2, subscribe_atom::value, self);
    self->send(server3, subscribe_atom::value, self);
    MESSAGE("sleeping until leader got elected");
    std::this_thread::sleep_for(raft::election_timeout * 2);
  }

  void shutdown() {
    using namespace caf;
    using namespace vast::system;
    self->send_exit(server1, exit_reason::user_shutdown);
    self->wait_for(server1);
    self->send_exit(server2, exit_reason::user_shutdown);
    self->wait_for(server2);
    self->send_exit(server3, exit_reason::user_shutdown);
    self->wait_for(server3);
  }

  template <class... Ts>
  auto replicate(const caf::actor& server, Ts&&... xs) {
    using namespace caf;
    using namespace vast::system;
    auto command = make_message(std::forward<Ts>(xs)...);
    self->request(server, consensus_timeout, replicate_atom::value,
                  command).receive(
      [](ok_atom) { /* nop */ },
      error_handler()
    );
  }

  template <class... Ts>
  caf::message await(vast::system::raft::index_type index) {
    caf::message result;
    auto n = 0;
    self->receive_for(n, 3) (
      [&](vast::system::raft::index_type i, const caf::message& msg) {
        REQUIRE_EQUAL(i, index);
        result = msg;
      },
      error_handler()
    );
    return result;
  }

  caf::actor server1;
  caf::actor server2;
  caf::actor server3;
};

} // namespace fixtures

#endif
