#ifndef FIXTURES_ACTOR_SYSTEM_AND_EVENTS_HPP
#define FIXTURES_ACTOR_SYSTEM_AND_EVENTS_HPP

#include "fixtures/actor_system.hpp"
#include "fixtures/events.hpp"

namespace fixtures {

struct actor_system_and_events : actor_system, events {
  actor_system_and_events() {
    // Manually assign monotonic IDs to events.
    auto id = event_id{0};
    auto assign = [&](auto& xs) {
      for (auto& x : xs)
        x.id(id++);
    };
    assign(bro_conn_log);
    assign(bro_dns_log);
    id += 1000; // Cause an artificial gap in the ID sequence.
    assign(bro_http_log);
    assign(bgpdump_txt);
  }
};

} // namespace fixtures

#endif
