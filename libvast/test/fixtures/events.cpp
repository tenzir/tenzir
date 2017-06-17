#include "vast/format/bro.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/test.hpp"

#include "fixtures/events.hpp"

namespace fixtures {

std::vector<event> events::bro_conn_log;
std::vector<event> events::bro_dns_log;
std::vector<event> events::bro_http_log;
std::vector<event> events::bgpdump_txt;
std::vector<event> events::random;

events::events() {
  static bool initialized = false;
  if (initialized)
    return;
  initialized = true;
  MESSAGE("inhaling unit test suite events");
  bro_conn_log = inhale<format::bro::reader>(bro::conn);
  bro_dns_log = inhale<format::bro::reader>(bro::dns);
  bro_http_log = inhale<format::bro::reader>(bro::http);
  bgpdump_txt = inhale<format::bgpdump::reader>(bgpdump::updates20140821);
  random = extract(vast::format::test::reader{42, 1000});
  // Assign monotonic IDs to events starting at 0.
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

} // namespace fixtures
