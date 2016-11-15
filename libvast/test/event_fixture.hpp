#include <caf/all.hpp>

#include "vast/event.hpp"
#include "vast/format/bro.hpp"

#include "data.hpp"

using namespace vast;

struct event_fixture {
  event_fixture() {
    bro_conn_log = inhale<format::bro::reader>(m57_day11_18::conn);
    bro_dns_log = inhale<format::bro::reader>(m57_day11_18::dns);
    bro_http_log = inhale<format::bro::reader>(m57_day11_18::http);
  }

  std::vector<event> bro_conn_log;
  std::vector<event> bro_dns_log;
  std::vector<event> bro_http_log;

private:
  template <class Reader>
  static std::vector<event> inhale(char const* filename) {
    auto input = std::make_unique<std::ifstream>(filename);
    Reader reader{std::move(input)};
    maybe<event> e;
    std::vector<event> events;
    while (!e.error()) {
      e = reader.read();
      if (e)
        events.push_back(std::move(*e));
    }
    CHECK(e.error() == ec::end_of_input);
    REQUIRE(!events.empty());
    return events;
  }
};
