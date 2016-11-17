#ifndef TEST_EVENT_FIXTURE
#define TEST_EVENT_FIXTURE

#include <caf/all.hpp>

#include "vast/event.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/test.hpp"

#include "data.hpp"

using namespace vast;

struct event_fixture {
  event_fixture() {
    bro_conn_log = inhale<format::bro::reader>(bro::conn);
    bro_dns_log = inhale<format::bro::reader>(bro::dns);
    bro_http_log = inhale<format::bro::reader>(bro::http);
    bgpdump_txt = inhale<format::bgpdump::reader>(bgpdump::updates20140821);
    random = extract(vast::format::test::reader{42, 1000});
  }

  std::vector<event> bro_conn_log;
  std::vector<event> bro_dns_log;
  std::vector<event> bro_http_log;
  std::vector<event> bgpdump_txt;
  std::vector<event> random;

private:
  template <class Reader>
  static std::vector<event> inhale(char const* filename) {
    auto input = std::make_unique<std::ifstream>(filename);
    Reader reader{std::move(input)};
    return extract(reader);
  }

  template <class Reader>
  static std::vector<event> extract(Reader&& reader) {
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

#endif
