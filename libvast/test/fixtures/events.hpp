#ifndef FIXTURES_EVENTS_HPP
#define FIXTURES_EVENTS_HPP

#include <caf/all.hpp>

#include "vast/error.hpp"
#include "vast/event.hpp"

#include "data.hpp"
#include "test.hpp"

namespace fixtures {

using namespace vast;

struct events {
  events();

  static std::vector<event> bro_conn_log;
  static std::vector<event> bro_dns_log;
  static std::vector<event> bro_http_log;
  static std::vector<event> bgpdump_txt;
  static std::vector<event> random;

private:
  template <class Reader>
  static std::vector<event> inhale(char const* filename) {
    auto input = std::make_unique<std::ifstream>(filename);
    Reader reader{std::move(input)};
    return extract(reader);
  }

  template <class Reader>
  static std::vector<event> extract(Reader&& reader) {
    auto e = expected<event>{no_error};
    std::vector<event> events;
    while (e || !e.error()) {
      e = reader.read();
      if (e)
        events.push_back(std::move(*e));
    }
    REQUIRE(!e);
    CHECK(e.error() == ec::end_of_input);
    REQUIRE(!events.empty());
    return events;
  }
};

} // namespace fixtures

#endif
