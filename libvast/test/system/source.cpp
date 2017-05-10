#include "vast/format/bro.hpp"
#include "vast/system/source.hpp"

#include "vast/detail/make_io_stream.hpp"

#define SUITE system
#include "test.hpp"
#include "data.hpp"
#include "fixtures/actor_system.hpp"

using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(source_tests, fixtures::actor_system)

TEST(Bro source) {
  auto stream = detail::make_input_stream(bro::conn);
  REQUIRE(stream);
  format::bro::reader reader{std::move(*stream)};
  auto src = self->spawn(source<format::bro::reader>, std::move(reader));
  self->monitor(src);
  self->send(src, sink_atom::value, self);
  self->send(src, run_atom::value);
  self->receive([&](std::vector<event> const& events) {
    CHECK_EQUAL(events.size(), 8462u);
    CHECK_EQUAL(events[0].type().name(), "bro::conn");
  });
  // A source terminates normally after having consumed the entire input.
  self->receive([&](caf::down_msg const& msg) {
    CHECK(msg.reason == caf::exit_reason::normal);
  });
}

FIXTURE_SCOPE_END()
