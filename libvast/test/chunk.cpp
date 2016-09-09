#include "vast/chunk.hpp"
#include "vast/event.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/concept/printable/vast/event.hpp"

#include "test.hpp"

//using namespace vast;
//
//namespace {
//
//struct fixture {
//  fixture() {
//    auto event_type = type::integer{};
//    event_type.name("foo");
//    for (auto i = 0; i < 1e3; ++i) {
//      events.push_back(event::make(i, event_type));
//      events.back().id(1000 + i);
//    }
//  }
//
//  type event_type;
//  std::vector<event> events;
//};
//
//} // namespace <anonymous>
//
//FIXTURE_SCOPE(chunk_tests, fixture)
//
//TEST(chunk) {
//  MESSAGE("construct a chunk via chunk::writer");
//  chunk chk;
//  {
//    chunk::writer w{chk};
//    for (auto i = 0; i < 1e3; ++i)
//      REQUIRE(w.write(events[i]));
//  }
//  CHECK_EQUAL(chk.events(), 1e3);
//  MESSAGE("read from the chunk");
//  chunk::reader r{chk};
//  for (auto i = 0; i < 1e3; ++i) {
//    auto e = r.read();
//    REQUIRE(e);
//    auto expected = event::make(i, event_type);
//    expected.id(1000 + i);
//    CHECK_EQUAL(*e, expected);
//  }
//  MESSAGE("construct a chunk from a sequence of events");
//  chunk chk2;
//  CHECK(chk2 != chk);
//  CHECK(chk2.compress(events));
//  CHECK(chk2 == chk);
//  MESSAGE("assign IDs to events in the chunk");
//  ewah_bitstream ids;
//  ids.append(42, false);
//  ids.append(1e3 - 1, true);
//  CHECK(!chk.ids(ids)); // 1 event ID missing.
//  ids.push_back(true);
//  CHECK(chk.ids(std::move(ids)));
//}
//
//TEST(chunk_event_extraction) {
//  chunk chk;
//  chk.compress(events);
//  chunk::reader r{chk};
//  maybe<event> e;
//  MESSAGE("try to read out-of-bounds");
//  CHECK(!r.read(10));
//  CHECK(!r.read(2024));
//  MESSAGE("get first event");
//  e = r.read(1000);
//  REQUIRE(e);
//  CHECK_EQUAL(*get<integer>(*e), 0);
//  REQUIRE_EQUAL(e->id(), 1000u);
//  MESSAGE("seek forward");
//  e = r.read(1720);
//  REQUIRE(e);
//  CHECK_EQUAL(*get<integer>(*e), 720);
//  REQUIRE_EQUAL(e->id(), 1720u);
//  MESSAGE("read last event");
//  e = r.read(1999);
//  REQUIRE(e);
//  CHECK_EQUAL(*get<integer>(*e), 999);
//  REQUIRE_EQUAL(e->id(), 1999u);
//  MESSAGE("attempt to read one-past-last event");
//  REQUIRE(!r.read(2000));
//}
//
//TEST(chunk_serialization) {
//  chunk chk, chk2;
//  chk.compress(events);
//  std::vector<char> buf;
//  save(buf, chk);
//  load(buf, chk2);
//  CHECK(chk == chk2);
//}
//
//FIXTURE_SCOPE_END()
