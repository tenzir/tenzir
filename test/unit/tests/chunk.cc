#include "framework/unit.h"
#include "vast/chunk.h"
#include "vast/event.h"

using namespace vast;

SUITE("core")

TEST("chunk")
{
  auto t = type::integer{};
  t.name("i");

  chunk chk;
  chunk::writer w{chk};
  std::vector<event> es;
  for (auto i = 0; i < 1e3; ++i)
  {
    es.push_back(event::make(i, t));
    REQUIRE(w.write(es.back()));
  }

  w.flush();
  CHECK(chk.events() == 1e3);

  chunk::reader r{chk};
  for (auto i = 0; i < 1e3; ++i)
  {
    auto e = r.read();
    REQUIRE(e);
    CHECK(*e == event::make(i, t));
  }

  chunk copy{chk};
  CHECK(chk == copy);

  chunk from_events{es};
  CHECK(from_events == chk);

  // Assign IDs to the chunk.
  ewah_bitstream ids;
  ids.append(42, false);
  ids.append(1e3 - 1, true);
  CHECK(! chk.ids(ids));  // 1 event ID missing.

  ids.push_back(true);
  CHECK(chk.ids(std::move(ids)));
}

TEST("chunk event extraction")
{
  auto t = type::integer{};
  REQUIRE(t.name("test"));

  chunk chk;
  chunk::writer w{chk};
  std::vector<event> es;
  for (auto i = 0; i < 1024; ++i)
  {
    es.push_back(event::make(integer{1000 + i}, t));
    es.back().id(1000 + i);
    REQUIRE(w.write(es.back()));
  }

  w.flush();
  REQUIRE(chk.events() == 1024);

  chunk::reader r{chk};
  result<event> e = error{"not yet assigned"};

  e = r.read(1042);
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 1042);

  e = r.read(1010);
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 1010);

  CHECK(! r.read(10));
  CHECK(! r.read(999));
  CHECK(! r.read(2024));

  e = r.read(1011);
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 1011);

  e = r.read(1720);
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 1720);

  e = r.read(2023);
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 2023);

  e = r.read(2000);
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 2000);
}
