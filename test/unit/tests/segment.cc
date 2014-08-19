#include "framework/unit.h"

#include "vast/bitstream.h"
#include "vast/event.h"
#include "vast/segment.h"

using namespace vast;

SUITE("core")

TEST("segment")
{
  segment s;

  auto t = type::count{};
  t.name("count");
  chunk c;
  chunk::writer w{c};

  size_t i = 0;
  while (i < 1124)
  {
    event e{i, t};
    e.id(i + 1);

    REQUIRE(w.write(e));

    if (++i % 256 == 0)
    {
      w.flush();
      REQUIRE(s.push_back(std::move(c)));
      c = {};
      w = chunk::writer{c};
    }
  }

  w.flush();
  REQUIRE(s.push_back(std::move(c)));
  REQUIRE(s.meta().events == 1124);

  // Read all events out.
  segment::reader r{s};
  for (size_t i = 0; i < 1124; ++i)
  {
    event expected{i, t};
    expected.id(i + 1);

    auto e = r.read();
    REQUIRE(e);
    REQUIRE(*e == expected);
  }

  // Make sure the schema has the type of the event we put in.
  auto u = s.meta().schema.find_type("count");
  REQUIRE(u);
  CHECK(t == *u);

  // Read an event, which must have the same type.
  auto e = r.read(42);
  if (! e)
    std::cout << e.error() << std::endl;
  REQUIRE(e);
  CHECK(e->type() == *u);
}

TEST("segment seeking")
{
  auto t = type::integer{};
  REQUIRE(t.name("test"));

  segment s;
  std::vector<event> es;
  for (auto i = 0; i < 1024; ++i)
  {
    es.emplace_back(integer{1000 + i}, t);
    es.back().id(1000 + i);
    if (es.size() == 256)
    {
      s.push_back(chunk{es});
      es.clear();
    }
  }

  REQUIRE(s.meta().events == 1024);

  segment::reader r{s};
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
}
