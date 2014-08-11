#include "framework/unit.h"

#include "vast/bitstream.h"
#include "vast/event.h"
#include "vast/segment.h"

SUITE("segment")
using namespace vast;

TEST("reading and writing")
{
  segment s1;

  /// Construct a writer with 256 events per chunk and no upper bound on the
  /// total segment size.
  segment::writer w(&s1, 256);

  auto t = type::count{};
  t.name("count");

  for (size_t i = 0; i < 1124; ++i)
  {
    // Since the segment has no size restriction, it is always possible to add
    // more events.
    REQUIRE(w.write(event{i, t}));
  }

  // At this point, the writer has still 100 events that have not yet been
  // flushed. We can either (1) simply flush the remaining events, or (2)
  // attach the writer to a different segment.
  //
  // Let's begin with the first option.
  CHECK(w.flush());
  REQUIRE(s1.events() == 1124);

  // Let's add more events and then attempt the second option.
  for (size_t i = 0; i < 50; ++i)
    CHECK(w.write(event{i, t}));

  segment s2;
  w.attach_to(&s2);
  CHECK(w.flush());
  REQUIRE(s2.events() == 50);

  // Ensure that we get back what we put in the first segment.
  segment::reader r1{&s1};
  size_t n = 0;
  while (auto e = r1.read())
    CHECK(*e == (event{n++, t}));
  CHECK(n == 1124);

  // Same thing for the second segment.
  segment::reader r2{&s2};
  n = 0;
  while (auto e = r2.read())
    CHECK(*e == (event{n++, t}));
  CHECK(n == 50);
}

TEST("auto schematization")
{
  segment s;
  segment::writer w{&s};

  auto t = type::record{
    {"", type::integer{}},
    {"", type::boolean{}}};

  t.name("foo");

  for (size_t i = 0; i < 100; ++i)
    REQUIRE(w.write(event{record{42, true}, t}));

  REQUIRE(w.flush());
  auto u = s.schema().find_type("foo");
  REQUIRE(u);
  CHECK(t == *u);

  segment::reader r{&s};
  auto e = r.read();
  REQUIRE(e);
  CHECK(e->type() == *u);
}

TEST("seeking")
{
  auto t = type::integer{};
  REQUIRE(t.name("test"));

  segment s;
  s.base(1000);
  segment::writer w{&s, 256};
  for (auto i = 0; i < 1024; ++i)
    REQUIRE(w.write(event{1000 + i, t}));
  CHECK(w.flush());
  REQUIRE(s.events() == 1024);

  segment::reader r{&s};
  trial<event> e = error{"not yet assigned"};

  CHECK(r.seek(1042));
  e = r.read();
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 1042);

  CHECK(r.seek(1010));
  e = r.read();
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 1010);

  CHECK(! r.seek(10));
  CHECK(! r.seek(999));
  CHECK(! r.seek(2024));

  CHECK(r.seek(1011));
  e = r.read();
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 1011);

  CHECK(r.seek(1720));
  e = r.read();
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 1720);

  CHECK(r.seek(2023));
  e = r.read();
  REQUIRE(e);
  CHECK(*get<integer>(*e) == 2023);
}

TEST("event loading")
{
  auto t = type::count{};
  t.name("count");

  segment s;
  {
    segment::writer w{&s, 10};
    for (size_t i = 0; i < 256; ++i)
      CHECK(w.write(event{i, t}));
  }
  CHECK(s.events() == 256);

  auto b = 42u;
  s.base(b);

  auto o = s.load(b);
  REQUIRE(o);
  auto& first = *o;
  CHECK(first.id() == b);
  CHECK(*get<count>(first) == 0);

  o = s.load(b + 42);
  REQUIRE(o);
  auto& mid1 = *o;
  CHECK(mid1.id() == b + 42);
  CHECK(*get<count>(mid1) == 42);

  o = s.load(256);
  REQUIRE(o);
  auto& mid2 = *o;
  CHECK(mid2.id() == 256);
  CHECK(*get<count>(mid2) == 256 - b);

  o = s.load(b + 255);
  REQUIRE(o);
  auto& last = *o;
  CHECK(last.id() == b + 255);
  CHECK(*get<count>(last) == 255);
}

TEST("event extraction")
{
  auto t = type::count{};
  t.name("count");

  segment s;
  {
    segment::writer w{&s, 10};
    for (size_t i = 0; i < 256; ++i)
      CHECK(w.write(event{i, t}));
  }
  s.base(1000);

  ewah_bitstream mask;
  mask.append(1000, false);
  for (auto i = 0; i < 256; ++i)
    mask.push_back(i % 4 == 0);
  mask.append(1000, false);
  segment::reader r{&s};

  auto mi = mask.begin();
  auto mend = mask.end();
  CHECK(*mi == 1000);

  event_id id = s.base();
  while (mi != mend)
  {
    auto e = r.read(*mi);
    REQUIRE(e);
    CHECK(e->id() == id);
    id += 4;
    ++mi;
  }
}
