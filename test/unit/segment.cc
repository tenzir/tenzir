#include "test.h"
#include "vast/bitstream.h"
#include "vast/event.h"
#include "vast/segment.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(segment_reading_and_writing)
{
  segment s1;

  /// Construct a writer with 256 events per chunk and no upper bound on the
  /// total segment size.
  segment::writer w(&s1, 256);

  for (size_t i = 0; i < 1124; ++i)
  {
    // Since the segment has no size restriction, it is always possible to add
    // more events.
    BOOST_REQUIRE(w.write(event{i}));
  }

  // At this point, the writer has still 100 events that have not yet been
  // flushed. We can either (1) simply flush the remaining events, or (2)
  // attach the writer to a different segment.
  //
  // Let's begin with the first option.
  BOOST_CHECK(w.flush());
  BOOST_REQUIRE_EQUAL(s1.events(), 1124);

  // Let's add more events and then attempt the second option.
  for (size_t i = 0; i < 50; ++i)
    BOOST_CHECK(w.write(event{i}));

  segment s2;
  w.attach_to(&s2);
  BOOST_CHECK(w.flush());
  BOOST_REQUIRE_EQUAL(s2.events(), 50);

  // Ensure that we get back what we put in the first segment.
  segment::reader r1{&s1};
  size_t n = 0;
  while (auto e = r1.read())
    BOOST_CHECK_EQUAL(*e, (event{n++}));
  BOOST_CHECK_EQUAL(n, 1124);

  // Same thing for the second segment.
  segment::reader r2{&s2};
  n = 0;
  while (auto e = r2.read())
    BOOST_CHECK_EQUAL(*e, (event{n++}));
  BOOST_CHECK_EQUAL(n, 50);
}

BOOST_AUTO_TEST_CASE(auto_schematization)
{
  segment s;
  segment::writer w{&s};

  record_type rec;
  rec.args.emplace_back("", type::make<int_type>());
  rec.args.emplace_back("", type::make<bool_type>());
  auto t = type::make<record_type>("foo", std::move(rec));

  for (size_t i = 0; i < 100; ++i)
  {
    event e{42, true};
    e.type(t);
    BOOST_REQUIRE(w.write(e));
  }

  BOOST_REQUIRE(w.flush());
  auto u = s.schema().find_type("foo");
  BOOST_REQUIRE(u);
  BOOST_CHECK(*t == *u);
  BOOST_CHECK(t == u);

  segment::reader r{&s};
  auto e = r.read();
  BOOST_REQUIRE(e);
  BOOST_CHECK(e->type() == u);
}

BOOST_AUTO_TEST_CASE(segment_seeking)
{
  segment s;
  s.base(1000);
  segment::writer w{&s, 256};
  for (auto i = 0; i < 1024; ++i)
    BOOST_CHECK(w.write(event{1000 + i}));
  BOOST_CHECK(w.flush());
  BOOST_REQUIRE_EQUAL(s.events(), 1024);

  segment::reader r{&s};
  trial<event> e = error{"not yet assigned"};

  BOOST_CHECK(r.seek(1042));
  e = r.read();
  BOOST_REQUIRE(e);
  BOOST_CHECK_EQUAL(e->front(), 1042);

  BOOST_CHECK(r.seek(1010));
  e = r.read();
  BOOST_REQUIRE(e);
  BOOST_CHECK_EQUAL(e->front(), 1010);

  BOOST_CHECK(! r.seek(10));
  BOOST_CHECK(! r.seek(999));
  BOOST_CHECK(! r.seek(2024));

  BOOST_CHECK(r.seek(1011));
  e = r.read();
  BOOST_REQUIRE(e);
  BOOST_CHECK_EQUAL(e->front(), 1011);

  BOOST_CHECK(r.seek(1720));
  e = r.read();
  BOOST_REQUIRE(e);
  BOOST_CHECK_EQUAL(e->front(), 1720);

  BOOST_CHECK(r.seek(2023));
  e = r.read();
  BOOST_REQUIRE(e);
  BOOST_CHECK_EQUAL(e->front(), 2023);
}

BOOST_AUTO_TEST_CASE(segment_event_loading)
{
  segment s;
  {
    segment::writer w{&s, 10};
    for (size_t i = 0; i < 256; ++i)
      BOOST_CHECK(w.write(event{i}));
  }
  BOOST_CHECK_EQUAL(s.events(), 256);

  auto b = 42u;
  s.base(b);

  auto o = s.load(b);
  BOOST_REQUIRE(o);
  auto& first = *o;
  BOOST_CHECK_EQUAL(first.id(), b);
  BOOST_CHECK_EQUAL(first[0], 0u);

  o = s.load(b + 42);
  BOOST_REQUIRE(o);
  auto& mid1 = *o;
  BOOST_CHECK_EQUAL(mid1.id(), b + 42);
  BOOST_CHECK_EQUAL(mid1[0], 42u);

  o = s.load(256);
  BOOST_REQUIRE(o);
  auto& mid2 = *o;
  BOOST_CHECK_EQUAL(mid2.id(), 256);
  BOOST_CHECK_EQUAL(mid2[0], 256u - b);

  o = s.load(b + 255);
  BOOST_REQUIRE(o);
  auto& last = *o;
  BOOST_CHECK_EQUAL(last.id(), b + 255);
  BOOST_CHECK_EQUAL(last[0], 255u);
}

BOOST_AUTO_TEST_CASE(segment_event_extraction)
{
  segment s;
  {
    segment::writer w{&s, 10};
    for (size_t i = 0; i < 256; ++i)
      BOOST_CHECK(w.write(event{i}));
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
  BOOST_CHECK_EQUAL(*mi, 1000);

  event_id id = s.base();
  while (mi != mend)
  {
    auto e = r.read(*mi);
    BOOST_REQUIRE(e);
    BOOST_CHECK_EQUAL(e->id(), id);
    id += 4;
    ++mi;
  }
}
