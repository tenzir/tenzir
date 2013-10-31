#include "test.h"
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
    BOOST_CHECK(w.write(event{i}));
  }

  // At this point, the writer has still 100 events that have not yet been
  // flushed. We can either (1) simply flush the remaining events, or (2)
  // attach the writer to a different segment.
  //
  // Let's begin with the first option.
  BOOST_CHECK(w.flush());
  BOOST_REQUIRE_EQUAL(s1.events(), 1124);

  // Then add some more events, and attempt the second option.
  for (size_t i = 0; i < 50; ++i)
    BOOST_CHECK(w.write(event{i}));

  segment s2;
  w.attach_to(&s2);
  BOOST_CHECK(w.flush());
  BOOST_REQUIRE_EQUAL(s2.events(), 50);

  segment::reader r1(&s1);
  event e;
  size_t n = 0;
  while (r1.read(&e))
    BOOST_CHECK_EQUAL(e, (event{n++}));
  BOOST_CHECK_EQUAL(n, 1124);

  segment::reader r2(&s2);
  n = 0;
  while (r2.read(&e))
    BOOST_CHECK_EQUAL(e, (event{n++}));
  BOOST_CHECK_EQUAL(n, 50);
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
  event e;
  BOOST_CHECK(r.seek(1042));
  BOOST_CHECK(r.read(&e));
  BOOST_CHECK_EQUAL(e[0], 1042);
  BOOST_CHECK(r.seek(1010));
  BOOST_CHECK(r.read(&e));
  BOOST_CHECK_EQUAL(e[0], 1010);
  BOOST_CHECK(! r.seek(10));
  BOOST_CHECK(r.seek(1011));
  BOOST_CHECK(r.read(&e));
  BOOST_CHECK_EQUAL(e[0], 1011);
  BOOST_CHECK(r.seek(1720));
  BOOST_CHECK(r.read(&e));
  BOOST_CHECK_EQUAL(e[0], 1720);
  BOOST_CHECK(! r.seek(2024));
  BOOST_CHECK(r.seek(2023));
  BOOST_CHECK(r.read(&e));
  BOOST_CHECK_EQUAL(e[0], 2023);
}

BOOST_AUTO_TEST_CASE(segment_event_extraction)
{
  segment s1;
  {
    segment::writer w(&s1, 10);
    for (size_t i = 0; i < 256; ++i)
      BOOST_CHECK(w.write(event{i}));
  }
  BOOST_CHECK_EQUAL(s1.events(), 256);

  auto b = 42u;
  s1.base(b);

  auto o = s1.load(b);
  BOOST_REQUIRE(o);
  auto& first = *o;
  BOOST_CHECK_EQUAL(first.id(), b);
  BOOST_CHECK_EQUAL(first[0], 0u);

  o = s1.load(b + 42);
  BOOST_REQUIRE(o);
  auto& mid1 = *o;
  BOOST_CHECK_EQUAL(mid1.id(), b + 42);
  BOOST_CHECK_EQUAL(mid1[0], 42u);

  o = s1.load(256);
  BOOST_REQUIRE(o);
  auto& mid2 = *o;
  BOOST_CHECK_EQUAL(mid2.id(), 256);
  BOOST_CHECK_EQUAL(mid2[0], 256u - b);

  o = s1.load(b + 255);
  BOOST_REQUIRE(o);
  auto& last = *o;
  BOOST_CHECK_EQUAL(last.id(), b + 255);
  BOOST_CHECK_EQUAL(last[0], 255u);
}
