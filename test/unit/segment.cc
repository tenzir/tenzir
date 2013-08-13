#include "test.h"
#include "vast/event.h"
#include "vast/segment.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(segment_operations)
{
  segment s1;

  /// Construct a writer with 256 events per chunk and no upper bound on the
  /// total segment size.
  segment::writer w(&s1, 256 /*, 0*/);

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
  while (r1.read(e))
    BOOST_CHECK_EQUAL(e, (event{n++}));
  BOOST_CHECK_EQUAL(n, 1124);

  segment::reader r2(&s2);
  n = 0;
  while (r2.read(e))
    BOOST_CHECK_EQUAL(e, (event{n++}));
  BOOST_CHECK_EQUAL(n, 50);
}
