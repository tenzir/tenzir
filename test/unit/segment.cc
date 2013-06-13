#include "test.h"
#include "vast/event.h"
#include "vast/segment.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(segment_operations)
{
  segment s;
  segment::writer w(&s);
  for (size_t i = 0; i < (1 << 10); ++i)
  {
    w << event{42, i, std::to_string(i)};
    if ((i + 1) % (1 << 8) == 0)
      w.flush();
  }

  segment::reader r(&s);
  size_t n = 0;
  while (r)
  {
    event e;
    r >> e;
    BOOST_CHECK_EQUAL(e, (event{42, n, std::to_string(n)}));
    ++n;
  }

  BOOST_CHECK_EQUAL(n, 1 << 10);
}
