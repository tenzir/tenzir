#include "test.h"

#include "vast/chunk.h"
#include "vast/event.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(chunking)
{
  chunk chk;

  // Upon destruction, the writer's IO streams flush their state into the
  // referenced chunk.
  {
    chunk::writer w(chk);
    for (size_t i = 0; i < 1e3; ++i)
      BOOST_CHECK(w.write(event{i}));
    BOOST_CHECK(chk.size() == 1e3);
  }

  chunk::reader r(chk);
  for (size_t i = 0; i < 1e3; ++i)
  {
    event e;
    BOOST_CHECK(r.read(e));
    BOOST_CHECK(e == event{i});
  }

  chunk copy(chk);
  BOOST_CHECK(chk == copy);
}
