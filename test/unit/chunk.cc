#include "framework/unit.h"
#include "vast/chunk.h"
#include "vast/event.h"

using namespace vast;

TEST("chunks")
{
  chunk chk;

  {
    chunk::writer w(chk);
    for (size_t i = 0; i < 1e3; ++i)
      CHECK(w.write(event{i}));

    CHECK(chk.elements() == 1e3);

    // Upon destruction, the writer's IO streams flush their state into the
    // referenced chunk.
  }

  chunk::reader r(chk);
  for (size_t i = 0; i < 1e3; ++i)
  {
    event e;
    CHECK(r.read(e));
    CHECK(e == event{i});
  }

  chunk copy(chk);
  CHECK(chk == copy);
}
