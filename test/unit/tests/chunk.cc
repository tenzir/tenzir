#include "framework/unit.h"
#include "vast/chunk.h"
#include "vast/event.h"

using namespace vast;

TEST("chunks")
{
  chunk chk;

  auto t = type::integer{};
  t.name("i");

  // Upon destruction, the writer's IO streams flush their state into the
  // referenced chunk.
  {
    chunk::writer w(chk);
    for (size_t i = 0; i < 1e3; ++i)
      CHECK(w.write(event{i, t}));

    CHECK(chk.elements() == 1e3);
  }

  chunk::reader r(chk);
  for (size_t i = 0; i < 1e3; ++i)
  {
    event e;
    CHECK(r.read(e));
    CHECK(e == event{i, t});
  }

  chunk copy{chk};
  CHECK(chk == copy);
}
