#include "framework/unit.h"
#include "vast/block.h"
#include "vast/event.h"
#include "vast/serialization/arithmetic.h"

using namespace vast;

SUITE("core")

TEST("block")
{
  block blk;

  // Upon destruction, the writer's IO streams flush their state into the
  // referenced block.
  {
    block::writer w{blk};
    for (size_t i = 0; i < 1e3; ++i)
      CHECK(w.write(i));

    CHECK(blk.elements() == 1e3);
  }

  block::reader r{blk};
  for (size_t i = 0; i < 1e3; ++i)
  {
    size_t j;
    CHECK(r.read(j));
    CHECK(j == i);
  }

  block copy{blk};
  CHECK(blk == copy);
}
