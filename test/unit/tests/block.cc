#include "vast/block.h"
#include "vast/event.h"
#include "vast/concept/serializable/builtin.h"

#include "test.h"

using namespace vast;

TEST(block)
{
  block blk;
  // Upon destruction, the writer's IO streams flush their state into the
  // referenced block.
  {
    block::writer w{blk};
    for (size_t i = 0; i < 1e3; ++i)
      CHECK(w.write(i));
    CHECK(blk.elements() == 1e3);
    MESSAGE("flushing block");
  }
  MESSAGE("reading block");
  block::reader r{blk};
  for (size_t i = 0; i < 1e3; ++i)
  {
    size_t j;
    CHECK(r.read(j));
    CHECK(j == i);
  }
  MESSAGE("copying block");
  block copy{blk};
  CHECK(blk == copy);
}
