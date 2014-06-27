#include "framework/unit.h"

#include <forward_list>
#include "vast/util/alloc.h"

SUITE("util")

using namespace vast;

TEST("stack allocator")
{
  using allocator = util::stack_alloc<uint64_t, 16>;
  using short_list = std::forward_list<uint64_t, allocator>;

  short_list list;
  list.push_front(21); // On the stack.

  // Arena is now full.
  CHECK(list.get_allocator().arena().used() ==
        list.get_allocator().arena().size());

  // On the heap.
  list.push_front(42);
  list.push_front(84);
  list.push_front(168);
}
