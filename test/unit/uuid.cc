#include "framework/unit.h"

#include <unordered_set>

#include "vast/uuid.h"
#include "vast/print.h"

SUITE("core")

using namespace vast;

TEST("UUID")
{
  CHECK(sizeof(uuid) == 16ul);
  uuid u{"01234567-89ab-cdef-0123-456789abcdef"};
  CHECK(to_string(u) == "01234567-89ab-cdef-0123-456789abcdef");

  std::unordered_set<uuid> set;
  set.insert(u);
  set.insert(uuid::random());
  set.insert(uuid::random());
  CHECK(set.find(u) != set.end());
}
