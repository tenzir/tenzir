#include <unordered_set>

#include "vast/uuid.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/uuid.hpp"

#include "test.hpp"

using namespace vast;

TEST(UUID) {
  CHECK(sizeof(uuid) == 16ul);
  auto u = to<uuid>("01234567-89ab-cdef-0123-456789abcdef");
  REQUIRE(u);
  CHECK(to_string(*u) == "01234567-89ab-cdef-0123-456789abcdef");

  std::unordered_set<uuid> set;
  set.insert(*u);
  set.insert(uuid::random());
  set.insert(uuid::random());
  CHECK(set.find(*u) != set.end());
}
