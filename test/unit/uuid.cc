#include "test.h"
#include <unordered_set>
#include "vast/uuid.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(uuids)
{
  BOOST_CHECK_EQUAL(sizeof(uuid), 16ul);
  uuid u("01234567-89ab-cdef-0123-456789abcdef");
  BOOST_CHECK_EQUAL(to_string(u), "01234567-89ab-cdef-0123-456789abcdef");

  BOOST_CHECK_EQUAL(std::hash<uuid>()(u), u.hash());
  std::unordered_set<uuid> set;
  set.insert(u);
  BOOST_CHECK(set.find(u) != set.end());
  BOOST_CHECK(*set.begin() == u);
}
