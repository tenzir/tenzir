#include "test.h"
#include <unordered_set>
#include "vast/uuid.h"
#include "vast/util/convert.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(uuids)
{
  BOOST_CHECK_EQUAL(sizeof(uuid), 16ul);
  uuid u{"01234567-89ab-cdef-0123-456789abcdef"};
  BOOST_CHECK_EQUAL(to<std::string>(u), "01234567-89ab-cdef-0123-456789abcdef");

  std::unordered_set<uuid> set;
  set.insert(u);
  set.insert(uuid::random());
  set.insert(uuid::random());
  BOOST_CHECK(set.find(u) != set.end());
}
