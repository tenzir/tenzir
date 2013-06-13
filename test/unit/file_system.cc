#include "test.h"
#include "vast/file_system.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(basic_filesystem_tests)
{
  using std::to_string;
  path p("/tmp");
  p /= "vast-unit-test-dir";
  p /= string(to_string(getpid()));
  BOOST_CHECK(! p.is_regular_file());
  BOOST_CHECK(! exists(p));
  BOOST_CHECK(mkdir(p));
  BOOST_CHECK(exists(p));
  BOOST_CHECK(p.is_directory());
  BOOST_CHECK(rm(p));
  BOOST_CHECK(! p.is_directory());
}
