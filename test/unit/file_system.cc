#include "test.h"
#include "vast/file_system.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(path_operations)
{
  path p = "/usr/local/bin/foo";
  BOOST_CHECK_EQUAL(p.parent(), "/usr/local/bin");
  BOOST_CHECK_EQUAL(p.basename(), "foo");
  BOOST_CHECK_EQUAL(path("/usr/local/bin/foo.bin").basename(true), "foo");

  BOOST_CHECK_EQUAL(p.root(), "/");
  BOOST_CHECK_EQUAL(path("usr/local").root(), "");

  BOOST_CHECK_EQUAL(p.complete(), p);
  BOOST_CHECK_EQUAL(path("foo/").complete(), path::current() / "foo/");

  auto pieces = p.split();
  BOOST_REQUIRE_EQUAL(pieces.size(), 5);
  BOOST_CHECK_EQUAL(pieces[0], "/");
  BOOST_CHECK_EQUAL(pieces[1], "usr");
  BOOST_CHECK_EQUAL(pieces[2], "local");
  BOOST_CHECK_EQUAL(pieces[3], "bin");
  BOOST_CHECK_EQUAL(pieces[4], "foo");
}

BOOST_AUTO_TEST_CASE(path_trimming)
{
  path p = "/usr/local/bin/foo";

  BOOST_CHECK_EQUAL(p.trim(0), "");
  BOOST_CHECK_EQUAL(p.trim(1), "/");
  BOOST_CHECK_EQUAL(p.trim(2), "/usr");
  BOOST_CHECK_EQUAL(p.trim(3), "/usr/local");
  BOOST_CHECK_EQUAL(p.trim(4), "/usr/local/bin");
  BOOST_CHECK_EQUAL(p.trim(5), p);
  BOOST_CHECK_EQUAL(p.trim(6), p);
  BOOST_CHECK_EQUAL(p.trim(-1), "foo");
  BOOST_CHECK_EQUAL(p.trim(-2), "bin/foo");
  BOOST_CHECK_EQUAL(p.trim(-3), "local/bin/foo");
  BOOST_CHECK_EQUAL(p.trim(-4), "usr/local/bin/foo");
  BOOST_CHECK_EQUAL(p.trim(-5), p);
  BOOST_CHECK_EQUAL(p.trim(-6), p);
}

BOOST_AUTO_TEST_CASE(path_chopping)
{
  path p = "/usr/local/bin/foo";

  BOOST_CHECK_EQUAL(p.chop(0), p);
  BOOST_CHECK_EQUAL(p.chop(-1), "/usr/local/bin");
  BOOST_CHECK_EQUAL(p.chop(-2), "/usr/local");
  BOOST_CHECK_EQUAL(p.chop(-3), "/usr");
  BOOST_CHECK_EQUAL(p.chop(-4), "/");
  BOOST_CHECK_EQUAL(p.chop(-5), "");
  BOOST_CHECK_EQUAL(p.chop(1), "usr/local/bin/foo");
  BOOST_CHECK_EQUAL(p.chop(2), "local/bin/foo");
  BOOST_CHECK_EQUAL(p.chop(3), "bin/foo");
  BOOST_CHECK_EQUAL(p.chop(4), "foo");
  BOOST_CHECK_EQUAL(p.chop(5), "");
}

BOOST_AUTO_TEST_CASE(basic_filesystem_tests)
{
  using std::to_string;
  path base = "vast-unit-test-file-system-test";
  path p("/tmp");
  p /= base / string(to_string(getpid()));
  BOOST_CHECK(! p.is_regular_file());
  BOOST_CHECK(! exists(p));
  BOOST_CHECK(mkdir(p));
  BOOST_CHECK(exists(p));
  BOOST_CHECK(p.is_directory());
  BOOST_CHECK(rm(p));
  BOOST_CHECK(! p.is_directory());
  BOOST_CHECK(p.parent().is_directory());
  BOOST_CHECK(rm(p.parent()));
  BOOST_CHECK(! p.parent().is_directory());
}
