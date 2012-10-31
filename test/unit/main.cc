#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE "VAST Unit Test Suite"

#include "test.h"
#include "vast/configuration.h"
#include "vast/logger.h"

int main(int argc, char* argv[])
{
  vast::configuration config;
  config.load("/dev/null");
  vast::init(config);

  boost::unit_test::unit_test_log.set_stream(vast::logger::get()->console());
  char const* args[] = {"", "--log_level=test_suite"};
  auto rc = boost::unit_test::unit_test_main(
      &init_unit_test,
      sizeof(args) / sizeof(char*),
      const_cast<char**>(args));

  if (rc)
    LOG(error, core) << "unit test suite exited with error code " << rc;

  return rc;
}
