#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE "VAST Unit Test Suite"

#include <boost/test/unit_test.hpp>
#include "vast/program.h"
#include "vast/logger.h"

int main(int argc, char* argv[])
{
  vast::configuration config;
  if (! config.load())
    std::exit(1);

  vast::program program(config);
  program.start();

  boost::unit_test::unit_test_log.set_stream(vast::LOGGER->console());

  char const* args[] = {"", "--log_level=test_suite"};
  auto rc = boost::unit_test::unit_test_main(
      &init_unit_test,
      sizeof(args) / sizeof(char*),
      const_cast<char**>(args));

  if (rc)
    LOG(error, core) << "unit test suite exited with error code " << rc;

  program.stop();

  return rc;
}
