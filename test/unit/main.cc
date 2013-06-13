#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE "VAST Unit Test Suite"

#include "test.h"
#include "vast/configuration.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/detail/cppa_type_info.h"

int main(int argc, char* argv[])
{
  using namespace vast;
  detail::cppa_announce_types();

  configuration config;
  config.load("/dev/null");

  auto log_filename = "/tmp/vast_unit_test.log";
  logger::instance()->init(logger::error, logger::error, log_filename);

  //boost::unit_test::unit_test_log.set_stream(logger::instance()->console());
  auto rc = boost::unit_test::unit_test_main(&init_unit_test, argc, argv);

  if (rc)
    VAST_LOG_ERROR("unit test suite exited with error code " << rc);

  rm(log_filename);
  return rc;
}
