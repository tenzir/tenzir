#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE "VAST Unit Test Suite"

#include <cstring>
#include "test.h"
#include "vast/configuration.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/type_info.h"

int main(int argc, char* argv[])
{
  using namespace vast;

  announce_builtin_types();

  auto delete_logs = true;
  if (argc >= 2 && ! std::strcmp(argv[1], "keep"))
    delete_logs = false;

  auto log_dir = "/tmp/vast-unit-test";
  if (! logger::instance()->init(logger::quiet, logger::trace, true, log_dir))
  {
    std::cerr << "failed to initialize logger" << std::endl;
    return 1;
  }

  auto rc = boost::unit_test::unit_test_main(&init_unit_test, argc, argv);
  if (rc)
    std::cerr << "unit test suite failed" << std::endl;

  if (delete_logs)
    rm(log_dir);

  return rc;
}
