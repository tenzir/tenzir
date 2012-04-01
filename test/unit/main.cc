#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE "VAST Unit Test Suite"

#include <boost/test/unit_test.hpp>
#include "vast/program.h"
#include "vast/util/logger.h"

vast::program VAST;

int main(int argc, char* argv[])
{
    if (! VAST.init(argc, argv))
        std::exit(-1);

    // TODO: Once the new logger is in place, send the unit test output to it.
    //boost::unit_test::unit_test_log.set_stream(std::ostream& str);

    char const* args[] = {"", "--log_level=test_suite"};
    auto rc = boost::unit_test::unit_test_main(
        &init_unit_test,
        sizeof(args) / sizeof(char*),
        const_cast<char**>(args));

    if (rc)
        LOG(error, core) << "unit test suite exited with error code " << rc;

    return rc;
}
