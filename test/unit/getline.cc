#include "test.h"
#include <vast/io/array_stream.h>
#include <vast/io/getline.h>

using namespace vast;

BOOST_AUTO_TEST_CASE(vast_io_getline)
{
  auto str = "\n1st\nline\rn3\r\nline4\nline5\n\nline6\n";
  for (size_t i = 3; i < 10; ++i)
  {
    io::array_input_stream input(str, std::strlen(str), i);
    std::string line;
    BOOST_CHECK_EQUAL(io::getline(input, line), true);
    BOOST_CHECK_EQUAL(line, "");
    BOOST_CHECK_EQUAL(io::getline(input, line), true);
    BOOST_CHECK_EQUAL(line, "1st");
    BOOST_CHECK_EQUAL(io::getline(input, line), true);
    BOOST_CHECK_EQUAL(line, "line");
    BOOST_CHECK_EQUAL(io::getline(input, line), true);
    BOOST_CHECK_EQUAL(line, "n3");
    BOOST_CHECK_EQUAL(io::getline(input, line), true);
    BOOST_CHECK_EQUAL(line, "line4");
    BOOST_CHECK_EQUAL(io::getline(input, line), true);
    BOOST_CHECK_EQUAL(line, "line5");
    BOOST_CHECK_EQUAL(io::getline(input, line), true);
    BOOST_CHECK_EQUAL(line, "");
    BOOST_CHECK_EQUAL(io::getline(input, line), true);
    BOOST_CHECK_EQUAL(line, "line6");
    BOOST_CHECK_EQUAL(io::getline(input, line), false);
  }
}
