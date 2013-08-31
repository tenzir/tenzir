#include "test.h"
#include "vast/io/formatted.h"
#include "vast/io/container_stream.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(formatted_output)
{
  std::string sink;
  auto out = io::make_container_output_stream(sink);
  out << 42;
  BOOST_CHECK_EQUAL(sink, "42");
  out << " " << 43;
  BOOST_CHECK_EQUAL(sink, "42 43");
}

BOOST_AUTO_TEST_CASE(formatted_input)
{
  std::string source("42 43 foo bar");
  auto in = io::make_container_input_stream(source);
  int i;
  in >> i;
  BOOST_CHECK_EQUAL(i, 42);
  in >> i;
  BOOST_CHECK_EQUAL(i, 43);
  std::string str;
  in >> str;
  BOOST_CHECK_EQUAL(str, "foo");
  in >> str;
  BOOST_CHECK_EQUAL(str, "bar");
}
