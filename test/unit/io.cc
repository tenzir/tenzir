#include "framework/unit.h"

#include "vast/io/formatted.h"
#include "vast/io/container_stream.h"

using namespace vast;

SUITE("I/O")

TEST("formatted output")
{
  std::string sink;
  auto out = io::make_container_output_stream(sink);
  out << 42;
  CHECK(sink == "42");
  out << " " << 43;
  CHECK(sink == "42 43");
}

TEST("formatted input")
{
  std::string source("42 43 foo bar");
  auto in = io::make_container_input_stream(source);
  int i;
  in >> i;
  CHECK(i == 42);
  in >> i;
  CHECK(i == 43);
  std::string str;
  in >> str;
  CHECK(str == "foo");
  in >> str;
  CHECK(str == "bar");
}
