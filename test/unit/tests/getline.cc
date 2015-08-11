#include "vast/io/array_stream.h"
#include "vast/io/getline.h"

#define SUITE IO
#include "test.h"

using namespace vast;

TEST(getline) {
  auto str = "\n1st\nline\rn3\r\nline4\nline5\n\nline6\n";
  for (size_t i = 3; i < 10; ++i) {
    io::array_input_stream input(str, std::strlen(str), i);
    std::string line;
    CHECK(io::getline(input, line));
    CHECK(line == "");
    CHECK(io::getline(input, line));
    CHECK(line == "1st");
    CHECK(io::getline(input, line));
    CHECK(line == "line");
    CHECK(io::getline(input, line));
    CHECK(line == "n3");
    CHECK(io::getline(input, line));
    CHECK(line == "line4");
    CHECK(io::getline(input, line));
    CHECK(line == "line5");
    CHECK(io::getline(input, line));
    CHECK(line == "");
    CHECK(io::getline(input, line));
    CHECK(line == "line6");
  }
}
