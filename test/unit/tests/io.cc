#include <cstring>

#include "vast/io/algorithm.h"
#include "vast/io/buffered_stream.h"
#include "vast/io/iterator.h"
#include "vast/io/formatted.h"
#include "vast/io/container_stream.h"
#include "vast/io/range.h"
#include "vast/io/stream_device.h"

#define SUITE IO
#include "test.h"

using namespace vast;

namespace {

std::vector<uint8_t> data =
{
   0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
  10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
  20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
  30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
  40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
  50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
  60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
  70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
  80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
  90, 91, 92, 93, 94, 95, 96, 97, 98, 99,
};

} // namespace <anonymous>

TEST(container_input_stream) {
  // Test reading from container with a block size that is a multiple of the
  // input.
  {
    auto is = io::make_container_input_stream(data, 10);
    void const* in_buf;
    size_t in_size;
    for (size_t i = 0; i < 10; ++i) {
      REQUIRE(is.next(&in_buf, &in_size));
      CHECK(in_size == 10);
      auto front = reinterpret_cast<uint8_t const*>(in_buf);
      CHECK(*front == i * 10);
    }
    CHECK(!is.next(&in_buf, &in_size));
    CHECK(is.bytes() == 100);
  }
  // Test reading from container with a block size that is *not* a multiple of
  // the input.
  {
    auto is = io::make_container_input_stream(data, 3);
    for (size_t i = 0; i < 33; ++i) {
      auto buf = is.next_block();
      REQUIRE(buf);
      CHECK(buf.size() == 3);
      CHECK(*buf.as<uint8_t>() == i * 3);
    }
    CHECK(is.bytes() == 99);
    auto buf = is.next_block();
    CHECK(buf.size() == 1);
    CHECK(*buf.as<uint8_t>() == 99);
    CHECK(is.bytes() == 100);
    buf = is.next_block();
    CHECK(!buf);
    CHECK(is.bytes() == 100);
  }
}

TEST(container_output_stream) {
  std::string str;
  auto os = io::make_container_output_stream(str);
  if (auto buf = os.next_block())
    for (size_t i = 0; i < buf.size(); ++i)
      *buf.at(i) = i;
  for (size_t i = 0; i < str.size(); ++i)
    CHECK(str[i] == i);
}

TEST(range-based input_stream access) {
  auto is = io::make_container_input_stream(data, 4);
  size_t i = 0;
  for (auto& buf : io::input_stream_range{is}) {
    REQUIRE(buf);
    REQUIRE(buf.size() == 4);
    CHECK(*buf.as<uint8_t>() == i);
    i += buf.size();
  }
}

TEST(istream adapter) {
  std::istringstream iss{"foo"};
  io::istream_device idev{iss};
  io::buffered_input_stream in{idev};

  auto block = in.next_block();
  REQUIRE(block);

  std::string str{block.as<char>(), block.size()};
  CHECK(str == "foo");
}

TEST(ostream adapter) {
  std::string str = "Heiliger Strohsack!";
  std::ostringstream oss;
  io::ostream_device odev{oss};
  io::buffered_output_stream out{odev};

  auto block = out.next_block();
  REQUIRE(block);
  REQUIRE(block.size() > str.size());

  std::copy(str.begin(), str.end(), block.data());
  out.rewind(block.size() - str.size());
  REQUIRE(out.flush());

  CHECK(oss.str().size() == str.size());
  CHECK(oss.str() == str);
}

TEST(input_iterator) {
  decltype(data) buf;
  auto in = io::make_container_input_stream(data, 3);
  io::input_iterator begin{in};
  std::copy(begin, io::input_iterator{}, std::back_inserter(buf));
  CHECK(buf == data);
  CHECK(in.bytes() == data.size());
}

TEST(output_iterator) {
  std::string str;
  auto sink = io::make_container_output_stream(str);

  std::string source = "foobar";
  auto out = std::copy(source.begin(), source.end(), io::output_iterator{sink});

  // It's a bit cumbersome to manually rewind the current block of the output
  // stream, but this is how we do it. An alterantive would be to keep a shared
  // pointer to a buffer such that rewinding only occurs when the last iterator
  // goes out of scope, but iterators should be cheap to create and copy, so
  // we're going for the more verbose version here at the cost of usability.
  out.rewind();

  CHECK(str == source);
  CHECK(sink.bytes() == source.size());
}

TEST(formatted output) {
  std::string sink;
  auto out = io::make_container_output_stream(sink);
  out << 42;
  CHECK(sink == "42");
  out << " " << 43;
  CHECK(sink == "42 43");
}

TEST(formatted input) {
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
