#include "test.h"
#include "vast/schema.h"
#include "vast/to_string.h"
#include "vast/fs/fstream.h"
#include "vast/fs/path.h"

// Bring the contents of a file into a std::string.
std::string load(const vast::fs::path& path)
{
  vast::fs::ifstream in(path);

  std::string storage;
  in.unsetf(std::ios::skipws); // No white space skipping.
  std::copy(std::istream_iterator<char>(in),
            std::istream_iterator<char>(),
            std::back_inserter(storage));

  return storage;
}

#define DEFINE_SCHEMA_TEST_CASE(name, input)                        \
  BOOST_AUTO_TEST_CASE(name)                                        \
  {                                                                 \
    vast::schema s0, s1;                                            \
    s0.read(input);                                                 \
                                                                    \
    auto str = vast::to_string(s0);                                 \
    s1.load(str);                                                   \
    BOOST_CHECK_EQUAL(str, vast::to_string(s1));                    \
  }

// Contains the test case defintions for all taxonomy test files.
#include "test/unit/schema_test_cases.h"

BOOST_AUTO_TEST_CASE(offset_computation)
{
  vast::schema schema;
  auto str =
    "type foo: count"
    "event e(r: record{ f: foo, r0: record{ f: foo }, r1: record{ f: foo }})";

  schema.load(str);
  auto& events = schema.events();
  BOOST_REQUIRE_EQUAL(events.size(), 1);
  auto e = events[0];
  auto offsets = vast::schema::offsets(&e, "foo");
  BOOST_CHECK_EQUAL(offsets.size(), 3);
  BOOST_CHECK(offsets[0] == std::vector<size_t>({0, 0}));
  BOOST_CHECK(offsets[1] == std::vector<size_t>({0, 1, 0}));
  BOOST_CHECK(offsets[2] == std::vector<size_t>({0, 2, 0}));
}
