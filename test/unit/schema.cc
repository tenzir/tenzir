#include "test.h"
#include <fstream>
#include "vast/schema.h"
#include "vast/util/convert.h"

using namespace vast;

// Bring the contents of a file into a std::string.
std::string load(const std::string& path)
{
  std::ifstream in(path);
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
    auto s0 = schema::read(input);                                  \
    BOOST_REQUIRE(s0);                                              \
                                                                    \
    auto str = to_string(*s0);                                      \
    auto s1 = schema::load(str);                                    \
    BOOST_REQUIRE(s1);                                              \
    BOOST_CHECK_EQUAL(str, to_string(*s1));                         \
  }

// Contains the test case defintions for all taxonomy test files.
#include "test/unit/schema_test_cases.h"

BOOST_AUTO_TEST_CASE(offset_computation)
{
  auto str =
    "type foo: count"
    "event e(r: record{ f: foo, r0: record{ f: foo }, r1: record{ f: foo }})";

  auto sch = schema::load(str);
  BOOST_REQUIRE(sch);
  auto offsets = schema::symbol_offsets(&sch->events()[0], {"foo"});
  BOOST_REQUIRE(offsets);
  BOOST_REQUIRE_EQUAL(offsets->size(), 3);
  BOOST_CHECK((*offsets)[0] == std::vector<size_t>({0, 0}));
  BOOST_CHECK((*offsets)[1] == std::vector<size_t>({0, 1, 0}));
  BOOST_CHECK((*offsets)[2] == std::vector<size_t>({0, 2, 0}));

  str =
    "type foo: record{ a: int, b: int, c: record{ x: int, y: addr, z: int}}"
    "event e(r: record{ f: foo, r0: record{ f: foo }, r1: record{ f: foo }})";

  sch = schema::load(str);
  BOOST_REQUIRE(sch);
  offsets = schema::symbol_offsets(&sch->events()[0], {"foo", "c", "y"});
  BOOST_REQUIRE(offsets);
  BOOST_REQUIRE_EQUAL(offsets->size(), 3);
  BOOST_CHECK((*offsets)[0] == std::vector<size_t>({0, 0, 2, 1}));
  BOOST_CHECK((*offsets)[1] == std::vector<size_t>({0, 1, 0, 2, 1}));
  BOOST_CHECK((*offsets)[2] == std::vector<size_t>({0, 2, 0, 2, 1}));
  //for (auto& inner : offsets)
  //{
  //  for (auto& i : inner)
  //    std::cout << i << " ";

  //  std::cout << std::endl;
  //}
}
