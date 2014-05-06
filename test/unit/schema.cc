#include "test.h"
#include "vast/file_system.h"
#include "vast/schema.h"
#include "vast/io/serialization.h"
#include "vast/util/convert.h"

using namespace vast;

#define DEFINE_SCHEMA_TEST_CASE(name, input)                        \
  BOOST_AUTO_TEST_CASE(name)                                        \
  {                                                                 \
    auto contents = load(input);                                    \
    BOOST_REQUIRE(contents);                                        \
    auto lval = contents->begin();                                  \
    auto s0 = parse<schema>(lval, contents->end());                 \
    BOOST_REQUIRE(s0);                                              \
                                                                    \
    auto str = to_string(*s0);                                      \
    lval = str.begin();                                             \
    auto s1 = parse<schema>(lval, str.end());                       \
    BOOST_REQUIRE(s0);                                              \
    BOOST_CHECK_EQUAL(str, to_string(*s1));                         \
  }

// Contains the test case defintions for all taxonomy test files.
#include "test/unit/schema_test_cases.h"

BOOST_AUTO_TEST_CASE(schema_serialization)
{
  schema sch;
  std::vector<argument> args;
  args.emplace_back("s1", type::make<string_type>());
  args.emplace_back("d1", type::make<double_type>());
  args.emplace_back("c", type::make<uint_type>());
  args.emplace_back("i", type::make<int_type>());
  args.emplace_back("s2", type::make<string_type>());
  args.emplace_back("d2", type::make<double_type>());
  sch.add(type::make<record_type>("foo", std::move(args)));

  std::vector<uint8_t> buf;
  BOOST_CHECK(io::archive(buf, sch));

  schema sch2;
  BOOST_CHECK(io::unarchive(buf, sch2));

  BOOST_CHECK(sch2.find_type("foo"));
  BOOST_CHECK_EQUAL(to_string(sch), to_string(sch2));
}

BOOST_AUTO_TEST_CASE(offset_finding)
{
  std::string str =
    "type a : int\n"
    "type inner : record{ x: int, y: double }\n"
    "type middle : record{ a: int, b: inner }\n"
    "type outer : record{ a: middle, b: record { y: string }, c: int }\n"
    "type foo : record{ a: int, b: double, c: outer, d: middle }";

  auto lval = str.begin();
  auto sch = parse<schema>(lval, str.end());
  BOOST_REQUIRE(sch);

  //auto offs = sch->find_offsets({"a"});
  //decltype(offs) expected;
  //expected.emplace(sch->find_type("outer"), offset{0, 0});
  //expected.emplace(sch->find_type("middle"), offset{0});
  //expected.emplace(sch->find_type("foo"), offset{0});
  //expected.emplace(sch->find_type("foo"), offset{2, 0, 0});
  //expected.emplace(sch->find_type("foo"), offset{3, 0});
  //BOOST_CHECK(offs == expected);

  //offs = sch->find_offsets({"b", "y"});
  //expected.clear();
  //expected.emplace(sch->find_type("foo"), offset{2, 0, 1, 1});
  //expected.emplace(sch->find_type("foo"), offset{2, 1, 0});
  //expected.emplace(sch->find_type("foo"), offset{3, 1, 1});
  //expected.emplace(sch->find_type("middle"), offset{1, 1});
  //expected.emplace(sch->find_type("outer"), offset{0, 1, 1});
  //expected.emplace(sch->find_type("outer"), offset{1, 0});
  //BOOST_CHECK(offs == expected);

  auto foo = util::get<record_type>(sch->find_type("foo")->info());
  BOOST_REQUIRE(foo);

  auto t = foo->at(offset{0});
  BOOST_REQUIRE(t);
  BOOST_CHECK(t->info() == type::make<int_type>()->info());

  t = foo->at(offset{2, 0, 1, 1});
  BOOST_REQUIRE(t);
  BOOST_CHECK(t->info() == type::make<double_type>()->info());

  t = foo->at(offset{2, 0, 1});
  BOOST_REQUIRE(t);
  BOOST_CHECK_EQUAL(t->name(), "inner");
  BOOST_CHECK(util::get<record_type>(t->info()));
}

BOOST_AUTO_TEST_CASE(merging)
{
  std::string str =
    "type a : int\n"
    "type inner : record { x: int, y: double }\n";

  auto lval = str.begin();
  auto s1 = parse<schema>(lval, str.end());
  BOOST_REQUIRE(s1);

  str =
    "type a : int\n"  // Same type allowed.
    "type b : int\n";

  lval = str.begin();
  auto s2 = parse<schema>(lval, str.end());
  BOOST_REQUIRE(s2);

  auto merged = schema::merge(*s1, *s2);
  BOOST_REQUIRE(merged);
  BOOST_CHECK(merged->find_type("a"));
  BOOST_CHECK(merged->find_type("b"));
  BOOST_CHECK(merged->find_type("inner"));
}
