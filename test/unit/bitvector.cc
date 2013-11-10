#include "test.h"
#include "vast/bitvector.h"
#include "vast/convert.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(bitvector_to_string)
{
  bitvector a;
  bitvector b{10};
  bitvector c{78, true};

  BOOST_CHECK_EQUAL(to_string(a), "");
  BOOST_CHECK_EQUAL(to_string(b), "0000000000");
  BOOST_CHECK_EQUAL(to_string(c), std::string(78, '1'));
}

BOOST_AUTO_TEST_CASE(bitvector_basic_ops)
{
  bitvector x;
  x.push_back(true);
  x.push_back(false);
  x.push_back(true);

  BOOST_CHECK_EQUAL(x.size(), 3);
  BOOST_CHECK_EQUAL(x.blocks(), 1);

  x.append(0xffff);

  BOOST_REQUIRE_EQUAL(x.blocks(), 2);
  BOOST_CHECK_EQUAL(x.size(), 3 + bitvector::block_width);
}

BOOST_AUTO_TEST_CASE(bitvector_bitwise_ops)
{
  bitvector a{6};
  BOOST_CHECK_EQUAL(a.size(), 6);
  BOOST_CHECK_EQUAL(a.blocks(), 1);

  a.flip(3);
  BOOST_CHECK_EQUAL(to_string(a), "001000");
  BOOST_CHECK_EQUAL(to_string(a << 1), "010000");
  BOOST_CHECK_EQUAL(to_string(a << 2), "100000");
  BOOST_CHECK_EQUAL(to_string(a << 3), "000000");
  BOOST_CHECK_EQUAL(to_string(a >> 1), "000100");
  BOOST_CHECK_EQUAL(to_string(a >> 2), "000010");
  BOOST_CHECK_EQUAL(to_string(a >> 3), "000001");
  BOOST_CHECK_EQUAL(to_string(a >> 4), "000000");

  bitvector b{a};
  b[5] = b[1] = 1;
  BOOST_CHECK_EQUAL(to_string(b), "101010");
  BOOST_CHECK_EQUAL(to_string(~b), "010101");

  BOOST_CHECK_EQUAL(to_string(a | ~b), "011101");
  BOOST_CHECK_EQUAL(to_string((~a << 2) & b), to_string(a));

  BOOST_CHECK_EQUAL(b.count(), 3);

  BOOST_CHECK_EQUAL(to_string(b, false), "010101");
}

BOOST_AUTO_TEST_CASE(bitvector_backward_search)
{
  bitvector x;
  x.append(0xffff);
  x.append(0x30abffff7000ffff);

  auto i = x.find_last();
  BOOST_CHECK_EQUAL(i, 125);
  i = x.find_prev(i);
  BOOST_CHECK_EQUAL(i, 124);
  i = x.find_prev(i);
  BOOST_CHECK_EQUAL(i, 119);
  BOOST_CHECK_EQUAL(x.find_prev(63), 15);

  bitvector y;
  y.append(0xf0ffffffffffff0f);
  BOOST_CHECK_EQUAL(y.find_last(), 63);
  BOOST_CHECK_EQUAL(y.find_prev(59), 55);
}

BOOST_AUTO_TEST_CASE(bitvector_iteration)
{
  bitvector x;
  x.append(0x30abffff7000ffff);

  std::string str;
  std::transform(
      bitvector::const_iterator::begin(x),
      bitvector::const_iterator::end(x),
      std::back_inserter(str),
      [](bitvector::const_reference bit) { return bit ? '1' : '0'; });

  BOOST_CHECK_EQUAL(to_string(x, false), str);

  std::string rts;
  std::transform(
      bitvector::const_iterator::rbegin(x),
      bitvector::const_iterator::rend(x),
      std::back_inserter(rts),
      [](bitvector::const_reference bit) { return bit ? '1' : '0'; });

  std::reverse(str.begin(), str.end());
  BOOST_CHECK_EQUAL(str, rts);

  std::string ones;
  std::transform(
      bitvector::one_const_iterator::begin(x),
      bitvector::one_const_iterator::end(x),
      std::back_inserter(ones),
      [](bitvector::const_reference bit) { return bit ? '1' : '0'; });

  BOOST_CHECK_EQUAL(ones, "111111111111111111111111111111111111111111");

  auto i = bitvector::one_const_iterator::rbegin(x);
  BOOST_CHECK_EQUAL(i.base().pos(), 61);
  ++i;
  BOOST_CHECK_EQUAL(i.base().pos(), 60);
  ++i;
  BOOST_CHECK_EQUAL(i.base().pos(), 55);
  while (i != bitvector::one_const_iterator::rend(x))
    ++i;
  BOOST_CHECK_EQUAL(i.base().pos(), 0);

  auto j = bitvector::one_iterator::rbegin(x);
  BOOST_CHECK_EQUAL(j.base().pos(), 61);
  *j.base() = false;
  ++j;
  *j.base() = false;
  j = bitvector::one_iterator::rbegin(x);
  BOOST_CHECK_EQUAL(j.base().pos(), 55);
}
