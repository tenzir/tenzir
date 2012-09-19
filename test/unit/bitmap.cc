#include "test.h"
#include "vast/bitmap.h"
#include "vast/to_string.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(basic_bitmap)
{
  bitmap<int> bm;
  bm.push_back(42);
  bm.push_back(84);
  bm.push_back(42);
  bm.push_back(21);

  std::cerr << to_string(bm[21]->bits()) << std::endl;
  std::cerr << to_string(bm[42]->bits()) << std::endl;
  std::cerr << to_string(bm[84]->bits()) << std::endl;
}

BOOST_AUTO_TEST_CASE(range_encoded_bitmap)
{
  bitmap<int, null_bitstream, range_encoder> bm;
  bm.push_back(42);
  bm.push_back(84);
  bm.push_back(42);
  bm.push_back(21);
  bm.push_back(30);

  std::cerr << to_string(bm[21]->bits()) << std::endl;
  std::cerr << to_string(bm[30]->bits()) << std::endl;
  std::cerr << to_string(bm[42]->bits()) << std::endl;
  std::cerr << to_string(bm[84]->bits()) << std::endl;
}
