#include "test.h"
#include "vast/cow.h"
#include "vast/io/serialization.h"

using namespace vast;

class copyable
{
public:
  copyable() = default;

  copyable(copyable const&)
  {
    ++copies_;
  }

  size_t copies() const
  {
    return copies_;
  }

private:
  static size_t copies_;
};

size_t copyable::copies_ = 0;


BOOST_AUTO_TEST_CASE(copy_on_write)
{
  cow<copyable> c1;
  auto c2 = c1;
  BOOST_CHECK_EQUAL(&c1.read(), &c2.read());
  BOOST_CHECK_EQUAL(c1->copies(), 0);

  // Copies the tuple.
  BOOST_CHECK_EQUAL(c2.write().copies(), 1);

  BOOST_CHECK_EQUAL(c1->copies(), 1);
  BOOST_CHECK_EQUAL(c2->copies(), 1);
  BOOST_CHECK(&c1.read() != &c2.read());
}

BOOST_AUTO_TEST_CASE(cow_serialization)
{
  cow<int> x{42}, y;
  std::vector<uint8_t> buf;
  io::archive(buf, x);
  io::unarchive(buf, y);
  BOOST_CHECK_EQUAL(*x, *y);
}
