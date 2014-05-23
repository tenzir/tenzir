#include "framework/unit.h"
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

SUITE("util")

TEST("copy-on-write")
{
  cow<copyable> c1;
  auto c2 = c1;
  CHECK(&c1.read() == &c2.read());
  CHECK(c1->copies() == 0);

  // Copies the tuple.
  CHECK(c2.write().copies() == 1);

  CHECK(c1->copies() == 1);
  CHECK(c2->copies() == 1);
  CHECK(&c1.read() != &c2.read());
}

TEST("copy-on-write serialization")
{
  cow<int> x{42}, y;
  std::vector<uint8_t> buf;
  io::archive(buf, x);
  io::unarchive(buf, y);
  CHECK(*x == *y);
}
