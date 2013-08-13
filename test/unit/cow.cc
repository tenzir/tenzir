#include "test.h"
#include "vast/cow.h"
#include "vast/io/container_stream.h"

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
  BOOST_CHECK_EQUAL(&cget(c1), &cget(c2));
  BOOST_CHECK_EQUAL(cget(c1).copies(), 0);

  // Copies the tuple.
  BOOST_CHECK_EQUAL(get(c2).copies(), 1);

  BOOST_CHECK_EQUAL(cget(c1).copies(), 1);
  BOOST_CHECK_EQUAL(cget(c2).copies(), 1);
  BOOST_CHECK(&cget(c1) != &cget(c2));
}

BOOST_AUTO_TEST_CASE(cow_serialization)
{
  cow<int> x{42};
  std::vector<uint8_t> buf;

  {
    auto sink = io::make_container_output_stream(buf);
    binary_serializer serializer(sink);
    serializer << x;
  }

  cow<int> y;

  {
    auto source = io::make_container_input_stream(buf);
    binary_deserializer deserializer(source);
    deserializer >> y;
  }

  BOOST_CHECK_EQUAL(cget(x), cget(y));
}
