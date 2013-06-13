#include "test.h"
#include "vast/intrusive.h"

using namespace vast;

struct T : public intrusive_base<T>
{
  int i = 42;
  std::string s = "Hier steppt der Baer!";
  std::vector<int> v{1, 2, 3, 4, 5};
};

BOOST_AUTO_TEST_CASE(auto_reffing)
{
  intrusive_ptr<T> x;
  BOOST_CHECK(! x);

  x = new T;
  BOOST_CHECK_EQUAL(x->ref_count(), 1);

  {
    auto y = x;
    BOOST_CHECK_EQUAL(x->ref_count(), 2);
    BOOST_CHECK_EQUAL(y->ref_count(), 2);
  }

  BOOST_CHECK_EQUAL(x->ref_count(), 1);
}

BOOST_AUTO_TEST_CASE(manual_reffing)
{
  T* raw;
  intrusive_ptr<T> x;
  BOOST_CHECK(! x);

  x = new T;
  BOOST_CHECK(x);
  BOOST_CHECK_EQUAL(x->ref_count(), 1);

  raw = x.get();
  ref(raw);
  BOOST_CHECK_EQUAL(x->ref_count(), 2);

  unref(raw);
  BOOST_CHECK_EQUAL(x->ref_count(), 1);

  auto ptr = x.release();
  BOOST_CHECK_EQUAL(ptr, raw);
  unref(raw); // Deletes x.
}
