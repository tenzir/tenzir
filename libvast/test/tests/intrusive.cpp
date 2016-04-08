#include "vast/util/intrusive.hpp"

#define SUITE util
#include "test.hpp"

using namespace vast;

struct T : public intrusive_base<T> {
  int i = 42;
  std::string s = "Hier steppt der Baer!";
  std::vector<int> v{1, 2, 3, 4, 5};
};

TEST(intrusive_ptr_automatic_reffing) {
  intrusive_ptr<T> x;
  CHECK(!x);

  x = new T;
  CHECK(x->ref_count() == 1);

  {
    auto y = x;
    CHECK(x->ref_count() == 2);
    CHECK(y->ref_count() == 2);
  }

  CHECK(x->ref_count() == 1);
}

TEST(intrusive_ptr_manual_reffing) {
  T* raw;
  intrusive_ptr<T> x;
  CHECK(!x);

  x = new T;
  CHECK(x);
  CHECK(x->ref_count() == 1);

  raw = x.get();
  ref(raw);
  CHECK(x->ref_count() == 2);

  unref(raw);
  CHECK(x->ref_count() == 1);

  auto ptr = x.release();
  CHECK(ptr == raw);
  unref(raw); // Deletes x.
}
