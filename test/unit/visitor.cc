#include "test.h"
#include "vast/util/visitor.h"

using namespace vast;

struct t1;
struct t2;
using concrete_visitor = util::const_visitor<t1, t2>;

struct t : util::visitable_with<concrete_visitor>
{
  virtual ~t() = default;
};

struct t1 : util::visitable<t, t1, concrete_visitor>
{
  int i = 42;
};

struct t2 : util::visitable<t, t2, concrete_visitor>
{
  double d = 4.2;
};

struct checker : concrete_visitor
{
  virtual void visit(t1 const& x)
  {
    BOOST_CHECK_EQUAL(x.i, 42);
  }

  virtual void visit(t2 const& x)
  {
    BOOST_CHECK_EQUAL(x.d, 4.2);
  }
};

BOOST_AUTO_TEST_CASE(visitor)
{
  t1 x;
  t2 y;
  t* b1 = &x;
  t* b2 = &y;
  checker v;
  b1->accept(v);
  b2->accept(v);
}
