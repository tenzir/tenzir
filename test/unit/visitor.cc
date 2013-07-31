#include "test.h"
#include "vast/util/visitor.h"

struct base;
struct t1;
struct t2;

typedef vast::util::const_visitor<base, t1, t2> concrete_visitor;

struct base
{
  VAST_ACCEPT(concrete_visitor)
  VAST_ACCEPT_CONST(concrete_visitor)
};

struct t1 : base
{
  VAST_ACCEPT(concrete_visitor)
  VAST_ACCEPT_CONST(concrete_visitor)

  int i = 42;
};

struct t2 : base
{
  VAST_ACCEPT(concrete_visitor)
  VAST_ACCEPT_CONST(concrete_visitor)

  double d = 4.2;
};

struct printer : concrete_visitor
{
  virtual void visit(base const&)
  {
    BOOST_CHECK(! "should not be invoked");
  }

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

  base* b1 = &x;
  base* b2 = &y;

  printer p;
  b1->accept(p);
  b2->accept(p);
}
