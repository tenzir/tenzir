#include "test.h"

#include "vast/logger.h"

#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_TRACE

using namespace vast;

int foo()
{
  VAST_ENTER();
  VAST_RETURN(-1);
  return -1;
};

void bar(int i, std::string s, char c)
{
  VAST_ENTER_WITH(VAST_ARG(i, s, c));
  VAST_MSG("about to call foo");
  foo();
  VAST_LEAVE("returning with a message");
};

TEST(tracing)
{
  foo();
  bar(42, "***", 'A');
}

#endif
