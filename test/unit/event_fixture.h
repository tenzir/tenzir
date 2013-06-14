#ifndef VAST_TEST_UNIT_EVENT_FIXTURE_H
#define VAST_TEST_UNIT_EVENT_FIXTURE_H

#include <vector>
#include "vast/event.h"

struct event_fixture
{
  event_fixture();
  std::vector<vast::event> events;
};

#endif
