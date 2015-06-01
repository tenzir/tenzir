#ifndef FIXTURES_EVENTS_H
#define FIXTURES_EVENTS_H

#include <vector>

#include "vast/event.h"

using namespace vast;

namespace fixtures {

struct simple_events
{
  simple_events()
    : events0(512),
      events1(2048),
      events(1024)
  {
    type0 = type::record{{"c", type::count{}}, {"s", type::string{}}};
    type0.name("test_record_event");
    type1 = type::record{{"r", type::real{}}, {"s", type::boolean{}}};
    type1.name("test_record_event2");

    for (auto i = 0u; i < events0.size(); ++i)
    {
      events0[i] = event::make(record{i, std::to_string(i)}, type0);
      events0[i].id(i);
    }

    for (auto i = 0u; i < events1.size(); ++i)
    {
      events1[i] = event::make(record{4.2 + i, i % 2 == 0}, type1);
      events1[i].id(events0.size() + i);
    }

    for (auto i = 0u; i < events.size(); ++i)
    {
      if (i % 2 == 0)
        events[i] = event::make(record{i, std::to_string(i)}, type0);
      else
        events[i] = event::make(record{4.2 + i, true}, type1);
      events[i].id(events0.size() + events1.size() + i);
    }
  }

  type type0;
  type type1;
  std::vector<event> events0;
  std::vector<event> events1;
  std::vector<event> events;
};

} // namespace fixtures

#endif
