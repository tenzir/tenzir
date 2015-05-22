#ifndef FIXTURES_CHUNKS_H
#define FIXTURES_CHUNKS_H

#include <vector>

#include "vast/event.h"
#include "vast/chunk.h"

using namespace vast;

namespace fixtures {

struct chunks
{
  chunks()
  {
    type0 = type::record{{"c", type::count{}}, {"s", type::string{}}};
    type0.name("test_record_event");
    chunk::writer w0{chunk0};
    for (auto i = 0u; i < 1024u; ++i)
    {
      auto e = event::make(record{i, to_string(i)}, type0);
      e.id(i);
      e.timestamp(time::now());
      REQUIRE(w0.write(e));
    }
    w0.flush();
    REQUIRE(chunk0.events() == 1024u);

    type1 = type::real{};
    type1.name("test_double_event");
    chunk::writer w1{chunk1};
    for (auto i = 0u; i < 500u; ++i)
    {
      auto e = event::make(4.2 + i, type1);
      e.id(i + chunk0.events());
      e.timestamp(time::now());
      REQUIRE(w1.write(e));
    }
    w1.flush();
  }

  chunk chunk0;
  type type0;
  chunk chunk1;
  type type1;
};

} // namespace fixtures

#endif
