#include "framework/unit.h"
#include "vast/chunk.h"
#include "vast/event.h"

using namespace vast;

SUITE("core")

TEST("chunk")
{
  chunk chk;
  auto t = type::integer{};
  t.name("i");

  chunk::writer w{chk};
  std::vector<event> es;
  for (auto i = 0; i < 1e3; ++i)
  {
    es.emplace_back(i, t);
    REQUIRE(w.write(es.back()));
  }

  w.flush();
  CHECK(chk.events() == 1e3);

  chunk::reader r{chk};
  for (auto i = 0; i < 1e3; ++i)
  {
    auto e = r.read();
    REQUIRE(e);
    CHECK(*e == event{i, t});
  }

  chunk copy{chk};
  CHECK(chk == copy);

  chunk from_events{es};
  CHECK(from_events == chk);

  // Assign IDs to the chunk.
  ewah_bitstream ids;
  ids.append(42, false);
  ids.append(1e3 - 1, true);

  CHECK(! chk.ids(ids));  // 1 event ID missing.

  ids.push_back(true);
  CHECK(chk.ids(std::move(ids)));
}
