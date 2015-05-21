#include <caf/all.hpp>

#include "vast/bitmap_index.h"
#include "vast/event.h"
#include "vast/filesystem.h"
#include "vast/actor/archive.h"
#include "vast/actor/node.h"
#include "vast/concept/serializable/bitmap_index.h"
#include "vast/concept/serializable/chunk.h"
#include "vast/concept/serializable/io.h"

#include "framework/unit.h"
#include "test_data.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("import")
{
  VAST_INFO("inhaling a single Bro log");
  scoped_actor self;
  auto dir = path{"vast-test-import"};
  if (exists(dir))
    REQUIRE(rm(dir));
  auto n = self->spawn<node>("test-node", dir);
  // TODO: use a fixture which creates all core actors.
  std::vector<message> msgs = {
    make_message("spawn", "archive"),
    make_message("spawn", "index"),
    make_message("spawn", "importer"),
    make_message("spawn", "identifier"),
    make_message("spawn", "source", "bro", "-r", m57_day11_18::ftp),
    make_message("connect", "importer", "identifier"),
    make_message("connect", "importer", "archive"),
    make_message("connect", "importer", "index"),
    make_message("connect", "source", "importer"),
    make_message("send", "source", "run")
  };
  for (auto& msg : msgs)
    self->sync_send(n, msg).await([](ok_atom) {});
  // We first get the SOURCE, wait until it's done, then terminate IMPORTER.
  // Thereafter, we can guarantee that ARCHIVE and INDEX have received all
  // events.
  self->sync_send(n, get_atom::value, "source").await(
    [&](actor const& a, std::string const& fqn, std::string const& type)
    {
      CHECK(fqn == "source@test-node");
      CHECK(type == "source");
      REQUIRE(a != invalid_actor);
      self->monitor(a);
    }
  );
  self->receive([&](down_msg const& msg) { CHECK(msg.reason == exit::done); });
  self->sync_send(n, get_atom::value, "importer").await(
    [&](actor const& a, std::string const& fqn, std::string const& type)
    {
      CHECK(fqn == "importer@test-node");
      CHECK(type == "importer");
      REQUIRE(a != invalid_actor);
      self->monitor(a);
    }
  );
  self->send(n, "stop");
  self->await_all_other_actors_done();

  VAST_INFO("checking that indexes have been written correctly");
  path id_range;
  for (auto& p0 : directory{dir / "index"})
    if (p0.is_directory())
      for (auto& p1 : directory{p0})
        if (p1.is_directory())
        {
          id_range = p1;
          break;
        }
  REQUIRE(! id_range.empty());
  auto ftp = id_range / "ftp" / "data";
  REQUIRE(exists(dir));
  REQUIRE(exists(ftp));
  uint64_t last_flush;
  address_bitmap_index<default_bitstream> abmi;
  port_bitmap_index<default_bitstream> pbmi;
  REQUIRE(load(ftp / "id" / "orig_h", last_flush, abmi));
  REQUIRE(load(ftp / "id" / "orig_p", last_flush, pbmi));
  REQUIRE(abmi.size() == 2);
  REQUIRE(pbmi.size() == 2);

  VAST_INFO("performing manual bitmap index lookup");
  auto eq = relational_operator::equal;
  auto orig_h = abmi.lookup(eq, *to<address>("192.168.1.105"));
  auto orig_p = pbmi.lookup(greater, *to<port>("49320/?"));
  REQUIRE(orig_h);
  CHECK((*orig_h)[0] == 1);
  CHECK((*orig_h)[1] == 1);
  REQUIRE(orig_p);
  CHECK((*orig_p)[0] == 1);
  CHECK((*orig_p)[1] == 0);

  VAST_INFO("checking that ARCHIVE has successfully stored the segment");
  path segment_file;
  for (auto& p : directory{dir / "archive"})
    if (p.basename() != "meta.data")
    {
      segment_file = p;
      break;
    }
  REQUIRE(! segment_file.empty());
  archive::segment s;
  REQUIRE(load(segment_file, s));
  REQUIRE(s.size() == 1);
  REQUIRE(s.front().events() == 2);
  chunk::reader r{s.front()};
  auto e = r.read();
  REQUIRE(e);
  auto rec = get<record>(*e);
  REQUIRE(e);
  CHECK(rec->at(1) == "VFU8tqz6is3");

  VAST_INFO("removing temporary directory");
  CHECK(rm(dir));
}
