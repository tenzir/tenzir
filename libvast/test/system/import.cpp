#include "vast/bitmap_index.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"
#include "vast/actor/archive.hpp"
#include "vast/actor/node.hpp"
#include "vast/concept/serializable/io.hpp"
#include "vast/concept/serializable/vast/bitmap_index.hpp"
#include "vast/concept/serializable/vast/chunk.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/concept/printable/vast/error.hpp"

#define SUITE actors
#include "test.hpp"
#include "data.hpp"
#include "fixtures/core.hpp"

using namespace vast;

FIXTURE_SCOPE(core_scope, fixtures::core)

TEST(import) {
  MESSAGE("inhaling a Bro FTP log");
  auto n = make_core();
  run_source(n, "bro", "-r", m57_day11_18::ftp);
  stop_core(n);
  self->await_all_other_actors_done();

  MESSAGE("checking that indexes have been written correctly");
  path id_range;
  for (auto& p0 : directory{dir / "index"})
    if (p0.is_directory())
      for (auto& p1 : directory{p0})
        if (p1.is_directory()) {
          id_range = p1;
          break;
        }
  REQUIRE(! id_range.empty());
  auto ftp = id_range / "bro::ftp" / "data";
  REQUIRE(exists(dir));
  REQUIRE(exists(ftp));
  uint64_t last_flush;
  address_bitmap_index<default_bitstream> abmi;
  port_bitmap_index<default_bitstream> pbmi;
  REQUIRE(load(ftp / "id" / "orig_h", last_flush, abmi));
  REQUIRE(load(ftp / "id" / "orig_p", last_flush, pbmi));
  REQUIRE(abmi.size() == 2);
  REQUIRE(pbmi.size() == 2);

  MESSAGE("performing manual bitmap index lookup");
  auto eq = relational_operator::equal;
  auto orig_h = abmi.lookup(eq, *to<address>("192.168.1.105"));
  auto orig_p = pbmi.lookup(greater, *to<port>("49320/?"));
  REQUIRE(orig_h);
  CHECK((*orig_h)[0] == 1);
  CHECK((*orig_h)[1] == 1);
  REQUIRE(orig_p);
  CHECK((*orig_p)[0] == 1);
  CHECK((*orig_p)[1] == 0);

  MESSAGE("checking that ARCHIVE has successfully stored the segment");
  path segment_file;
  for (auto& p : directory{dir / "archive"})
    if (p.basename() != "meta.data") {
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
}

FIXTURE_SCOPE_END()
