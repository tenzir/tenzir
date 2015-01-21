#include <caf/all.hpp>

#include "vast/bitmap_index.h"
#include "vast/configuration.h"
#include "vast/event.h"
#include "vast/filesystem.h"
#include "vast/actor/archive.h"
#include "vast/actor/program.h"
#include "vast/io/serialization.h"

#include "framework/unit.h"
#include "test_data.h"

using namespace caf;
using namespace vast;

SUITE("actors")

TEST("all-in-one import")
{
  configuration cfg;
  *cfg["tracker.port"] = 42001;
  *cfg['v'] = 0;
  *cfg['V'] = 5;
  *cfg['C'] = true;
  *cfg['I'] = "bro";
  *cfg['r'] = m57_day11_18::ftp;
  REQUIRE(cfg.verify());

  path dir = *cfg.get("directory");
  if (exists(dir))
    REQUIRE(rm(dir));

  anon_send(spawn<program>(cfg), atom("run"));
  await_all_actors_done();

  //
  // Check that indexes have been written successfully.
  //

  path part;
  for (auto& p : directory{dir / "index"})
    if (p.is_directory())
    {
      part = p;
      break;
    }

  REQUIRE(! part.empty());
  auto ftp = part / "types" / "ftp";

  REQUIRE(exists(dir));
  REQUIRE(exists(ftp));

  uint64_t size;
  address_bitmap_index<default_bitstream> abmi;
  port_bitmap_index<default_bitstream> pbmi;

  REQUIRE(vast::io::unarchive(ftp / "id" / "orig_h" / "index", size, abmi));
  REQUIRE(vast::io::unarchive(ftp / "id" / "orig_p" / "index", size, pbmi));

  REQUIRE(size == 2);
  REQUIRE(size == abmi.size());
  REQUIRE(size == pbmi.size());

  auto eq = relational_operator::equal;
  auto orig_h = abmi.lookup(eq, *to<address>("192.168.1.105"));
  auto orig_p = pbmi.lookup(greater, *to<port>("49320/?"));

  REQUIRE(orig_h);
  CHECK((*orig_h)[0] == 1);
  CHECK((*orig_h)[1] == 1);

  REQUIRE(orig_p);
  CHECK((*orig_p)[0] == 1);
  CHECK((*orig_p)[1] == 0);

  //
  // Check that the archive has successfully stored the segment.
  //

  path segment_file;
  for (auto& p : directory{dir / "archive"})
    if (p.basename() != "meta.data")
    {
      segment_file = p;
      break;
    }
  REQUIRE(! segment_file.empty());

  archive::segment s;
  REQUIRE(vast::io::unarchive(segment_file, s));
  REQUIRE(s.size() == 1);
  REQUIRE(s.front().events() == 2);

  chunk::reader r{s.front()};
  auto e = r.read();
  REQUIRE(e);
  auto rec = get<record>(*e);
  REQUIRE(e);
  CHECK(rec->at(1) == "VFU8tqz6is3");

  CHECK(rm(dir));
}
