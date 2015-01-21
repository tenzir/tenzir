#include <caf/all.hpp>

#include "vast/bitmap_index.h"
#include "vast/configuration.h"
#include "vast/filesystem.h"
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

  CHECK(rm(dir));
}
