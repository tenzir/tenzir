/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/chunk.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/si.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/directory.hpp"
#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/io/read.hpp"
#include "vast/meta_index.hpp"
#include "vast/path.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/system/partition.hpp"
#include "vast/table_slice.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/binary_deserializer.hpp>

#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

using namespace vast;

static caf::settings get_status_proc() {
  using namespace parser_literals;
  auto is = std::ifstream{"/proc/self/status"};
  auto lines = detail::line_range{is};
  caf::settings result;
  auto skip = ignore(+parsers::any);
  auto ws = ignore(+parsers::space);
  auto kvp = [&](const char* k, const std::string_view human_friendly, auto v) {
    using T = decltype(v);
    return (k >> ws >> si_parser<T>{} >> skip)->*[=, &result](T x) {
      result[human_friendly] = x;
    };
  };
  auto rss = kvp("VmRSS:", "current-memory-usage", size_t{});
  auto size = kvp("VmHWM:", "peak-memory-usage", size_t{});
  auto swap = kvp("VmSwap:", "swap-space-usage", size_t{});
  auto p = rss | size | swap | skip;
  while (true) {
    lines.next();
    if (lines.done())
      break;
    auto line = lines.get();
    if (!p(line))
      std::cerr << "failed to parse /proc/self/status:" << line << std::endl;
  }
  return result;
}

int main(int argc, char** argv) {
  factory<synopsis>::initialize(); // argh :(

  if (argc < 3) {
    std::cerr << "Usage: " << argv[0]
              << " <path/to/vast.db> <output_filename>\n";
    return 1;
  }
  auto dbdir = vast::path{argv[1]};
  if (!exists(dbdir)) {
    std::cerr << "directory not found\n";
    return 1;
  }
  auto dir = dbdir / "index";
  auto fname = dir / "index.bin";
  if (!exists(fname)) {
    std::cerr << "file not found: " << fname.str() << std::endl;
    return 1;
  }
  auto out_filename = vast::path{argv[2]};
  std::ofstream out(argv[2], std::ios::binary);
  std::cout << "loading state from" << fname.str() << std::endl;
  auto buffer = io::read(fname);
  if (!buffer) {
    std::cerr << "failed to read index file:" << render(buffer.error())
              << std::endl;
    return 1;
  }
  // TODO: Create a `index_ondisk_state` struct and move this part of the
  // code into an `unpack()` function.
  auto index = fbs::GetIndex(buffer->data());
  if (index->index_type() != fbs::index::Index::v0) {
    std::cerr << "invalid index version\n";
    return 1;
  }
  // meta_index meta_idx;
  auto index_v0 = index->index_as_v0();
  auto partition_uuids = index_v0->partitions();
  VAST_ASSERT(partition_uuids);
  size_t n = 0;
  for (auto uuid_fb : *partition_uuids) {
    if (++n > 100)
      break;
    VAST_ASSERT(uuid_fb);
    vast::uuid partition_uuid;
    unpack(*uuid_fb, partition_uuid);
    auto partition_path = dir / to_string(partition_uuid);
    if (exists(partition_path)) {
      auto chunk = chunk::mmap(partition_path);
      if (!chunk) {
        std::cerr << "could not mmap partition at" << partition_path.str()
                  << std::endl;
        continue;
      }
      auto partition = fbs::GetPartition(chunk->data());
      if (partition->partition_type() != fbs::partition::Partition::v0) {
        std::cerr << "found unsupported version for partition"
                  << to_string(partition_uuid) << std::endl;
        continue;
      }
      auto partition_v0 = partition->partition_as_v0();
      VAST_ASSERT(partition_v0);
      auto synopses = partition_v0->partition_synopsis()->synopses();
      for (auto s : *synopses) {
        vast::synopsis_ptr ptr;
        if (auto error = unpack(*s, ptr)) {
          std::cerr << "error deserializing synopsis: " << render(error)
                    << std::endl;
          continue;
        }
        if (!ptr)
          continue;
        std::vector<char> buf;
        caf::binary_serializer source(nullptr, buf);
        if (auto error = source(ptr)) {
          std::cerr << "error deserializing synopsis\n";
          continue;
        }
        std::cout << "partition " << to_string(partition_uuid)
                  << to_string(ptr->type())
                  << " synopsis "
                     "size "
                  << ptr->size_bytes() << " offset " << out.tellp()
                  << std::endl;
        out.write(&buf[0], buf.size());
        out.flush();
      }
      // partition_synopsis ps;
      // system::unpack(*partition_v0, ps);
      // std::cout << "merging partition synopsis from"
      //           << to_string(partition_uuid) << std::endl;
      // meta_idx.merge(partition_uuid, std::move(ps));
    } else {
      std::cerr << "found partition" << to_string(partition_uuid)
                << "in the index state but not on disk; this may have been "
                   "caused by an unclean shutdown\n";
    }
  }

  auto memory_stats = get_status_proc();
  std::cout << "Memory stats:\n" << to_string(memory_stats) << std::endl;
}
