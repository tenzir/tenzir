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
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/io/read.hpp"
#include "vast/path.hpp"
#include "vast/uuid.hpp"

#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <string>

using std::cerr;
using std::cout;
using std::endl;

using namespace std::string_literals;

namespace fs = std::filesystem;

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: ./lsvast <path/to/vast.db>\n";
    return 1;
  }
  auto vast_db = fs::path{argv[1]};
  auto index_dir = vast_db / "index";
  std::cout << "Index\n";
  for (auto file : fs::directory_iterator{index_dir}) {
    auto path = file.path();
    auto stem = path.stem().string();
    if (stem == "index")
      continue;
    auto bytes
      = vast::io::read(vast::path{(fs::current_path() / file).string()});
    if (!bytes) {
      std::cerr << to_string(bytes.error()) << std::endl;
      return 0;
    }
    const vast::fbs::Partition* partition
      = vast::fbs::GetPartition(bytes->data());
    if (!partition) {
      std::cerr << "  ! couldnt parse as partition: " << path << "\n";
      continue;
    }
    vast::uuid id;
    if (partition->uuid())
      unpack(*partition->uuid(), id);

    std::cout << "  " << path.string() << "\n";
    std::cout << "    id: " << to_string(id) << "\n";
    std::cout << "    offset: " << partition->offset() << "\n";
    std::cout << "    events: " << partition->events() << "\n";
    std::cout << "\n";
  }
  return 0;
}
