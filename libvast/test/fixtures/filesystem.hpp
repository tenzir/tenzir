#ifndef FIXTURES_FILESYSTEM_HPP
#define FIXTURES_FILESYSTEM_HPP

#include "vast/filesystem.hpp"

namespace fixtures {

struct filesystem {
  filesystem() {
    // Fresh afresh.
    rm(directory);
    mkdir(directory);
  }

  vast::path directory = "vast-unit-test";
};

} // namespace fixtures

#endif

