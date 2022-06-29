//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/partition_synopsis.hpp"

namespace vast {

struct partition_synopsis::unit_test_access {
  unit_test_access(partition_synopsis& ps)
    : use_sketches_(ps.use_sketches_),
      offset_(ps.offset_),
      events_(ps.events_),
      min_import_time_(ps.min_import_time_),
      max_import_time_(ps.max_import_time_) {
  }

  bool& use_sketches_;
  uint64_t& offset_;
  uint64_t& events_;
  time& min_import_time_;
  time& max_import_time_;
};

} // namespace vast
