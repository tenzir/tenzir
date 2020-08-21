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

#pragma once

#include "vast/test/data.hpp"
#include "vast/test/test.hpp"

#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/fwd.hpp"

namespace fixtures {

using namespace vast;

struct events {
  events();

  /// Maximum size of all generated slices.
  static constexpr size_t slice_size = 8;

  // TODO: remove these entirely; all operations should be on table slices.
  static std::vector<table_slice_ptr> zeek_conn_log;
  static std::vector<table_slice_ptr> zeek_dns_log;
  static std::vector<table_slice_ptr> zeek_http_log;
  static std::vector<table_slice_ptr> random;

  static std::vector<table_slice_ptr> zeek_conn_log_full;

  /// 10000 ascending integer values, starting at 0.
  static std::vector<table_slice_ptr> ascending_integers;

  /// 10000 integer values, alternating between 0 and 1.
  static std::vector<table_slice_ptr> alternating_integers;

  template <class... Ts>
  static std::vector<std::vector<data>> make_rows(Ts... xs) {
    return {std::vector<data>{data{std::move(xs)}}...};
  }

  auto take(const std::vector<table_slice_ptr>& xs, size_t n) {
    VAST_ASSERT(n <= xs.size());
    auto first = xs.begin();
    auto last = first + n;
    return std::vector<table_slice_ptr>{first, last};
  }
};

} // namespace fixtures
