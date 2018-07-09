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

#include <caf/all.hpp>

#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/fwd.hpp"

#include "data.hpp"
#include "test.hpp"

namespace fixtures {

using namespace vast;

struct events {
  events();

  /// Maximum size of all generated slices.
  static size_t slice_size;

  using event_list = std::vector<event>;

  using slice_list = std::vector<table_slice_ptr>;

  using const_slice_list = std::vector<const_table_slice_ptr>;

  static event_list bro_conn_log;
  static event_list bro_dns_log;
  static event_list bro_http_log;
  static event_list bgpdump_txt;
  static event_list random;

  static slice_list bro_conn_log_slices;
  // TODO: table_slice::recursive_add flattens too much, why the following
  //       slices won't work. However, flatten(value) is also broken
  //       at the moment (cf. #3215), so we can't fix it until then.
  // static slice_list bro_http_log_slices;
  // static slice_list bro_dns_log_slices;
  // static slice_list bgpdump_txt_slices;
  // static slice_list random_slices;

  static const_slice_list const_bro_conn_log_slices;
  // static const_slice_list const_bro_http_log_slices;
  // static const_slice_list const_bro_dns_log_slices;
  // static const_slice_list const_bgpdump_txt_slices;
  // static const_slice_list const_random_slices;

  /// 10000 ascending integer values, starting at 0.
  static event_list ascending_integers;
  static slice_list ascending_integers_slices;
  static const_slice_list const_ascending_integers_slices;

  /// 10000 integer values, alternating between 0 and 1.
  static event_list alternating_integers;
  static slice_list alternating_integers_slices;
  static const_slice_list const_alternating_integers_slices;

  static record_type bro_conn_log_layout();

  template <class... Ts>
  static std::vector<vector> make_rows(Ts... xs) {
    return {make_vector(xs)...};
  }

  slice_list copy(slice_list xs);

private:
  template <class Reader>
  static event_list inhale(const char* filename) {
    auto input = std::make_unique<std::ifstream>(filename);
    Reader reader{std::move(input)};
    return extract(reader);
  }

  template <class Reader>
  static event_list extract(Reader&& reader) {
    auto e = expected<event>{no_error};
    event_list events;
    while (e || !e.error()) {
      e = reader.read();
      if (e)
        events.push_back(std::move(*e));
    }
    REQUIRE(!e);
    CHECK(e.error() == ec::end_of_input);
    REQUIRE(!events.empty());
    return events;
  }
};

} // namespace fixtures

