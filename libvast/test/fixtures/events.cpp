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

#include "fixtures/events.hpp"

#include "vast/default_table_slice.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/test.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

namespace fixtures {

size_t events::slice_size = 100;

std::vector<event> events::bro_conn_log;
std::vector<event> events::bro_dns_log;
std::vector<event> events::bro_http_log;
std::vector<event> events::bgpdump_txt;
std::vector<event> events::random;

std::vector<table_slice_ptr> events::bro_conn_log_slices;
std::vector<table_slice_ptr> events::bro_dns_log_slices;
std::vector<table_slice_ptr> events::bro_http_log_slices;
std::vector<table_slice_ptr> events::bgpdump_txt_slices;
std::vector<table_slice_ptr> events::random_slices;

events::events() {
  static bool initialized = false;
  if (initialized)
    return;
  initialized = true;
  MESSAGE("inhaling unit test suite events");
  bro_conn_log = inhale<format::bro::reader>(bro::conn);
  bro_dns_log = inhale<format::bro::reader>(bro::dns);
  bro_http_log = inhale<format::bro::reader>(bro::http);
  bgpdump_txt = inhale<format::bgpdump::reader>(bgpdump::updates20140821);
  random = extract(vast::format::test::reader{42, 1000});
  // Assign monotonic IDs to events starting at 0.
  auto i = id{0};
  auto assign = [&](auto& xs) {
    for (auto& x : xs)
      x.id(i++);
  };
  assign(bro_conn_log);
  assign(bro_dns_log);
  i += 1000; // Cause an artificial gap in the ID sequence.
  assign(bro_http_log);
  assign(bgpdump_txt);
  MESSAGE("building slices of " << slice_size << " events each");
  auto slice_up = [&](const std::vector<event>& src) {
    VAST_ASSERT(src.size() > 0);
    auto layout = caf::get<record_type>(src.front().type());
    record_field tstamp_field{"timestamp", timestamp_type{}};
    layout.fields.insert(layout.fields.begin(), std::move(tstamp_field));
    auto builder = default_table_slice::make_builder(std::move(layout));
    auto full_slices = src.size() / slice_size;
    std::vector<table_slice_ptr> slices;
    auto i = src.begin();
    auto make_slice = [&](size_t size) {
      if (size == 0)
        return;
      auto first_id_in_slice = i->id();
      for (size_t j = 0; j < size; ++j) {
        auto& e = *i++;
        builder->add(e.timestamp());
        builder->recursive_add(e.data());
      }
      slices.emplace_back(builder->finish());
      slices.back()->offset(first_id_in_slice);
    };
    // Insert full slices.
    for (size_t i = 0; i < full_slices; ++i)
      make_slice(slice_size);
    // Insert remaining events.
    make_slice(src.size() % slice_size);
    return slices;
  };
  bro_conn_log_slices = slice_up(bro_conn_log);
  bro_dns_log_slices = slice_up(bro_dns_log);
  bro_http_log_slices = slice_up(bro_http_log);
  bgpdump_txt_slices = slice_up(bgpdump_txt);
  random_slices = slice_up(random);
}

} // namespace fixtures
