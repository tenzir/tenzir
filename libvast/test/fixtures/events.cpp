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

namespace {

timestamp epoch;

std::vector<event> make_ascending_integers(size_t count) {
  std::vector<event> result;
  type layout = type{record_type{{"value", integer_type{}}}}.name("test::int");
  for (size_t i = 0; i < count; ++i) {
    result.emplace_back(event::make(vector{static_cast<integer>(i)}, layout));
    result.back().timestamp(epoch + std::chrono::seconds(i));
  }
  return result;
}

std::vector<event> make_alternating_integers(size_t count) {
  std::vector<event> result;
  type layout = type{record_type{{"value", integer_type{}}}}.name("test::int");
  for (size_t i = 0; i < count; ++i) {
    result.emplace_back(event::make(vector{static_cast<integer>(i % 2)},
                                    layout));
    result.back().timestamp(epoch + std::chrono::seconds(i));
  }
  return result;
}

} // namespace <anonymous>

size_t events::slice_size = 100;

std::vector<event> events::bro_conn_log;
std::vector<event> events::bro_dns_log;
std::vector<event> events::bro_http_log;
std::vector<event> events::bgpdump_txt;
std::vector<event> events::random;

std::vector<table_slice_ptr> events::bro_conn_log_slices;
// std::vector<table_slice_ptr> events::bro_dns_log_slices;
// std::vector<table_slice_ptr> events::bro_http_log_slices;
// std::vector<table_slice_ptr> events::bgpdump_txt_slices;
// std::vector<table_slice_ptr> events::random_slices;

std::vector<const_table_slice_ptr> events::const_bro_conn_log_slices;
// std::vector<const_table_slice_ptr> events::const_bro_http_log_slices;
// std::vector<const_table_slice_ptr> events::const_bro_dns_log_slices;
// std::vector<const_table_slice_ptr> events::const_bgpdump_txt_slices;
// std::vector<const_table_slice_ptr> events::const_random_slices;

std::vector<event> events::ascending_integers;
std::vector<table_slice_ptr> events::ascending_integers_slices;
std::vector<const_table_slice_ptr> events::const_ascending_integers_slices;

std::vector<event> events::alternating_integers;
std::vector<table_slice_ptr> events::alternating_integers_slices;
std::vector<const_table_slice_ptr> events::const_alternating_integers_slices;

record_type events::bro_conn_log_layout() {
  return const_bro_conn_log_slices[0]->layout();
}

std::vector<table_slice_ptr> events::copy(std::vector<table_slice_ptr> xs) {
  std::vector<table_slice_ptr> result;
  result.reserve(xs.size());
  for (auto& x : xs)
    result.emplace_back(x->clone());
  return result;
}

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
  ascending_integers = make_ascending_integers(10000);
  alternating_integers = make_alternating_integers(10000);
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
  assign(ascending_integers);
  assign(alternating_integers);
  MESSAGE("building slices of " << slice_size << " events each");
  auto slice_up = [&](const std::vector<event>& src) {
    VAST_ASSERT(src.size() > 0);
    VAST_ASSERT(caf::holds_alternative<record_type>(src[0].type()));
    auto layout = caf::get<record_type>(src[0].type());
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
        auto add_res = builder->add(e.timestamp());
        if (!add_res)
          FAIL("builder->add() failed");
        auto rec_add_res = builder->recursive_add(e.data());
        if (!rec_add_res)
          FAIL("builder->recursive_add() failed");
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
  //bro_dns_log_slices = slice_up(bro_dns_log);
  //bro_http_log_slices = slice_up(bro_http_log);
  //bgpdump_txt_slices = slice_up(bgpdump_txt);
  //random_slices = slice_up(random);
  ascending_integers_slices = slice_up(ascending_integers);
  alternating_integers_slices = slice_up(alternating_integers);
  auto to_const_vector = [](const auto& xs) {
    std::vector<const_table_slice_ptr> result;
    result.reserve(xs.size());
    result.insert(result.end(), xs.begin(), xs.end());
    return result;
  };
  const_bro_conn_log_slices = to_const_vector(bro_conn_log_slices);
  // const_bro_dns_log_slices = to_const_vector(bro_dns_log_slices);
  // const_bro_http_log_slices = to_const_vector(bro_http_log_slices);
  // const_bgpdump_txt_slices = to_const_vector(bgpdump_txt_slices);
  // const_random_slices = to_const_vector(random_slices);
  const_ascending_integers_slices = to_const_vector(ascending_integers_slices);
  const_alternating_integers_slices
    = to_const_vector(alternating_integers_slices);
  auto to_events = [](const auto& slices) {
    std::vector<event> result;
    for (auto& slice : slices) {
      auto xs = slice->rows_to_events();
      std::move(xs.begin(), xs.end(), std::back_inserter(result));
    }
    return result;
  };
#define SANITY_CHECK(event_vec, slice_vec)                                     \
  {                                                                            \
    auto flat_log = to_events(slice_vec);                                      \
    REQUIRE_EQUAL(event_vec.size(), flat_log.size());                          \
    for (size_t i = 0; i < event_vec.size(); ++i) {                            \
      if (flatten(event_vec[i]) != flat_log[i]) {                              \
        FAIL(#event_vec << " != " << #slice_vec);                              \
      }                                                                        \
    }                                                                          \
  }
  SANITY_CHECK(bro_conn_log, const_bro_conn_log_slices);
  //SANITY_CHECK(bro_dns_log, const_bro_dns_log_slices);
  //SANITY_CHECK(bro_http_log, const_bro_http_log_slices);
  //SANITY_CHECK(bgpdump_txt, const_bgpdump_txt_slices);
  //SANITY_CHECK(random, const_random_slices);
}

} // namespace fixtures
