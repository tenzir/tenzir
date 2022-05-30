//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE partition_synopsis

#include "vast/partition_synopsis.hpp"

#include "vast/bloom_filter_synopsis.hpp"
#include "vast/defaults.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

namespace {

struct fixture : fixtures::events {};

} // namespace

FIXTURE_SCOPE(partition_tests, fixture)

TEST(custom index_config) {
  // Setup.
  using namespace std::string_literals;
  auto ps = vast::partition_synopsis{};
  auto capacity = vast::defaults::system::max_partition_size;
  auto synopsis_opts = vast::index_config{
    .rules = {
      {
        .targets = {"zeek.http.uri"s},
        .fp_rate = 0.001,
      },
      {
        .targets = {":addr"s},
        .fp_rate = 0.05,
      },
    },
  };
  // Ingest.
  for (const auto& slice : zeek_http_log)
    ps.add(slice, capacity, synopsis_opts);
  ps.shrink();
  // Verify field synopses.
  auto&& layout = zeek_http_log.at(0).layout();
  auto const& layout_rt = caf::get<vast::record_type>(layout);
  auto uri_key = layout_rt.resolve_key("uri");
  auto host_key = layout_rt.resolve_key("host");
  REQUIRE(uri_key);
  REQUIRE(host_key);
  auto uri_field = vast::qualified_record_field(layout, *uri_key);
  auto host_field = vast::qualified_record_field(layout, *host_key);
  auto const& host_synopsis = ps.field_synopses_.at(host_field);
  CHECK_EQUAL(host_synopsis, nullptr);
  auto& url_synopsis = ps.field_synopses_.at(uri_field);
  REQUIRE_NOT_EQUAL(url_synopsis, nullptr);
  auto const& type = url_synopsis->type();
  auto attributes = vast::detail::collect(type.attributes());
  auto url_parameters = vast::parse_parameters(url_synopsis->type());
  REQUIRE(url_parameters.has_value());
  CHECK_EQUAL(url_parameters->p, 0.001);
  // Verify type synopses.
  auto& string_synopsis = ps.type_synopses_.at(vast::type{vast::string_type{}});
  auto& address_synopsis
    = ps.type_synopses_.at(vast::type{vast::address_type{}});
  auto string_parameters = vast::parse_parameters(string_synopsis->type());
  auto address_parameters = vast::parse_parameters(address_synopsis->type());
  REQUIRE(string_parameters);
  REQUIRE(url_parameters);
  CHECK_EQUAL(string_parameters->p,
              vast::defaults::system::string_synopsis_fp_rate);
  CHECK_EQUAL(address_parameters->p, 0.05);
}

FIXTURE_SCOPE_END()
