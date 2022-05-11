//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/buffered_builder.hpp"

#include "vast/detail/hash_scalar.hpp"

#define SUITE buffered_builder
#include "vast/concept/convertible/data.hpp"
#include "vast/data.hpp"
#include "vast/sketch/sketch.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

struct nop_builder : sketch::buffered_builder {
  caf::expected<sketch::sketch>
  build(const std::unordered_set<uint64_t>&) override {
    return caf::no_error;
  }
};

} // namespace

FIXTURE_SCOPE(partition_sketch_tests, fixtures::events)

TEST() {
  nop_builder builder;
  auto slice = zeek_conn_log[0];
  auto record_batch = to_record_batch(slice);
  auto uids = record_batch->column(1);
  REQUIRE(uids != nullptr);
  CHECK_EQUAL(builder.add(uids), caf::none);
  // Build baseline by hashing strings manually.
  std::unordered_set<uint64_t> manual_digests;
  for (auto i = 0u; i < slice.rows(); ++i)
    manual_digests.insert(detail::hash_scalar<string_type>(slice.at(i, 1)));
  CHECK_EQUAL(builder.digests(), manual_digests);
}

FIXTURE_SCOPE_END()
