//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/query_queue.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/system/catalog.hpp"
#include "vast/test/test.hpp"
#include "vast/uuid.hpp"

#include <caf/test/dsl.hpp>

using namespace vast::test;

namespace vast {

namespace {

std::vector<uuid> xs
  = {unbox(to<uuid>("00000000-0000-0000-0000-000000000000")),
     unbox(to<uuid>("11111111-1111-1111-1111-111111111111")),
     unbox(to<uuid>("22222222-2222-2222-2222-222222222222")),
     unbox(to<uuid>("33333333-3333-3333-3333-333333333333")),
     unbox(to<uuid>("44444444-4444-4444-4444-444444444444")),
     unbox(to<uuid>("55555555-5555-5555-5555-555555555555")),
     unbox(to<uuid>("66666666-6666-6666-6666-666666666666")),
     unbox(to<uuid>("77777777-7777-7777-7777-777777777777")),
     unbox(to<uuid>("88888888-8888-8888-8888-888888888888")),
     unbox(to<uuid>("99999999-9999-9999-9999-999999999999")),
     unbox(to<uuid>("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
     unbox(to<uuid>("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")),
     unbox(to<uuid>("cccccccc-cccc-cccc-cccc-cccccccccccc")),
     unbox(to<uuid>("dddddddd-dddd-dddd-dddd-dddddddddddd")),
     unbox(to<uuid>("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")),
     unbox(to<uuid>("ffffffff-ffff-ffff-ffff-ffffffffffff"))};

system::receiver_actor<atom::done> dummy_client = {};

system::catalog_lookup_result cands(uint32_t start, uint32_t end) {
  if (end > xs.size() || start > end)
    FAIL("can't generate more than 16 candidates");
  system::catalog_lookup_result result{};
  for (auto i = start; i < end; ++i) {
    auto id = xs[i];
    result.candidate_infos[vast::type{}].partition_infos.emplace_back(
      id, 0u, time{}, vast::type{}, version::current_partition_version);
  }
  return result;
}

system::catalog_lookup_result cands(uint32_t num) {
  return cands(0, num);
}

// We need to be able to generate queries with random query ids.
query_context make_random_query_context() {
  auto result = query_context::make_count(
    "test", caf::actor{}, count_query_context::estimate, expression{});
  result.id = uuid::random();
  return result;
}

uuid make_insert(query_queue& q, system::catalog_lookup_result&& candidates) {
  uint32_t cands_size = candidates.size();
  auto query_context = make_random_query_context();
  REQUIRE_SUCCESS(q.insert(query_state{.query_contexts_per_type
                                       = {{vast::type{}, query_context}},
                                       .client = dummy_client,
                                       .candidate_partitions = cands_size,
                                       .requested_partitions = cands_size},
                           std::move(candidates)));
  return query_context.id;
}

uuid make_insert(query_queue& q, system::catalog_lookup_result&& candidates,
                 uint32_t taste_size,
                 uint64_t priority = query_context::priority::normal) {
  uint32_t cands_size = candidates.size();
  auto query_context = make_random_query_context();
  query_context.priority = priority;
  REQUIRE_SUCCESS(q.insert(query_state{.query_contexts_per_type
                                       = {{vast::type{}, query_context}},
                                       .client = dummy_client,
                                       .candidate_partitions = cands_size,
                                       .requested_partitions = taste_size},
                           std::move(candidates)));
  return query_context.id;
}

} // namespace

TEST(insert violating precondidtions) {
  query_queue q;
  REQUIRE(q.queries().empty());
  REQUIRE(q.insert(query_state{}, cands(0)));
  REQUIRE(q.insert(query_state{}, cands(5)));
  CHECK(q.queries().empty());
}

TEST(mark as erased) {
  query_queue q;
  auto candidates = cands(1);
  make_insert(q, decltype(candidates){candidates}, candidates.size());
  CHECK_EQUAL(q.queries().size(), 1u);
  q.mark_partition_erased(
    candidates.candidate_infos[vast::type{}].partition_infos.front().uuid);
  const auto out = unbox(q.next());
  CHECK(out.erased);
}

TEST(single query) {
  query_queue q;
  make_insert(q, cands(3), 3);
  CHECK_EQUAL(q.queries().size(), 1u);
  auto a = unbox(q.next());
  auto b = unbox(q.next());
  auto c = unbox(q.next());
  CHECK_ERROR(q.next());
  CHECK_EQUAL(q.queries().size(), 1u);
  CHECK_EQUAL(q.handle_completion(c.queries.at(0)), std::nullopt);
  CHECK_EQUAL(q.handle_completion(b.queries.at(0)), std::nullopt);
  CHECK_EQUAL(q.queries().size(), 1u);
  CHECK_EQUAL(q.handle_completion(a.queries.at(0)), dummy_client);
  CHECK(q.queries().empty());
}

TEST(2 overlapping queries) {
  query_queue q;
  auto qid1 = make_insert(q, cands(3));
  auto qid2 = make_insert(q, cands(1, 4), 3, query_context::priority::low);
  CHECK_EQUAL(q.queries().size(), 2u);
  auto a = unbox(q.next());
  CHECK_EQUAL(q.handle_completion(a.queries.at(0)), std::nullopt);
  CHECK_EQUAL(q.handle_completion(a.queries.at(1)), std::nullopt);
  auto b = unbox(q.next());
  CHECK_EQUAL(b.queries.at(0), qid1);
  CHECK_EQUAL(b.queries.at(1), qid2);
  auto c = unbox(q.next());
  CHECK_EQUAL(c.queries.at(0), qid1);
  CHECK_EQUAL(q.handle_completion(c.queries.at(0)), std::nullopt);
  auto d = unbox(q.next());
  CHECK_EQUAL(q.queries().size(), 2u);
  CHECK_EQUAL(q.handle_completion(b.queries.at(1)), std::nullopt);
  CHECK_EQUAL(q.handle_completion(b.queries.at(0)), dummy_client);
  CHECK_ERROR(q.next());
  CHECK_EQUAL(q.queries().size(), 1u);
  CHECK_EQUAL(q.handle_completion(d.queries.at(0)), dummy_client);
  CHECK(q.queries().empty());
}

} // namespace vast
