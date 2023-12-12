//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/catalog_lookup_result.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/taxonomies.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

struct catalog_lookup_state {
  static constexpr auto name = "catalog-lookup";

  catalog_lookup_actor::pointer self = {};
  query_context query = {};
  std::deque<partition_synopsis_pair> remaining_partitions = {};
  uint64_t cache_capacity = {};
  detail::heterogeneous_string_hashset unprunable_fields = {};
  struct taxonomies taxonomies = {};
  std::unordered_map<type, expression> bound_exprs = {};
  std::vector<catalog_lookup_result> results = {};
  caf::typed_response_promise<std::vector<catalog_lookup_result>> get_rp = {};

  // The internal run loop. Calls itself with yields to the scheduler inbetween
  // partitions until no more parttions are remaining. Must be called at most
  // once.
  auto run() -> void;
};

auto make_catalog_lookup(
  catalog_lookup_actor::stateful_pointer<catalog_lookup_state> self,
  std::deque<partition_synopsis_pair> partitions,
  detail::heterogeneous_string_hashset unprunable_fields,
  struct taxonomies taxonomies, query_context query, uint64_t cache_capacity)
  -> catalog_lookup_actor::behavior_type;

} // namespace tenzir
