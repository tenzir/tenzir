//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/query_context.hpp"
#include "vast/system/actors.hpp"
#include "vast/uuid.hpp"

#include <caf/behavior.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <array>
#include <cstddef>
#include <cstdint>

namespace vast::system {

/// A query processor takes a query and collects hits from the INDEX.
/// Implementation hooks allow subtypes to configure how many hits are
/// requested and how hits are processed. The query processor implements the
/// following state machine:
///
/// ```
///                    +----------------+
///                    |                |
///               +--->+      idle      |
///               |    |                |
///               |    +-------+--------+
///               |            |
///               |            | (run)
///               |            v
///               |    +-------+--------+
///               |    |                |
///               |    | await query id |
///               |    |                |
///               |    +-------+--------+
///               |            |
///               |            | (query_id, scheduled, total)
///               |            |
///               |            |      +------+
///               |            |      |      |
///               |            v      v      | (ids)
///               |    +-------+------+-+    |
///               |    |                +----+
///               |    |  collect hits  |
///               |    |                +<---+
///               |    +-------+--------+    |
///               |            |             |
///               |            | (done)      |
///               |            v             |
///               |       XXXXXXXXXXXX       |
///               |      XX request  XX      |
///               +----+XX    more    XX+----+
///                no    XX   hits?  XX   yes
///                       XXXXXXXXXXXX
/// ```
class query_processor {
public:
  // -- member types -----------------------------------------------------------

  enum state_name {
    idle,
    await_query_id,
    await_results_until_done,
  };

  static constexpr size_t num_states = 3;

  // -- constants --------------------------------------------------------------

  /// Human-readable actor name for logging output.
  static constexpr const char* name = "query-processor";

  // -- constructors, destructors, and assignment operators --------------------

  explicit query_processor(caf::event_based_actor* self);

  virtual ~query_processor() noexcept = default;

  // -- convenience functions --------------------------------------------------

  /// Sends the query `expr` to `index` and transitions from `idle` to
  /// `await_query_id`.
  /// @pre `state() == idle`
  void start(vast::query_context query_context, index_actor index);

  /// @pre `state() == collect_hits`
  /// @returns false if there are no more partitions to schedule.
  bool request_more_results();

  // -- properties -------------------------------------------------------------

  /// @returns the current state.
  state_name state() {
    return state_;
  }

  /// @returns the current behavior.
  caf::behavior& behavior() {
    return behaviors_[state_];
  }

  /// @returns the behavior for state `x`.
  caf::behavior& behavior(state_name x) {
    return behaviors_[x];
  }

protected:
  // -- state management -------------------------------------------------------

  virtual void transition_to(state_name x);

  // -- implementation hooks ---------------------------------------------------

  /// Processes incoming done messages from the INDEX. The default
  /// implementation tansitions to the idle state.
  virtual void process_done();

  /// Implements the status handler.
  virtual record status(status_verbosity);

  // -- member variables -------------------------------------------------------

  /// Stores the name of the current state.
  state_name state_;

  /// Stores a behavior for each named state.
  std::array<caf::behavior, num_states> behaviors_;

  /// Points to the actor that runs this FSM.
  caf::event_based_actor* self_;

  /// Our query ID for collecting more hits.
  uuid query_id_;

  /// Our INDEX for querying and collecting more hits.
  index_actor index_;

  /// Keeps track of how many partitions were processed.
  struct {
    uint32_t received;
    uint32_t scheduled;
    uint32_t total;
  } partitions_;
};

// -- related functions --------------------------------------------------------

std::string to_string(query_processor::state_name x);

} // namespace vast::system

namespace fmt {

template <>
struct formatter<enum vast::system::query_processor::state_name> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(enum vast::system::query_processor::state_name value,
              FormatContext& ctx) const {
    switch (value) {
      case vast::system::query_processor::state_name::idle:
        return format_to(ctx.out(), "idle");
      case vast::system::query_processor::state_name::await_query_id:
        return format_to(ctx.out(), "await_query_id");
      case vast::system::query_processor::state_name::await_results_until_done:
        return format_to(ctx.out(), "await_results_until_done");
    }
    vast::die("unreachable");
  }
};

} // namespace fmt
