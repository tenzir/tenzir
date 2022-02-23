//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/query_processor.hpp"

#include <unordered_map>

namespace vast::system {

class counter_state : public query_processor {
public:
  // -- member types -----------------------------------------------------------

  using super = query_processor;

  // -- constants --------------------------------------------------------------

  static inline constexpr const char* name = "counter";

  // -- constructors, destructors, and assignment operators --------------------

  counter_state(caf::event_based_actor* self);

  void init(expression expr, index_actor index, bool skip_candidate_check);

protected:
  // -- implementation hooks ---------------------------------------------------

  // Gets called for every scheduled partition.
  void process_done() override;

private:
  // -- member variables -------------------------------------------------------

  /// Points to the client actor that launched the query.
  caf::actor client_;
};

caf::behavior counter(caf::stateful_actor<counter_state>* self, expression expr,
                      index_actor index, bool skip_candidate_check);

} // namespace vast::system
