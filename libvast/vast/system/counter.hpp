// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
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

  void init(expression expr, index_actor index, archive_actor archive,
            bool skip_candidate_check);

protected:
  // -- implementation hooks ---------------------------------------------------

  void process_hits(const ids& hits) override;

  void process_end_of_hits() override;

private:
  // -- member variables -------------------------------------------------------

  /// Stores whether we can skip candidate checks.
  bool skip_candidate_check_;

  /// Stores the user-defined query.
  expression expr_;

  /// Points to the ARCHIVE for performing candidate checks.
  archive_actor archive_;

  /// Points to the client actor that launched the query.
  caf::actor client_;

  /// Stores how many pending requests remain for the ARCHIVE.
  size_t pending_archive_requests_ = 0;

  /// Caches INDEX hits for evaluating candidates from the ARCHIVE.
  ids hits_;

  /// Caches expr_ tailored to different layouts.
  std::unordered_map<type, expression> checkers_;
};

caf::behavior
counter(caf::stateful_actor<counter_state>* self, expression expr,
        index_actor index, archive_actor archive, bool skip_candidate_check);

} // namespace vast::system
