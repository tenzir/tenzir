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

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/stream_stage_driver.hpp>

#include "vast/type.hpp"
#include "vast/event.hpp"

#include "vast/system/indexer_manager.hpp"

namespace vast::system {

using indexer_stage_filter = type;

struct indexer_stage_selector {
  inline bool operator()(const indexer_stage_filter& f, const event& x) const {
    return f == x.type();
  }
};

using indexer_downstream_manager
  = caf::broadcast_downstream_manager<event, indexer_stage_filter,
                                      indexer_stage_selector>;

class indexer_stage_driver
  : public caf::stream_stage_driver<event, indexer_downstream_manager> {
public:
  using super = caf::stream_stage_driver<event, indexer_downstream_manager>;

  using index_manager_factory = std::function<indexer_manager_ptr()>;

  indexer_stage_driver(downstream_manager_type& dm, index_manager_factory fac);

  ~indexer_stage_driver() noexcept override;

  void process(caf::downstream<output_type>& out,
               std::vector<input_type>& batch) override;

private:
  indexer_manager_ptr im_;
  index_manager_factory factory_;
};

} // namespace vast::system
