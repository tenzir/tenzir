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

#include "vast/system/indexer_stage_driver.hpp"

#include <caf/downstream.hpp>
#include <caf/stream_manager.hpp>

namespace vast::system {

indexer_stage_driver::indexer_stage_driver(downstream_manager_type& dm,
                                           index_manager_factory fac)
  : super(dm),
    im_(fac()),
    factory_(std::move(fac)) {
  // nop
}

indexer_stage_driver::~indexer_stage_driver() noexcept {
  // nop
}

void indexer_stage_driver::process(caf::downstream<output_type>& out,
                                   std::vector<input_type>& batch) {
  // Iterate batch to start new INDEXER actors when needed.
  for (auto& x : batch) {
    auto entry = im_->get_or_add(x.type());
    if (entry.second) {
      auto slot = out_.parent()
                  ->add_unchecked_outbound_path<output_type>(entry.first);
      out_.set_filter(slot, x.type());
    }
    // Dispatch event.
    out.push(std::move(x));
  }
}

} // namespace vast::system
