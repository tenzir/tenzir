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

#ifndef VAST_SYSTEM_RUN_PCAP_WRITER_HPP
#define VAST_SYSTEM_RUN_PCAP_WRITER_HPP

#include <memory>
#include <string>
#include <string_view>

#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/expression.hpp"
#include "vast/logger.hpp"

#include "vast/system/run_writer_base.hpp"
#include "vast/system/run_writer_base.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/tracker.hpp"

#include "vast/format/pcap.hpp"

#include "vast/concept/parseable/to.hpp"

#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"

#include "vast/detail/make_io_stream.hpp"

namespace vast::system {

/// PCAP subcommant to `import`.
/// @relates application
class run_pcap_writer : public run_writer_base {
public:
  using super = run_writer_base;

  run_pcap_writer(command* parent, std::string_view name);

protected:
  expected<caf::actor> make_sink(caf::scoped_actor& self,
                                 caf::message args) override;

private:
  std::string output;
  bool uds;
  unsigned flush;
};

} // namespace vast::system

#endif
