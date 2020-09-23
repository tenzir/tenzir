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

#include "vast/format/writer_factory.hpp"

#include "vast/config.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/make_writer.hpp"
#include "vast/format/null.hpp"
#include "vast/format/zeek.hpp"

#if VAST_HAVE_PCAP
#  include "vast/format/pcap.hpp"
#endif

#if VAST_HAVE_ARROW
#  include "vast/format/arrow.hpp"
#endif

namespace vast {

void factory_traits<format::writer>::initialize() {
  using namespace format;
  using fac = factory<writer>;
  fac::add("ascii", make_writer<ascii::writer>);
  fac::add("csv", make_writer<csv::writer>);
  fac::add("json", make_writer<format::json::writer>);
  fac::add("null", make_writer<null::writer>);
  fac::add("zeek", make_writer<zeek::writer>);
#if VAST_HAVE_PCAP
  fac::add("pcap", make_writer<pcap::writer>);
#endif
#if VAST_HAVE_ARROW
  fac::add("arrow", make_writer<arrow::writer>);
#endif
}

} // namespace vast
