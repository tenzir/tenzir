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

#include "vast/system/default_application.hpp"

#include "vast/system/application.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/export_command.hpp"
#include "vast/system/import_command.hpp"
#include "vast/system/reader_command.hpp"
#include "vast/system/remote_command.hpp"
#include "vast/system/start_command.hpp"
#include "vast/system/writer_command.hpp"

#include "vast/format/ascii.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/mrt.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/system/pcap_reader_command.hpp"
#include "vast/system/pcap_writer_command.hpp"
#endif

namespace vast::system {

default_application::default_application() {
  // Add program commands that run locally.
  add<start_command>("start");
  // Add program composed commands.
  import_ = add<import_command>("import");
  import_->add<reader_command<format::bro::reader>>("bro");
  import_->add<reader_command<format::mrt::reader>>("mrt");
  import_->add<reader_command<format::bgpdump::reader>>("bgpdump");
  export_ = add<export_command>("export");
  export_->add<writer_command<format::bro::writer>>("bro");
  export_->add<writer_command<format::csv::writer>>("csv");
  export_->add<writer_command<format::ascii::writer>>("ascii");
  export_->add<writer_command<format::json::writer>>("json");
#ifdef VAST_HAVE_PCAP
  import_->add<pcap_reader_command>("pcap");
  export_->add<pcap_writer_command>("pcap");
#endif
  // Add program commands that always run remotely.
  add<remote_command>("stop");
  add<remote_command>("show");
  add<remote_command>("spawn");
  add<remote_command>("send");
  add<remote_command>("kill");
  add<remote_command>("peer");
}

} // namespace vast::system
