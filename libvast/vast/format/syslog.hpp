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

namespace vast::format::syslog {

// TODO: implement a syslog parser using VAST's parser DSL.
// TODO: implement a reader class that allows reading syslog files.

// For inspiration, take a look at these files:
// - libvast/vast/concept/parseable/*
//    All files in parseable/ build the foundation if VAST's parser DSL.
// - libvast/vast/format/mrt.hpp 
//    mrt.hpp also includes a parser, which is necessary for syslog, too.
// - libvast/vast/format/zeek.hpp, libvast/vast/format/pcap.hpp
//    These are easy to read reader/writer implementations.

} // namespace vast::format::syslog
