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

#include <cstddef>
#include <cstdint>
#include <string>

namespace vast::defaults {

namespace command {

extern const char* directory;
extern const char* endpoint;
extern const char* id;
extern const char* read_path;
extern const char* schema_path;
extern const char* write_path;
extern int64_t pseudo_realtime_factor;
extern size_t cutoff;
extern size_t flow_expiry;
extern size_t flush_interval;
extern size_t max_events;
extern size_t max_flow_age;
extern size_t max_flows;
extern std::string node_id;

} // namespace command

} // namespace vast::defaults
