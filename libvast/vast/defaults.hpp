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

#include <string>

namespace vast::defaults {

// -- default root command values ---------------------------------------------
constexpr auto root_command_dir = "vast";
constexpr auto root_command_endpoint = ":42000";
extern const std::string root_command_id;
constexpr auto root_command_node = false;
constexpr auto root_command_version = false;

// -- default start command values --------------------------------------------
constexpr auto start_command_bare = false;
constexpr auto start_command_foreground = false;

// -- default pcap writer command values --------------------------------------
constexpr auto pcap_writer_command_write = "-";
constexpr auto pcap_writer_command_uds = false;
constexpr auto pcap_writer_command_flush = 10000u;

// -- default pcap reader command values --------------------------------------
constexpr auto pcap_reader_command_read = "-";
constexpr auto pcap_reader_command_schema = "";
constexpr auto pcap_reader_command_uds = false;
constexpr auto pcap_reader_command_cutoff =
  std::numeric_limits<uint64_t>::max();
constexpr auto pcap_reader_command_flow_max = size_t{1} << 20;
constexpr auto pcap_reader_command_flow_age = 60u;
constexpr auto pcap_reader_command_flow_expiry = 10u;
constexpr auto pcap_reader_command_pseudo_realtime = 0;

// -- default export command values -------------------------------------------
constexpr auto export_command_continuous = false;
constexpr auto export_command_historical = false;
constexpr auto export_command_unified = false;
constexpr auto export_command_events = 0u;

// -- default reader command values -------------------------------------------
constexpr auto reader_command_read = "-";
constexpr auto reader_command_schema = "";
constexpr auto reader_command_uds = false;

// -- default writer command values -------------------------------------------
constexpr auto writer_command_write = "-";
constexpr auto writer_command_uds = false;

} // namespace vast::defaults

