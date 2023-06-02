//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/accountant_config.hpp"

#include "vast/component_config.hpp"
#include "vast/concept/parseable/detail/posix.hpp"
#include "vast/concept/parseable/vast/table_slice_encoding.hpp"

namespace vast {

caf::expected<accountant_config>
to_accountant_config(const caf::settings& opts) {
  accountant_config result;
  extract_settings(result.self_sink.enable, opts, "self-sink.enable");
  extract_settings(result.self_sink.slice_size, opts, "self-sink.slice-size");
  extract_settings(result.self_sink.slice_type, opts, "self-sink.slice-type");
  extract_settings(result.file_sink.enable, opts, "file-sink.enable");
  extract_settings(result.file_sink.path, opts, "file-sink.path");
  extract_settings(result.file_sink.real_time, opts, "file_sink.real-time");
  extract_settings(result.uds_sink.enable, opts, "uds-sink.enable");
  extract_settings(result.uds_sink.path, opts, "uds-sink.path");
  extract_settings(result.uds_sink.real_time, opts, "uds-sink.real-time");
  extract_settings(result.uds_sink.type, opts, "uds-sink.type");
  return result;
}

} // namespace vast
