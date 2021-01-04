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

#include "vast/accountant/config.hpp"

#include "vast/component_config.hpp"
#include "vast/concept/parseable/detail/posix.hpp"
#include "vast/concept/parseable/vast/table_slice_encoding.hpp"

namespace vast::system {

caf::expected<accountant_config>
to_accountant_config(const caf::settings& opts) {
  accountant_config result;
  extract_settings(result.enable, opts, "enable");
  extract_settings(result.self_sink.enable, opts, "self_sink.enable");
  extract_settings(result.self_sink.slice_size, opts, "self_sink.slize_size");
  extract_settings(result.self_sink.slice_type, opts, "self_sink.slize_type");
  extract_settings(result.file_sink.enable, opts, "file_sink.enable");
  extract_settings(result.file_sink.path, opts, "file_sink.path");
  extract_settings(result.uds_sink.enable, opts, "uds_sink.enable");
  extract_settings(result.uds_sink.path, opts, "uds_sink.path");
  extract_settings(result.uds_sink.type, opts, "uds_sink.type");
  return result;
}

} // namespace vast::system
