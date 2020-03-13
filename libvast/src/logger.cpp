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

#include "vast/logger.hpp"

#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/system/configuration.hpp"

#include <caf/atom.hpp>

namespace vast {

namespace {

/// Converts a verbosity atom to its integer counterpart.
unsigned to_level_int(caf::atom_value x) {
  switch (atom_uint(to_lowercase(x))) {
    default:
      return VAST_LOG_LEVEL_QUIET;
    case caf::atom_uint("quiet"):
      return VAST_LOG_LEVEL_QUIET;
    case caf::atom_uint("error"):
      return VAST_LOG_LEVEL_ERROR;
    case caf::atom_uint("warning"):
      return VAST_LOG_LEVEL_WARNING;
    case caf::atom_uint("info"):
      return VAST_LOG_LEVEL_INFO;
    case caf::atom_uint("verbose"):
      return VAST_LOG_LEVEL_VERBOSE;
    case caf::atom_uint("debug"):
      return VAST_LOG_LEVEL_DEBUG;
    case caf::atom_uint("trace"):
      return VAST_LOG_LEVEL_TRACE;
  }
}

// A trick that uses explicit template instantiation of a static member
// as explained here: https://gist.github.com/dabrahams/1528856
template <class Tag>
struct stowed {
  static typename Tag::type value;
};
template <class Tag>
typename Tag::type stowed<Tag>::value;

template <class Tag, typename Tag::type x>
struct stow_private {
  stow_private() {
    stowed<Tag>::value = x;
  }
  static stow_private instance;
};
template <class Tag, typename Tag::type x>
stow_private<Tag, x> stow_private<Tag, x>::instance;

// A tag type for caf::logger::cfg.
struct logger_cfg {
  using type = caf::logger::config(caf::logger::*);
};

template struct stow_private<logger_cfg, &caf::logger::cfg_>;

} // namespace

void fixup_logger(const system::configuration& cfg) {
  // Reset the logger so we can support the VERBOSE level
  namespace lg = defaults::logger;
  auto logger = caf::logger::current_logger();
  // A workaround for the lack of an accessor function for logger.cfg_,
  // see https://github.com/actor-framework/actor-framework/issues/1066.
  auto& cfg_ = logger->*stowed<logger_cfg>::value;
  auto verbosity = caf::get_if<caf::atom_value>(&cfg, "system.verbosity");
  auto file_verbosity = verbosity ? *verbosity : lg::file_verbosity;
  auto console_verbosity = verbosity ? *verbosity : lg::console_verbosity;
  file_verbosity = get_or(cfg, "system.file-verbosity", file_verbosity);
  console_verbosity
    = caf::get_or(cfg, "system.console-verbosity", console_verbosity);
  cfg_.file_verbosity = to_level_int(file_verbosity);
  cfg_.console_verbosity = to_level_int(console_verbosity);
  cfg_.verbosity = std::max(cfg_.file_verbosity, cfg_.console_verbosity);
}

} // namespace vast
