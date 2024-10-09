//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/fill_status_map.hpp"

#include "tenzir/config.hpp"
#include "tenzir/data.hpp"

#include <caf/config_value.hpp>
#include <caf/scheduled_actor.hpp>

#include <sstream>
#include <thread>

#if TENZIR_LINUX

#  ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#  endif // _GNU_SOURCE

#  include <sys/syscall.h>
#  include <sys/types.h>

#  include <unistd.h>

#endif // TENZIR_LINUX

namespace {

std::string thread_id() {
  std::stringstream ss;
  ss << std::this_thread::get_id();
  return std::move(ss).str();
}

#if TENZIR_LINUX

// Gets the PID associated with a certain PThread for comparison with the PID
// shown in htop's tree mode.
// c.f. https://stackoverflow.com/a/26526741/1974431
pid_t pthread_id() {
  return syscall(SYS_gettid);
}

#endif // TENZIR_LINUX

} // namespace

namespace tenzir::detail {

void fill_status_map(record& xs, caf::scheduled_actor* self) {
  xs["actor-id"] = uint64_t{self->id()};
  xs["thread-id"] = thread_id();
#if TENZIR_LINUX
  xs["pthread-id"] = int64_t{pthread_id()};
#endif // TENZIR_LINUX
  xs["name"] = self->name();
  xs["mailbox-size"] = uint64_t{self->mailbox().size()};
}

} // namespace tenzir::detail
