//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/fill_status_map.hpp"

#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/detail/algorithms.hpp"
#include "vast/system/status.hpp"

#include <caf/config_value.hpp>
#include <caf/downstream_manager.hpp>
#include <caf/inbound_path.hpp>
#include <caf/outbound_path.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/stream_manager.hpp>

#include <sstream>
#include <thread>

#if VAST_LINUX

#  ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#  endif // _GNU_SOURCE

#  include <sys/syscall.h>
#  include <sys/types.h>

#  include <unistd.h>

#endif // VAST_LINUX

namespace {

std::string thread_id() {
  std::stringstream ss;
  ss << std::this_thread::get_id();
  return std::move(ss).str();
}

#if VAST_LINUX

// Gets the PID associated with a certain PThread for comparison with the PID
// shown in htop's tree mode.
// c.f. https://stackoverflow.com/a/26526741/1974431
pid_t pthread_id() {
  return syscall(SYS_gettid);
}

#endif // VAST_LINUX

} // namespace

namespace vast::detail {

void fill_status_map(record& xs, caf::stream_manager& mgr) {
  // Manager status.
  put(xs, "idle", mgr.idle());
  put(xs, "congested", mgr.congested());
  // Downstream status.
  auto& out = mgr.out();
  auto& downstream = put_dictionary(xs, "downstream");
  put(downstream, "buffered", out.buffered());
  put(downstream, "max-capacity", out.max_capacity());
  put(downstream, "paths", out.num_paths());
  put(downstream, "stalled", out.stalled());
  put(downstream, "clean", out.clean());
  out.for_each_path([&](auto& opath) {
    auto name = "slot-" + std::to_string(opath.slots.sender);
    auto& slot = put_dictionary(downstream, name);
    put(slot, "pending", opath.pending());
    put(slot, "clean", opath.clean());
    put(slot, "closing", opath.closing);
    put(slot, "next-batch-id", opath.next_batch_id);
    put(slot, "open-credit", opath.open_credit);
    put(slot, "desired-batch-size", opath.desired_batch_size);
    put(slot, "max-capacity", opath.max_capacity);
  });
  // Upstream status.
  auto& upstream = put_dictionary(xs, "upstream");
  auto& ipaths = mgr.inbound_paths();
  if (!ipaths.empty())
    put(xs, "inbound-paths-idle", mgr.inbound_paths_idle());
  for (auto ipath : ipaths) {
    auto name = "slot-" + std::to_string(ipath->slots.receiver);
    auto& slot = put_dictionary(upstream, name);
    put(slot, "priority", to_string(ipath->prio));
    put(slot, "assigned-credit", ipath->assigned_credit);
    put(slot, "last-acked-batch-id", ipath->last_acked_batch_id);
  }
}

void fill_status_map(record& xs, caf::scheduled_actor* self) {
  put(xs, "actor-id", self->id());
  put(xs, "thread-id", thread_id());
#if VAST_LINUX
  put(xs, "pthread-id", pthread_id());
#endif // VAST_LINUX
  put(xs, "name", self->name());
  put(xs, "mailbox-size", self->mailbox().size());
  size_t counter = 0;
  std::string name;
  for (auto& mgr : unique_values(self->stream_managers())) {
    name = "stream-";
    name += std::to_string(counter++);
    fill_status_map(put_dictionary(xs, name), *mgr);
  }
}

} // namespace vast::detail
