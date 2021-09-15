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
  xs["idle"] = mgr.idle();
  xs["congested"] = mgr.congested();
  // Downstream status.
  auto& out = mgr.out();
  auto& downstream = insert_record(xs, "downstream");
  downstream["buffered"] = count{out.buffered()};
  downstream["max-capacity"] = integer{out.max_capacity()};
  downstream["paths"] = count{out.num_paths()};
  downstream["stalled"] = out.stalled();
  downstream["clean"] = out.clean();
  out.for_each_path([&](auto& opath) {
    auto name = "slot-" + std::to_string(opath.slots.sender);
    auto& slot = insert_record(downstream, name);
    slot["pending"] = opath.pending();
    slot["clean"] = opath.clean();
    slot["closing"] = opath.closing;
    slot["next-batch-id"] = integer{opath.next_batch_id};
    slot["open-credit"] = integer{opath.open_credit};
    slot["desired-batch-size"] = integer{opath.desired_batch_size};
    slot["max-capacity"] = integer{opath.max_capacity};
  });
  // Upstream status.
  auto& upstream = insert_record(xs, "upstream");
  auto& ipaths = mgr.inbound_paths();
  if (!ipaths.empty())
    xs["inbound-paths-idle"] = mgr.inbound_paths_idle();
  for (auto ipath : ipaths) {
    auto name = "slot-" + std::to_string(ipath->slots.receiver);
    auto& slot = insert_record(upstream, name);
    slot["priority"] = to_string(ipath->prio);
    slot["assigned-credit"] = integer{ipath->assigned_credit};
    slot["last-acked-batch-id"] = integer{ipath->last_acked_batch_id};
  }
}

void fill_status_map(record& xs, caf::scheduled_actor* self) {
  xs["actor-id"] = count{self->id()};
  xs["thread-id"] = thread_id();
#if VAST_LINUX
  xs["pthread-id"] = integer{pthread_id()};
#endif // VAST_LINUX
  xs["name"] = self->name();
  xs["mailbox-size"] = count{self->mailbox().size()};
  size_t counter = 0;
  std::string name;
  for (auto& mgr : unique_values(self->stream_managers())) {
    name = "stream-";
    name += std::to_string(counter++);
    fill_status_map(insert_record(xs, name), *mgr);
  }
}

} // namespace vast::detail
