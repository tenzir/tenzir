//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/stream_controlled.hpp"

#include <caf/actor_addr.hpp>
#include <caf/send.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace vast::system {

namespace {

auto flush_guard_counters
  = std::unordered_map<caf::actor_addr, std::unique_ptr<std::atomic<int>>>{};
auto flush_guard_mutex = std::mutex{};

} // namespace

flush_guard::flush_guard(flush_listener_actor flush_listener)
  : flush_listener_{std::move(flush_listener)} {
  VAST_ASSERT(!counter_);
  VAST_ASSERT(flush_listener_);
  VAST_ASSERT(flush_listener_->address());
  auto lock = std::unique_lock{flush_guard_mutex};
  auto& counter = flush_guard_counters[flush_listener_->address()];
  if (!counter) {
    counter = std::make_unique<std::atomic<int>>(1);
    caf::anon_send(flush_listener_, atom::flush_v, atom::add_v);
  } else {
    counter->fetch_add(1, std::memory_order_relaxed);
  }
  counter_ = counter.get();
}

flush_guard::~flush_guard() noexcept {
  VAST_ASSERT(counter_);
  VAST_ASSERT(flush_listener_);
  if (counter_->fetch_sub(1, std::memory_order_acq_rel) == 1)
    caf::anon_send(flush_listener_, atom::flush_v, atom::sub_v);
}

void intrusive_ptr_add_ref(const flush_guard* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const flush_guard* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

} // namespace vast::system
