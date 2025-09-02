//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/proxy_actor.hpp"

namespace tenzir::plugins::routes {

proxy::proxy(proxy_actor::pointer self)
  : self_{self} {
}

auto proxy::make_behavior() -> proxy_actor::behavior_type {
  return {
    [this](atom::get) -> caf::result<table_slice> {
      return get();
    },
    [this](atom::put, table_slice slice) -> caf::result<void> {
      return put(std::move(slice));
    },
  };
}

auto proxy::get() -> caf::result<table_slice> {
  TENZIR_ASSERT(not get_rp_, "concurrent get requests not allowed");
  // If there's data in the queue, return it immediately
  if (queue_) {
    auto result = std::move(*queue_);
    queue_.reset();
    return result;
  }
  // No data available, store the promise for later fulfillment
  get_rp_ = self_->make_response_promise<table_slice>();
  return *get_rp_;
}

auto proxy::put(table_slice slice) -> caf::result<void> {
  // Assert no concurrent put requests
  TENZIR_ASSERT(not put_rp_, "concurrent put requests not allowed");
  // If there's a pending get request, fulfill it immediately
  if (get_rp_) {
    TENZIR_ASSERT(not queue_);
    get_rp_->deliver(std::move(slice));
    get_rp_.reset();
    return {};
  }
  // No pending get request, check if queue is empty
  if (queue_) {
    // Queue is full, store the promise for later fulfillment
    put_rp_ = self_->make_response_promise<void>();
    queue_ = std::move(slice);
    return *put_rp_;
  }
  // Queue is empty, store the slice
  queue_ = std::move(slice);
  return {};
}

} // namespace tenzir::plugins::routes
