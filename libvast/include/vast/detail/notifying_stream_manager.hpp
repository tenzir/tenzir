//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/query_options.hpp"

#include <caf/default_downstream_manager.hpp>
#include <caf/detail/stream_stage_driver_impl.hpp>
#include <caf/detail/stream_stage_impl.hpp>
#include <caf/policy/arg.hpp>

namespace vast::detail {

template <class State>
void notify_listeners_if_clean(State& st, const caf::stream_manager& mgr,
                               caf::stream_slot slot
                               = caf::invalid_stream_slot) {
  if (st.flush_listeners.empty())
    return;
  // We intentionally don't check the inbound path state here because it will
  // only be marked as idle after an ack was sent for the last batch that was
  // received. However, acks are only sent once for each credit round, which
  // means that sometimes we wouldn't notify even though all batches are done.
  // In that case the listener would never get the notification and hang.
  if (slot != caf::invalid_stream_slot) {
    if (mgr.out().clean(slot))
      st.notify_flush_listeners();
  } else if (mgr.out().clean())
    st.notify_flush_listeners();
}

// A custom stream manager that is able to notify when all data has been
// processed. It relies on `Self->state` being a struct containing a function
// `notify_flush_listeners()` and a vector `flush_listeners`, which means that
// it is currently only usable in combination with the `index` or the
// `active_partition` actor.
template <class Self, class Driver>
class notifying_stream_manager : public caf::detail::stream_stage_impl<Driver> {
public:
  using super = caf::detail::stream_stage_impl<Driver>;

  template <class... Ts>
  notifying_stream_manager(Self* self, Ts&&... xs)
    : caf::stream_manager(self),
      super(self, std::forward<Ts>(xs)...),
      self_(self) {
    // nop
  }

  using super::handle;

  void handle(caf::stream_slots slots,
              caf::upstream_msg::ack_batch& x) override {
    super::handle(slots, x);
    auto slot = slots.receiver == notification_slot ? notification_slot
                                                    : caf::invalid_stream_slot;
    notify_listeners_if_clean(state(), *this, slot);
  }

  void input_closed(caf::error reason) override {
    super::input_closed(std::move(reason));
    notify_listeners_if_clean(state(), *this);
  }

  void finalize(const caf::error& reason) override {
    super::finalize(reason);
    // During shutdown of a stateful actor, CAF first destroys the state
    // in `local_actor::on_exit()` and then proceeds to stop the stream
    // managers with an `unreachable` error, so we can't touch it here.
    if (reason != caf::exit_reason::unreachable)
      state().notify_flush_listeners();
  }

  void set_notification_slot(caf::stream_slot slot) {
    notification_slot = slot;
  }

private:
  Self* self_;

  caf::stream_slot notification_slot = caf::invalid_stream_slot;

  auto self() {
    return self_;
  }

  auto& state() {
    return self()->state;
  }
};

/// Create a `notifying_stream_stage` and attaches it to the given actor.
// This is essentially a copy of `caf::attach_continous_stream_stage()`, but
// since the construction of the `stream_stage_impl` is buried quite deep there
// it is necesssary to duplicate the code.
template <class Self, class Init, class Fun, class Finalize = caf::unit_t,
          class DownstreamManager = caf::default_downstream_manager_t<Fun>,
          class Trait = caf::stream_stage_trait_t<Fun>>
auto attach_notifying_stream_stage(
  Self* self, bool continuous, Init init, Fun fun, Finalize fin = {},
  [[maybe_unused]] caf::policy::arg<DownstreamManager> token = {}) {
  using input_type = typename Trait::input;
  using output_type = typename Trait::output;
  using state_type = typename Trait::state;
  static_assert(
    std::is_same<void(state_type&),
                 typename caf::detail::get_callable_trait<Init>::fun_sig>::value,
    "Expected signature `void (State&)` for init function");
  static_assert(
    std::is_same<void(state_type&, caf::downstream<output_type>&, input_type),
                 typename caf::detail::get_callable_trait<Fun>::fun_sig>::value,
    "Expected signature `void (State&, downstream<Out>&, In)` "
    "for consume function");
  using caf::detail::stream_stage_driver_impl;
  using driver = stream_stage_driver_impl<typename Trait::input,
                                          DownstreamManager, Fun, Finalize>;
  using impl = notifying_stream_manager<Self, driver>;
  auto ptr = caf::make_counted<impl>(self, std::move(init), std::move(fun),
                                     std::move(fin));
  if (continuous)
    ptr->continuous(true);
  return ptr;
}

} // namespace vast::detail
