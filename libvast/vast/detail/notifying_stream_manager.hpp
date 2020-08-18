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

#include "vast/query_options.hpp"

#include <caf/default_downstream_manager.hpp>
#include <caf/detail/stream_stage_driver_impl.hpp>
#include <caf/detail/stream_stage_impl.hpp>
#include <caf/policy/arg.hpp>

namespace vast::detail {

template <class State>
void notify_listeners_if_clean(State& st, const caf::stream_manager& mgr) {
  if (!st.flush_listeners.empty() && mgr.inbound_paths_idle()
      && mgr.out().clean()) {
    st.notify_flush_listeners();
  }
}

// A custom stream manager that is able to notify when all data has been
// processed. It relies on the presence of a function
// `Self->state.notify_flush_listeners()`, which means that it is currently only
// usable in combination with the `index` actor.
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
    notify_listeners_if_clean(state(), *this);
  }

  void input_closed(error reason) override {
    super::input_closed(std::move(reason));
    notify_listeners_if_clean(state(), *this);
  }

  void finalize(const error& reason) override {
    super::finalize(reason);
    state().notify_flush_listeners();
  }

private:
  Self* self_;

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
caf::stream_stage_ptr<typename Trait::input, DownstreamManager>
attach_notifying_stream_stage(
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
