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

#include <caf/detail/stream_stage_impl.hpp>

namespace vast::detail {

template <class State>
void notify_listeners_if_clean(State& st, const caf::stream_manager& mgr) {
  if (!st.flush_listeners.empty() && mgr.inbound_paths_idle()
      && mgr.out().clean()) {
    st.notify_flush_listeners();
  }
}

template <class Driver>
class notifying_stream_manager : public caf::detail::stream_stage_impl<Driver> {
public:
  using super = caf::detail::stream_stage_impl<Driver>;

  template <class Self>
  notifying_stream_manager(Self* self)
    : caf::stream_manager(self),
      super(self, self) {
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
  auto self() {
    return this->driver_.self();
  }

  auto& state() {
    return self()->state;
  }
};

} // namespace vast::detail
