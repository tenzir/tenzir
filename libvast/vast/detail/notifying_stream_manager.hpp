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
    notify_listeners_if_clean();
  }

  void input_closed(error reason) override {
    super::input_closed(std::move(reason));
    notify_listeners_if_clean();
  }

  void finalize(const error& reason) override {
    super::finalize(reason);
    notify_listeners();
  }

private:
  void notify_listeners() {
    auto self = this->driver_.self();
    auto& st = self->state;
    st.notify_flush_listeners();
  }

  void notify_listeners_if_clean() {
    auto& st = this->driver_.self()->state;
    if (!st.flush_listeners.empty() && this->inbound_paths().empty()
        && this->out().clean()) {
      notify_listeners();
    }
  }
};

} // namespace vast::detail
