#include <cstring>
#include <fstream>

#include <caf/all.hpp>

#include "vast/key.h"
#include "vast/actor/identifier.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"

using namespace caf;

namespace vast {

identifier::identifier(actor store, path dir, event_id batch_size)
  : default_actor{"identifier"},
    store_{std::move(store)},
    dir_{std::move(dir)},
    batch_size_{batch_size} {
}

void identifier::on_exit() {
  store_ = invalid_actor;
  if (!save_state()) {
    VAST_ERROR(this, "failed to save local ID state");
    VAST_ERROR(this, "current ID:", id_);
    VAST_ERROR(this, "available IDs:", available_);
  }
}

behavior identifier::make_behavior() {
  if (exists(dir_)) {
    // Load current batch size.
    std::ifstream available{to_string(dir_ / "available")};
    if (!available) {
      VAST_ERROR(this, "failed to open ID batch file:", dir_ / "available",
                 '(' << std::strerror(errno) << ')');
      quit(exit::error);
      return {};
    }
    available >> available_;
    VAST_INFO(this, "found", available_, "local IDs");
    // Load next ID.
    std::ifstream next{to_string(dir_ / "next")};
    if (!next) {
      VAST_ERROR(this, "failed to open ID file:", dir_ / "next",
                 '(' << std::strerror(errno) << ')');
      quit(exit::error);
      return {};
    }
    next >> id_;
    VAST_INFO(this, "found next event ID:", id_);
  }
  return {
    [=](id_atom) {
      return id_;
    },
    [=](request_atom, event_id n) {
      auto rp = make_response_promise();
      if (n == 0) {
        rp.deliver(make_message(error{"cannot hand out 0 ids"}));
        return;
      }
      // If the requester wants more than we can locally offer, we give
      // everything we have, but double the batch size to avoid future
      // shortage.
      if (n > available_) {
        n = available_;
        batch_size_ *= 2;
        VAST_VERBOSE(this, "got exhaustive request, doubling batch size to",
                     batch_size_);
      }
      VAST_DEBUG(this, "hands out [" << id_ << ',' << id_ + n << "),",
                 available_ - n, "local IDs remaining");
      rp.deliver(make_message(id_atom::value, id_, id_ + n));
      id_ += n;
      available_ -= n;
      // Replenish if we're running low of IDs (or are already out of 'em).
      if (available_ == 0 || available_ < batch_size_ * 0.1) {
        // Avoid too frequent replenishing.
        if (time::snapshot() - last_replenish_ < time::seconds(10)) {
          batch_size_ *= 2;
          VAST_VERBOSE(this, "had to replenish twice within 10 secs,",
                       "doubling batch size to", batch_size_);
        }
        last_replenish_ = time::snapshot();
        VAST_DEBUG(this, "replenishes local IDs:", available_, "available,",
                   batch_size_, "requested");
        VAST_ASSERT(max_event_id - id_ >= batch_size_);
        sync_send(store_, add_atom::value, key::str("id"), batch_size_).then(
          [=](event_id old, event_id now) {
            id_ = old;
            available_ = now - old;
            VAST_VERBOSE(this, "got", available_, "new IDs starting at", old);
            if (!save_state()) {
              quit(exit::error);
              VAST_ERROR(this, "failed to save local ID state");
            }
          },
          [=](error const& e) {
            VAST_ERROR(this, "got error:", e);
            VAST_ERROR(this, "failed to obtain", n, "new IDs:");
            quit(exit::error);
          },
          others >> [=] {
            VAST_ERROR(this, "got unexpected message:",
                       to_string(current_message()));
            VAST_ERROR(this, "failed to obtain", n, "new IDs");
            quit(exit::error);
          }
        );
      }
    },
    catch_unexpected()
  };
}

bool identifier::save_state() {
  if (id_ == 0)
    return true;
  if (!exists(dir_) && !mkdir(dir_))
    return false;
  std::ofstream available{to_string(dir_ / "available")};
  if (!available)
    return false;
  available << available_ << std::endl;
  std::ofstream next{to_string(dir_ / "next")};
  if (!next)
    return false;
  next << id_ << std::endl;
  return true;
}

} // namespace vast
