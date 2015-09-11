#include <ios>
#include <iomanip>

#include "vast/actor/accountant.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"
#include "vast/util/coding.h"

namespace vast {

accountant::state::state(local_actor* self)
  : basic_state{self, "accountant"} {
}

void accountant::state::init(path const& filename) {
  if (!exists(filename.parent())) {
    auto t = mkdir(filename.parent());
    if (!t) {
      VAST_ERROR_AT(self, t.error());
      self->quit(exit::error);
      return;
    }
  }
  VAST_DEBUG_AT(self, "opens log file:", filename);
  file_.open(filename.str());
  if (!file_.is_open()) {
    VAST_ERROR_AT(self, "failed to open file in:", filename);
    self->quit(exit::error);
    return;
  }
  file_ << "time\thost\tpid\tactor\tinstance\tkey\tvalue\n";
  if (! file_) {
    VAST_ERROR_AT(self, "encountered error with log file", filename);
    self->quit(exit::error);
  }
}

accountant::behavior accountant::make(stateful_pointer self,
                                      path const& filename) {
  self->state.init(filename);
  auto record = [=](auto const& name, auto const& key, auto const& value) {
    auto node = self->current_sender().node();
    auto now = time::snapshot().time_since_epoch();
    auto timestamp = time::duration_cast<time::double_seconds>(now).count();
    self->state.file_ << std::fixed << std::showpoint << std::setprecision(6)
                      << timestamp << '\t' << std::hex;
    for (auto byte : node.host_id())
      self->state.file_ << static_cast<int>(byte);
    self->state.file_
      << std::dec << '\t'
      << node.process_id() << '\t'
      << name << '\t'
      << self->current_sender()->id() << '\t'
      << key << '\t'
      << std::setprecision(6) << value << '\n';
  };
  self->trap_exit(true);
  return {
    [=](exit_msg const& msg) {
      // Delay termination if we have still samples lingering in the mailbox.
      auto n = self->mailbox().count();
      if (n == 0) {
        self->quit(msg.reason);
      } else {
        VAST_DEBUG_AT(self, "delays exit with", n, "messages in mailbox");
        self->trap_exit(false);
        self->send(message_priority::normal, self, self->current_message());
      }
    },
    [=](std::string const& name, std::string const& key,
        std::string const& value) {
      record(name, key, value);
    },
    // Helpers to avoid to_string(..) in sender context.
    [=](std::string const& name, std::string const& key, time::extent value) {
      auto us = time::duration_cast<time::microseconds>(value).count();
      record(name, key, us);
    },
    [=](std::string const& name, std::string const& key, int64_t value) {
      record(name, key, value);
    },
    [=](std::string const& name, std::string const& key, uint64_t value) {
      record(name, key, value);
    },
    [=](std::string const& name, std::string const& key, double value) {
      record(name, key, value);
    }
  };
}

} // namespace vast
