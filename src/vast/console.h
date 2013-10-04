#ifndef VAST_CONSOLE_H
#define VAST_CONSOLE_H

#include <deque>
#include "vast/actor.h"
#include "vast/cow.h"
#include "vast/util/command_line.h"

namespace vast {

class event;

/// A console-based, interactive query client.
class console : public actor<console>
{
public:
  /// Spawns the console client.
  /// @param search The search actor the console interacts with.
  console(cppa::actor_ptr search);

  void act();
  char const* description() const;

private:
  void delayed_prompt_display(size_t ms = 100);

  std::deque<cow<event>> results_;
  cppa::actor_ptr search_;
  cppa::actor_ptr query_;
  util::command_line cmdline_;
};

} // namespace vast

#endif
