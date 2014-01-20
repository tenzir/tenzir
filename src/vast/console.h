#ifndef VAST_CONSOLE_H
#define VAST_CONSOLE_H

#include <deque>
#include "vast/actor.h"
#include "vast/cow.h"
#include "vast/expression.h"
#include "vast/file_system.h"
#include "vast/individual.h"
#include "vast/util/command_line.h"

namespace vast {

/// A console-based, interactive query client.
struct console : actor<console>
{
  struct options
  {
    uint64_t batch_size = 10;
    bool auto_follow = true;
  };

  enum print_mode
  {
    error,
    query
  };

  /// A stream of events representing a result. One can add events to the
  /// result and seek forward/backward.
  class result : public individual
  {
  public:
    /// Constructs a result from a valid AST.
    result(expr::ast ast);

    /// Adds an event to this result.
    /// @param e The event to add.
    void add(cow<event> e);

    /// Applies a function to a given number of existing events and advances
    /// the internal position.
    ///
    /// @param n The number of events to apply *f* to.
    ///
    /// @param f The function to apply to the *n* next events.
    ///
    /// @returns The number of events that *f* has been applied to.
    size_t apply(size_t n, std::function<void(event const&)> f);

    /// Adjusts the stream position.
    /// @param n The number of events.
    /// @returns The number of events seeked.
    size_t seek_forward(size_t n);

    /// Adjusts the stream position.
    /// @param n The number of events.
    /// @returns The number of events seeked.
    size_t seek_backward(size_t n);

    /// Retrieves the AST of this query result.
    expr::ast const& ast() const;

    /// Retrieves the number of events in the result.
    /// @returns The number of all events.
    size_t size() const;

  private:
    using pos_type = uint64_t;

    pos_type pos_ = 0;
    std::deque<cow<event>> events_;
    expr::ast ast_;
  };

  /// Spawns the console client.
  /// @param search The search actor the console interacts with.
  /// @param dir The directory where to save state.
  console(cppa::actor_ptr search, path dir);

  void act();
  char const* description() const;

  std::ostream& print(print_mode mode) const;
  void show_prompt(size_t ms = 100);
  std::pair<cppa::actor_ptr, result*> to_result(std::string const& str);

  path dir_;
  std::map<cppa::actor_ptr, result> results_;
  result* current_result_;
  cppa::actor_ptr current_query_;
  cppa::actor_ptr search_;
  util::command_line cmdline_;
  options opts_;
  bool follow_mode_ = false;
};

} // namespace vast

#endif
