#ifndef VAST_CONSOLE_H
#define VAST_CONSOLE_H

#include <deque>
#include "vast/actor.h"
#include "vast/expression.h"
#include "vast/event.h"
#include "vast/file_system.h"
#include "vast/individual.h"
#include "vast/util/command_line.h"

namespace vast {

/// A console-based, interactive query client.
struct console : actor_mixin<console, sentinel>
{
  struct options
  {
    uint64_t batch_size = 10;
    bool auto_follow = true;
  };

  /// A stream of events representing a result. One can add events to the
  /// result and seek forward/backward.
  class result : intrusive_base<result>, public individual
  {
  public:
    /// Default-constructs a result.
    result() = default;

    /// Constructs a result from a valid AST.
    result(expression ast);

    /// Saves the current result state to a given directory.
    /// @param p The directory to save the results under.
    /// @returns `true` on success.
    bool save(path const& p) const;

    /// Loads the result state from a given directory.
    /// @param p The directory to load the results from.
    /// @returns `true` on success.
    bool load(path const& p);

    /// Adds an event to this result.
    /// @param e The event to add.
    void add(event e);

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
    expression const& ast() const;

    /// Retrieves the number of events in the result.
    /// @returns The number of all events.
    size_t size() const;

    /// Sets the hits.
    /// @param n The number of hits of this result.
    void hits(uint64_t n);

    /// Retrieves the number of hits.
    /// @returns The number of hits of this result.
    uint64_t hits() const;

    /// Sets the progress.
    /// @param p the new progress in *[0,1]*.
    void progress(double p);

    /// Retrieves the progress.
    /// @returns The progress result progress.
    double progress() const;

    /// Retrieves the progress in percent.
    /// @param precision The number of decimal digits.
    /// @returns The progress result progress.
    double percent(size_t precision = 2) const;

  private:
    using pos_type = uint64_t;

    expression ast_;
    uint64_t hits_ = 0;
    double progress_ = 0.0;
    pos_type pos_ = 0;
    std::deque<event> events_;

  private:
    friend access;
    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);
  };

  enum print_mode
  {
    none,
    query,
    fail,
    warn,
    info
  };

  /// Spawns the console client.
  /// @param search The search actor the console interacts with.
  /// @param dir The directory where to save state.
  console(caf::actor search, path dir);

  caf::message_handler make_handler();
  std::string name() const;

  /// Prints status information to standard error.
  std::ostream& print(print_mode mode);

  /// Shows the prompt.
  /// @param ms The number of milliseconds to delay the prompt.
  void prompt(size_t ms = 100);

  /// Entes the query control mode.
  void follow();

  /// Leaves the query control mode.
  void unfollow();

  path dir_;
  intrusive_ptr<result> active_;
  std::vector<intrusive_ptr<result>> results_;
  std::map<caf::actor_addr, std::pair<caf::actor, intrusive_ptr<result>>>
    connected_;
  caf::actor search_;
  caf::actor keystroke_monitor_;
  util::command_line cmdline_;
  options opts_;
  bool following_ = false;
  bool appending_ = false;
  uint64_t expected_ = 0;
};

} // namespace vast

#endif
