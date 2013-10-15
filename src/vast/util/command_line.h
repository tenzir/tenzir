#ifndef VAST_UTIL_COMMAND_LINE_H
#define VAST_UTIL_COMMAND_LINE_H

#include <map>
#include <vector>
#include "vast/util/editline.h"

namespace vast {
namespace util {

/// An abstraction for a mode-based command line.
class command_line
{
public:
  /// The callback function for commands. The argument represents the argument
  /// ot the command. The return value indicates whether the callback
  /// succeeded; it's up to the application to put semantics on it.
  using callback = std::function<bool(std::string)>;

  /// Creates a new mode for a set of related commands. Only one mode can
  /// be active at a time. Each mode has its own history.
  ///
  /// @param name The name of the mode.
  ///
  /// @param desc A description of the mode.
  ///
  /// @param prompt The prompt of the mode.
  ///
  /// @returns `true` on success.
  bool add_mode(std::string name, std::string desc, std::string prompt);

  /// Adds a command to an existing mode.
  /// @param mode The name of the mode.
  /// @param cmd The name of the command to add.
  /// @param f The callback to invoke on the arguments of *cmd*.
  /// @returns `true` on success.
  bool add_command(std::string const& mode, std::string cmd, callback f);

  /// Adds a callback for unknown (i.e., not added) commands.
  /// @param mode The name of the mode.
  /// @param f The callback to invoke on the arguments of *cmd*.
  /// @returns `true` on success.
  bool on_unknown_command(std::string const& mode, callback f);

  /// Enters a given mode.
  /// @param mode The name of mode.
  /// @returns `true` on success.
  bool push_mode(std::string const& mode);

  /// Leaves the current mode.
  /// @returns `true` on success.
  bool pop_mode();

  /// Appends an entry to the history of the current mode.
  /// @param entry The history entry to add.
  /// @returns `true` on success.
  bool append_to_history(std::string const& entry);

  /// Process a single command.
  ///
  /// @param callback_result A result parameter that contains the return value
  /// of the callback, if executed.
  ///
  /// @returns `true` if a registered callback executed and false if the
  /// entered command did exist resolve.
  bool process(bool& callback_result);

  /// Retrieves a single character from the user in a blocking fashion.
  /// @param c The result parameter containing the character from STDIN.
  /// @returns `true` on success.
  bool get(char& c); 

private:
  struct mode
  {
    mode(std::string name = "", std::string desc = "", std::string prompt = "");

    std::string name;
    std::string description;
    std::map<std::string, callback> callbacks;
    callback unknown_command;
    editline::history hist;
    editline el;
  };

  std::vector<std::shared_ptr<mode>> mode_stack_;
  std::map<std::string, std::shared_ptr<mode>> modes_;
};

} // namespace util
} // namespace vast

#endif
