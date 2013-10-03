#ifndef VAST_UTIL_EDITLINE_H
#define VAST_UTIL_EDITLINE_H

#include <string>
#include <functional>

namespace vast {
namespace util {

class editline
{
public:
  /// Constructs an editline context.
  /// @param The name of the edit line context.
  editline(char const* name = "vast");

  ~editline();

  /// Sources an editline config.
  ///
  /// @param filename If not `nullptr` the config file name and otherwise
  /// attempts to read `$PWD/.editrc` and then `$HOME/.editrc`.
  ///
  /// @return `true` on success.
  bool source(char const* filename = nullptr);

  /// Adds a completion.
  /// @param cmd The name of the command.
  /// @param desc A description of *cmd*.
  bool complete(std::string cmd, std::string desc = "");

  /// Sets the prompt.
  /// @param str The new prompt string.
  void prompt(std::string str);

  /// Retrieves a character from the TTY.
  /// @param c The result parameter containing the character.
  /// @returns `true` on success.
  bool get(char& c);

  /// Retrieves a line from the TTY.
  /// @param line The result parameter containing the line.
  /// @returns `true` on success.
  bool get(std::string& line);

  /// Adds a string to the current line.
  /// @param str The string to add.
  void put(std::string const& str);

  /// Retrieves the current line.
  /// @return The current line.
  std::string line();

  /// Retrieves the current cursor position.
  /// @return The position of the cursor.
  size_t cursor();

  /// Appends to the current element of the history.
  /// @param str The string to append.
  void history_add(std::string const& str);

  /// Appends to the last new element of the history.
  /// @param str The string to add.
  void history_append(std::string const& str);

  /// Adds a new element to the history, and, if necessary, removes the oldest
  /// entry to keep the list to the history size.
  ///
  /// @param str The string to enter.
  void history_enter(std::string const& str);

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace util
} // namespace vast

#endif
