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
  editline(char const* name = "");

  /// Destructs the editline instance and releases internal resources.
  ~editline();

  /// Sources an editline config.
  ///
  /// @param filename If not `nullptr` the config file name and otherwise
  /// attempts to read `$PWD/.editrc` and then `$HOME/.editrc`.
  ///
  /// @return `true` on success.
  bool source(char const* filename = nullptr);

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

  /// Pushes a string back into the input queue.
  /// @param str The string to put back.
  void put(char const* str);

  /// Resets the TTY and the editline parser.
  void reset();

  /// Adapts to a changing TTY size.
  void resize();

  /// Makes the TTY beep.
  void beep();

  /// Adds a command to the history.
  /// @param line The line to record.
  void history_add(char const* line);

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace util
} // namespace vast

#endif
