#ifndef VAST_UTIL_EDITLINE_H
#define VAST_UTIL_EDITLINE_H

#include <string>
#include <functional>

namespace vast {
namespace util {

/// Wraps command line editing functionality provided by `libedit`.
class editline
{
public:
  /// A fixed-size history of entries.
  class history
  {
  public:
    history(int size = 1000, bool unique = true, std::string filename = "");
    ~history();

    void save();

    void load();

    void add(std::string const& str);

    void append(std::string const& str);

    void enter(std::string const& str);

  private:
    friend editline;
    struct impl;
    std::unique_ptr<impl> impl_;
  };

  /// The prompt to display in front of each command.
  class prompt
  {
  public:
    /// Constructs a prompt.
    /// @param str The initial prompt string.
    /// @param color The color of *str*.
    /// @param e The escape character to be used in the prompt.
    prompt(std::string str = ">> ", char const* color = nullptr, char e = '\1');

    /// Adds a (colored) string to the end of the prompt.
    /// @param str The string to append.
    /// @param color The color of *str*.
    void push(std::string str, char const* color = nullptr);

    /// Gets the prompt string to be passed to the editline prompt function.
    /// @param The prompt string.
    char const* display() const;

    /// Retrieves the escape character of the prompt.
    /// @returns The escape character for the prompt function.
    char escape() const;

  private:
    std::string str_;
    char esc_;
  };

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

  /// Sets the prompt.
  /// @param p The new prompt.
  void set(prompt p);

  /// Sets a history.
  /// @param hist The history to use.
  void set(history& hist);

  /// Adds a completion.
  /// @param cmd The name of the command.
  /// @param desc A description of *cmd*.
  bool complete(std::string cmd, std::string desc = "");

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

  /// Resets the TTY and editline parser.
  void reset();

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace util
} // namespace vast

#endif
