#ifndef VAST_UTIL_EDITLINE_H
#define VAST_UTIL_EDITLINE_H

#include <string>
#include <functional>
#include <vector>
#include "vast/util/trial.h"

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

  /// A completion context.
  class completer
  {
  public:
    /// The callback to execute on the matching prefixes for a candidate
    /// string. The first argument represent the prefix which was given, and
    /// the second the corresponding matches. The return value, if not empty,
    /// represents the string to insert back onto the command line.
    using callback =
      std::function<std::string(std::string const&, std::vector<std::string>)>;

    /// Adds a string to complete.
    /// @param str The string to complete.
    /// @returns `true` if *str* did not already exist.
    bool add(std::string str);

    /// Removes a registered string from the completer.
    /// @param str The string to remove.
    /// @returns `true` if *str* existed.
    bool remove(std::string const& str);

    /// Replaces the existing completions with a given set of new ones.
    /// @param completions The new completions to use.
    void replace(std::vector<std::string> completions);

    /// Sets a callback handler for the list of matches.
    /// @param f The function to execute for matching completions.
    void on(callback f);

    /// Completes a given string by calling the given callback.
    /// @param prefix The string to complete.
    /// @returns The result of the completion function.
    trial<std::string> complete(std::string const& prefix) const;

  private:
    std::vector<std::string> strings_;
    callback callback_;
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

  /// Sets a new completer.
  /// @param comp The completer to use
  void set(completer comp);

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

  /// Retrieves the completion context.
  completer& completion();

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace util
} // namespace vast

#endif
