#ifndef VAST_UTIL_CONFIGURATION_H
#define VAST_UTIL_CONFIGURATION_H

#include <sstream>
#include <string>
#include <map>
#include <vector>
#include "vast/exception.h"

namespace vast {
namespace util {

/// A command line parser and program option utility.
class configuration
{
  configuration(configuration const&) = delete;
  configuration& operator=(configuration) = delete;

public:
  /// Default-constructs a configuration.
  configuration() = default;

  /// Initializes the configuration from a configuration file.
  /// @param filename The name of the configuration file.
  /// @return `true` if configuration initialization was successful.
  /// @throw error::config
  void load(std::string const& filename);

  /// Initializes the configuration from command line parameters.
  /// @argc The argc parameter from main.
  /// @param argv The argv parameter from main.
  /// @throw error::config
  void load(int argc, char *argv[]);

  /// Checks whether the given option is set.
  /// @param option Name of the option to check.
  /// @return @c true if the given option is set.
  /// @throw error::config
  bool check(std::string const& option) const;

  /// Returns the value of the given option.
  /// @param opt The name of the option.
  /// @return The option value.
  /// @throw error::config
  std::string const& get(std::string const& opt) const;

  /// Retrieves an option as a specific type.
  /// @tparam T The type to convert the option to.
  /// @param opt The name of the option.
  /// @return The converted option value.
  /// @throw error::config
  template <typename T>
  T as(std::string const& opt) const
  {
    auto o = find_option(opt);
    if (! o)
      throw error::config("unknown option", opt);
    if (o->values_.empty())
      throw error::config("option has no value", opt);
    if (o->max_vals_ > 1)
      throw error::config("cannot cast multi-value option", opt);
    T x;
    std::istringstream ss(o->values_.front());
    ss >> x;
    return x;
  }

  /// Prints the usage into a given output stream.
  /// @param sink The output stream to receive the configuration.
  /// @param show_all Whether to also print invisible options.
  void usage(std::ostream& sink, bool show_all = false);
  
protected:
  class option
  {
    friend configuration;
  public:
    option(std::string name, std::string desc, char shortcut = '\0');

    template <typename T>
    option& init(T const& x)
    {
      std::ostringstream ss;
      ss << x;
      values_.push_back(ss.str());
      max_vals_ = (values_.size() == 1) ? 1 : -1;
      return *this;
    }

    template<class T, typename... Args>
    option& init(T const& head, Args... tail)
    {
      init(head);
      init(tail...);
      return *this;
    }

    option& single();
    option& multi(size_t n = -1);

  private:
    std::string name_;
    std::vector<std::string> values_;
    std::string description_;
    size_t max_vals_ = 0;
    bool defaulted_ = true;
    char shortcut_ = '\0';
  };

  /// A proxy class to add options to the configuration.
  class block
  {
    friend class configuration;
    block(block const&) = delete;
    block& operator=(block other) = delete;

  public:
    /// Move-constructs a block.
    /// @param other The block to move.
    block(block&& other);

    /// Separates hierarchical options.
    static constexpr char const* separator = ".";

    /// Adds a new option.
    /// @param name The option name.
    /// @param desc The option description.
    option& add(std::string const& name, std::string desc);

    /// Adds a new option with shortcut.
    /// @param shortcut The shortcut of the option (single character).
    /// @param name The option name.
    /// @param desc The option description.
    option& add(char shortcut, std::string const& name, std::string desc);

    /// Sets the visibility of this block when displaying the usage.
    bool visible() const;

    /// Sets the visibility of this block when displaying the usage.
    void visible(bool flag);

  private:
    block(std::string name, std::string prefix, configuration* config);

    std::string qualify(std::string const& name) const;

    bool visible_ = true;
    std::string name_;
    std::string prefix_;
    std::vector<option> options_;
    configuration* config_;
  };

  /// Creates a new option block.
  /// @param name The name of the option block.
  /// @param prefix The prefix of the block.
  /// @return The option block.
  block& create_block(std::string name, std::string prefix = "");

  /// Verifies that two given options are not specified at the same time.
  /// @param opt1 The first option.
  /// @param opt2 The second option.
  void conflicts(std::string const& opt1, std::string const& opt2) const;

  /// Verifies an option dependency.
  /// @param needy The option that depends on *required*.
  /// @param required The option that must exist when *needy* exists.
  void depends(std::string const& needy, std::string const& required) const;

  /// Sets the option banner for the usage.
  /// @param The banner string.
  void banner(std::string banner);

  /// Called after successfully loading the configuration to check the
  /// integrity of the options.
  virtual void verify();

private:
  option* find_option(std::string const& opt);
  option const* find_option(std::string const& opt) const;

  std::string banner_;
  std::map<std::string, std::string> shortcuts_;
  std::vector<block> blocks_;
};

template <>
inline std::vector<std::string> configuration::as(std::string const& opt) const
{
  auto o = find_option(opt);
  if (! o)
    throw error::config("invalid option cast", opt);
  return o->values_;
}

} // namespace util
} // namespace vast

#endif
