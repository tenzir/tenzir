#ifndef VAST_CONFIGURATION_H
#define VAST_CONFIGURATION_H

#include <boost/program_options.hpp>

namespace vast {

namespace po = boost::program_options;

/// The program configuration.
class configuration
{
  configuration(configuration const&) = delete;
  configuration& operator=(configuration const&) = delete;

public:
  /// Constructor.
  configuration();

  /// Initializes the configuration from a configuration file.
  /// @param filename The name of the configuration file.
  /// @return `true` if configuration initialization was successful.
  bool load(std::string const& filename = "");

  /// Initialize the configuration from the command line parameters.
  /// @argc The argc parameter from main.
  /// @param argv The argv parameter from main.
  /// @return `true` if configuration initialization was successful.
  bool load(int argc, char *argv[]);

  /// Check whether the given option is set.
  /// @param option Name of the option to check.
  /// @return @c true if the given option is set.
  bool check(char const* option) const;

  /// Return the value of the given option.
  /// @param option The name of the option.
  /// @return Option value.
  template <typename T>
  T const& get(char const* option) const
  {
    return config_[option].as<T>();
  }

  /// Prints the program help.
  ///
  /// @param out The stream to print the help instructions to.
  ///
  /// @param advanced If @c true, print all available program options rather
  /// than just a small subset.
  void print(std::ostream& out, bool advanced = false) const;

private:
  void init();

  /// Checks that two given options are not specified at the same time.
  /// @param opt1 Option 1.
  /// @param opt2 Option 2.
  void conflicts(char const* opt1, char const* opt2) const;

  /// Verifies an option dependency.
  /// @param for_what The parameter which depends on another option.
  /// @param required The required parameter.
  void depends(char const* for_what, char const* required) const;

  po::variables_map config_;              ///< Program configuration.
  po::options_description visible_;       ///< Visible options to the user.
  po::options_description all_;           ///< All existing program options.
};

} // namespace vast

#endif
