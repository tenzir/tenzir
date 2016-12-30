#ifndef VAST_SYSTEM_CONFIGURATION_HPP
#define VAST_SYSTEM_CONFIGURATION_HPP

#include <string>
#include <vector>

#include <caf/actor_system_config.hpp>

namespace vast {
namespace system {

/// Bundles all configuration parameters of a VAST system.
class configuration : public caf::actor_system_config {
public:
  /// Default-constructs a configuration.
  configuration();

  /// Constructs a configuration from the command line.
  /// @param argc The argument counter of `main`.
  /// @param argv The argument vector of `main`.
  configuration(int argc, char** argv);

  /// Constructs a configuration from a vector of string options.
  /// @param opts The vector with CAF options.
  configuration(const std::vector<std::string>& opts);
};

} // namespace system
} // namespace vast

#endif
