#ifndef VAST_CONFIGURATION_HPP
#define VAST_CONFIGURATION_HPP

#include <caf/actor_system_config.hpp>

namespace vast {

/// Bundles all configuration parameters of a VAST system.
class configuration : public caf::actor_system_config {
public:
  /// Default-constructs a configuration.
  configuration();

  /// Constructs a configuration from the command line.
  configuration(int argc, char** argv);
};

} // namespace vast

#endif // VAST_CONFIGURATION_HPP
