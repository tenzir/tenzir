#ifndef VAST_SYSTEM_CONFIGURATION_HPP
#define VAST_SYSTEM_CONFIGURATION_HPP

#include <caf/actor_system_config.hpp>

namespace vast {
namespace system {

/// Bundles all configuration parameters of a VAST system.
class configuration : public caf::actor_system_config {
public:
  /// Default-constructs a configuration.
  configuration();

  /// Constructs a configuration from the command line.
  configuration(int argc, char** argv);
};

} // namespace system
} // namespace vast

#endif
