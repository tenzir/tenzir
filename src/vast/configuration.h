#ifndef VAST_CONFIGURATION_H
#define VAST_CONFIGURATION_H

#include "util/configuration.h"

namespace vast {

/// The program configuration.
class configuration : public util::configuration
{
public:
  configuration();

protected:
  virtual void verify() override;
};

/// Initializes all singletons and creates global state.
/// @param config The configuration.
bool initialize(configuration const& config);

/// Destroys all singletons and deletes global state.
void shutdown();

} // namespace vast

#endif
