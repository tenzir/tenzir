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

/// Initializes VAST. This function must be called exactly once before creating
/// an instance of vast::program.
/// @param The program configuration.
void init(configuration const& config);

} // namespace vast

#endif
