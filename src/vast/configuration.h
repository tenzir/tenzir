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

} // namespace vast

#endif
