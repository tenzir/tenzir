#ifndef VAST_CONFIGURATION_H
#define VAST_CONFIGURATION_H

#include "util/configuration.h"

namespace vast {

/// The program configuration.
class configuration : public util::configuration<configuration>
{
public:
  configuration() = default;

  void initialize();
  std::string banner() const;
};

} // namespace vast

#endif
