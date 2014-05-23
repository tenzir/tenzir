#ifndef FRAMEWORK_CONFIGURATION_H
#define FRAMEWORK_CONFIGURATION_H

#include "vast/util/configuration.h"

namespace unit {

class configuration : public vast::util::configuration<configuration>
{
public:
  void initialize();
  std::string banner() const;
};

} // namespace unit

#endif
