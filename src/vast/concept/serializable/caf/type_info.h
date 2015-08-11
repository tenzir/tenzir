#ifndef VAST_CONCEPT_SERIALIZABLE_CAF_TYPE_INFO_H
#define VAST_CONCEPT_SERIALIZABLE_CAF_TYPE_INFO_H

#include <caf/abstract_uniform_type_info.hpp>

#include "vast/concept/serializable/caf/adapters.h"

namespace vast {

/// CAF type information which proxies (de)serialization calls through VAST's
/// serialization framework.
template <typename T>
class caf_type_info : public caf::abstract_uniform_type_info<T> {
public:
  caf_type_info(std::string name)
    : caf::abstract_uniform_type_info<T>(std::move(name)) {
  }

protected:
  void serialize(void const* ptr, caf::serializer* sink) const final {
    caf_to_vast_serializer s{*sink};
    auto x = reinterpret_cast<T const*>(ptr);
    s << *x;
  }

  void deserialize(void* ptr, caf::deserializer* source) const final {
    caf_to_vast_deserializer d{*source};
    auto x = reinterpret_cast<T*>(ptr);
    d >> *x;
  }
};

} // namespace vast

#endif
