#ifndef VAST_SYSTEM_META_STORE_HPP
#define VAST_SYSTEM_META_STORE_HPP

#include "vast/data.hpp"

#include "vast/system/key_value_store.hpp"

namespace vast {
namespace system {

using meta_store_type = key_value_store_type<std::string, data>;

} // namespace system
} // namespace vast

#endif
