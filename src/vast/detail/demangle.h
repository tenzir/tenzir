#ifndef VAST_DETAIL_DEMANGLE_H
#define VAST_DETAIL_DEMANGLE_H

#include <string>
#include <typeinfo>

namespace vast {
namespace detail {

/// Demangles a platform-specific type name.
/// @param name The platform-spcific type-name to demangle.
/// @param A platform-indpendent representation of *name*.
std::string demangle(char const* typeid_name);

/// Demangles a platform-specific type name from C++ RTTI.
/// @param info An `std::type_info` instance.
/// @param A platform-indpendent name for the type represented by *info*.
std::string demangle(const std::type_info& info);

} // namespace detail
} // namespace vast

#endif
