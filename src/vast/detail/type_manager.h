#ifndef VAST_DETAIL_TYPE_MANAGER_H
#define VAST_DETAIL_TYPE_MANAGER_H

#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <string>
#include "vast/singleton.h"
#include "vast/typedefs.h"

namespace vast {

class stable_type_info;

namespace detail {

/// Manages runtime type information.
class type_manager : public singleton<type_manager>
{
  friend singleton<type_manager>;

public:
  /// Registers a type with the type system.
  /// @tparam T The type to register.
  /// @throw `std::runtime_error` if `T` has already been registered.
  void add(std::type_info const& ti, std::unique_ptr<stable_type_info> uti);

  /// Retrieves type information by C++ RTTI.
  ///
  /// @param ti The C++ RTTI object to lookup.
  ///
  /// @return A pointer to VAST's type information for *ti* or `nullptr` if no
  /// such information exists.
  stable_type_info const* lookup(std::type_info const& ti) const;

  /// Retrieves type information by type ID.
  ///
  /// @param id The type ID to lookup.
  ///
  /// @return A pointer to VAST's type information for *id* or `nullptr` if no
  /// such information exists.
  stable_type_info const* lookup(type_id id) const;

  /// Retrieves type information by type name.
  ///
  /// @param name The type name to lookup.
  ///
  /// @return A pointer to VAST's type information for *name* or `nullptr` if
  /// no such information exists.
  stable_type_info const* lookup(std::string const& name) const;

private:
  // Singleton implementation.
  static type_manager* create();
  void initialize();
  void destroy();
  void dispose();

  std::atomic<uint16_t> id_;
  std::unordered_map<std::type_index, std::unique_ptr<stable_type_info>> by_ti_;
  std::unordered_map<type_id, stable_type_info*> by_id_;
  std::unordered_map<std::string, stable_type_info*> by_name_;
};

} // namespace detail
} // namespace vast

#endif
