#ifndef VAST_DETAIL_TYPE_MANAGER_H
#define VAST_DETAIL_TYPE_MANAGER_H

#include <memory>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <set>
#include <string>
#include "vast/aliases.h"
#include "vast/singleton.h"

namespace vast {

class global_type_info;

namespace detail {

/// Manages runtime type information.
class type_manager : public singleton<type_manager>
{
  friend singleton<type_manager>;

public:
  /// Registers a type with the type system.
  /// @param ti The type information of the type to register.
  /// @param f The factory constructing a `global_type_info` instance for *ti*.
  /// @returns `true` on sucess, `false` iff *ti* has already been registered.
  bool add(std::type_info const& ti,
           std::function<global_type_info*(type_id)> f);

  /// Retrieves type information by C++ RTTI.
  ///
  /// @param ti The C++ RTTI object to lookup.
  ///
  /// @returns A pointer to VAST's type information for *ti* or `nullptr` if no
  /// such information exists.
  global_type_info const* lookup(std::type_info const& ti) const;

  /// Retrieves type information by type ID.
  ///
  /// @param id The type ID to lookup.
  ///
  /// @returns A pointer to VAST's type information for *id* or `nullptr` if no
  /// such information exists.
  global_type_info const* lookup(type_id id) const;

  /// Registers a convertible-to relationship for an announced type.
  /// @param from The announced type information to convert to *to*.
  /// @param to The type information to convert *from* to.
  /// @returns `true` iff the type registration succeeded.
  bool add_link(global_type_info const* from, std::type_info const& to);

  /// Checks a convertible-to relationship for an announced type.
  /// @param from The announced type information to convert to *to*.
  /// @param to The type information to convert *from* to.
  /// @returns `true` iff *from* is convertible to *to*.
  bool check_link(global_type_info const* from, std::type_info const& to) const;

  /// Invokes a function on each known type information.
  /// @param f The function to invoke on each global_type_info instance.
  void each(std::function<void(global_type_info const&)> f) const;

private:
  // Singleton implementation.
  static type_manager* create();
  void initialize();
  void destroy();
  void dispose();

  type_id id_ = 0;
  std::unordered_map<std::type_index, std::unique_ptr<global_type_info>> by_ti_;
  std::unordered_map<type_id, global_type_info const*> by_id_;
  std::unordered_map<type_id, std::set<std::type_index>> conversions_;
};

} // namespace detail
} // namespace vast

#endif
