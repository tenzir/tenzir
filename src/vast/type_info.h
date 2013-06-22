#ifndef VAST_TYPE_INFO_H
#define VAST_TYPE_INFO_H

#include <cstdint>
#include <string>
#include <typeinfo>
#include <type_traits>
#include "vast/typedefs.h"
#include "vast/util/operators.h"

namespace vast {

class stable_type_info 
  : util::equality_comparable<stable_type_info>,
    util::equality_comparable<stable_type_info, std::type_info>,
    util::totally_ordered<stable_type_info>
{
  friend bool operator<(stable_type_info const& x, stable_type_info const& y);
  friend bool operator==(stable_type_info const& x, stable_type_info const& y);
  friend bool operator==(stable_type_info const& x, std::type_info const& y);

public:
  type_id id() const;

  std::string const& name() const;

  virtual bool equals(std::type_info const& ti) const = 0;

  virtual void* destroy(void const* instance) const = 0;

  virtual void* create(void const* instance = nullptr) const = 0;

protected:
  stable_type_info(type_id id, std::string name);

private:
  type_id id_ = 0;
  std::string name_;
};

/// Retrieves runtime type information about a given type.
///
/// @tparam T The type to inquire information about.
///
/// @return If VAST knows about `T`, the function returns a stable_type_info
/// object and an empty option otherwise.
stable_type_info const* stable_typeid(std::type_info const& ti);

// Helper overload.
template <typename T>
stable_type_info const* stable_typeid()
{
  return stable_typeid(typeid(T));
}

/// Registers a type with VAST's runtime type system.
/// @tparam T The type to register.
/// @note The order of invocations determines the underlying type
/// identifier. For example:
///
///     announce<T>();
///     announce<U();
///
/// is not the same as:
///
///     announce<U();
///     announce<T>();
///
/// It is therefore **crucial** to ensure a consistent order during the
/// announcement phase.
template <typename T>
bool announce()
{
  static_assert(std::is_default_constructible<T>::value,
                "announced types must be default-constructible");

  //return detail::type_manager::instance().add<T>();
  return false;
}
} // namespace vast

#endif
