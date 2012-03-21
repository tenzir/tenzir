#ifndef VAST_META_TYPE_H
#define VAST_META_TYPE_H

#include <string>
#include <vector>
#include <boost/operators.hpp>
#include "vast/meta/forward.h"
#include "vast/util/crc.h"

namespace vast {
namespace meta {

/// A type in the taxonomy.
class type : boost::equality_comparable<type>
           , public std::enable_shared_from_this<type>
{
public:
    /// Construts an empty type.
    type();

    /// Destroys a type. Objects of type type are owned through the base
    /// pointer and must therefore be deleted publicly and virtually.
    virtual ~type();

    /// Equality operator.
    /// @param other The other type.
    /// @return @c true @e iff both types are equal.
    bool operator==(const type& other) const;

    /// Tests whether a type is a symbol. Since only symbols have a name, a
    /// type is a symbol @e iff it has name.
    /// @return @c true @e iff the type is a symbol.
    bool is_symbol() const;

    /// Gets the name of the type.
    /// @return The type name.
    const std::string& name() const;

    /// Creates symbol by seting a name of this type.
    /// @param name The type name.
    type_ptr symbolize(const std::string& name);

    /// Gets the string representation of the type.
    /// @param resolve_symbols Whether to print the symbols or actual types.
    /// @return The string representation of the type.
    std::string to_string(bool resolve_symbols = false) const;

protected:
    /// Clones an object, i.e., perform a deep copy.
    ///
    /// @return While the base class implementation returns a @c type*, the
    ///     derived classes should return a @c derived*.
    /// @see http://accu.org/index.php/journals/522
    virtual type* clone() const = 0;

    virtual std::string to_string_impl(bool resolve_symbols) const = 0;

private:
    std::string name_;
    util::crc32::value_type checksum_;
};

/// A basic type. This type fits into an integral type, such as @c int, 
/// @c double, or @c uint64_t.
class basic_type : public type
{
public:
    basic_type();
    virtual ~basic_type();

protected:
    virtual basic_type* clone() const = 0;
    virtual std::string to_string_impl(bool resolve_symbols) const = 0;
};

/// A complex type.
class complex_type : public type
{
public:
    complex_type();
    virtual ~complex_type();

protected:
    virtual complex_type* clone() const = 0;
    virtual std::string to_string_impl(bool resolve_symbols) const = 0;
};

/// A container type.
class container_type : public complex_type
{
public:
    container_type();
    virtual ~container_type();

protected:
    virtual container_type* clone() const = 0;
    virtual std::string to_string_impl(bool resolve_symbols) const = 0;
};

/// A helper macro to declare complex types.
#define VAST_BEGIN_DECLARE_TYPE(t, base)                                    \
    class t : public base                                                   \
    {                                                                       \
    public:                                                                 \
        t();                                                                \
        virtual ~t();                                                       \
                                                                            \
    protected:                                                              \
        virtual t* clone() const;                                           \
        virtual std::string to_string_impl(bool resolve_symbols) const;     \
                                                                            \
    public:

/// A helper macro to declare compound/container types.
#define VAST_END_DECLARE_TYPE                                               \
    };

/// A helper macro to declare basic types.
#define VAST_DECLARE_BASIC_TYPE(t)                                          \
    VAST_BEGIN_DECLARE_TYPE(t, basic_type)                                  \
    VAST_END_DECLARE_TYPE

/// @class vast::meta::unknown_type @brief Inernal type for unknown types.
/// @class vast::meta::addr_type @brief An IP address type.
/// @class vast::meta::bool_type @brief A @c bool type.
/// @class vast::meta::count_type @brief A count (unsigned) type.
/// @class vast::meta::double_type @brief A @c double type.
/// @class vast::meta::int_type @brief An @c int type.
/// @class vast::meta::interval_type @brief A time interval type.
/// @class vast::meta::file_type @brief A file type.
/// @class vast::meta::port_type @brief A port type.
/// @class vast::meta::string_type @brief A string type.
/// @class vast::meta::subnet_type @brief A subnet type.
/// @class vast::meta::time_type @brief A time type.
/// @class vast::meta::enum_type @brief An enum type.
/// @class vast::meta::record @brief A record type with arguments.
/// @class vast::meta::vector_type @brief A vector type.
/// @class vast::meta::set_type @brief A set type.
/// @class vast::meta::table_type @brief A table type.

VAST_BEGIN_DECLARE_TYPE(unknown_type, type)
VAST_END_DECLARE_TYPE

VAST_DECLARE_BASIC_TYPE(addr_type)
VAST_DECLARE_BASIC_TYPE(bool_type)
VAST_DECLARE_BASIC_TYPE(count_type)
VAST_DECLARE_BASIC_TYPE(double_type)
VAST_DECLARE_BASIC_TYPE(int_type)
VAST_DECLARE_BASIC_TYPE(interval_type)
VAST_DECLARE_BASIC_TYPE(file_type)
VAST_DECLARE_BASIC_TYPE(port_type)
VAST_DECLARE_BASIC_TYPE(string_type)
VAST_DECLARE_BASIC_TYPE(subnet_type)
VAST_DECLARE_BASIC_TYPE(time_type)

VAST_BEGIN_DECLARE_TYPE(enum_type, complex_type)
    std::vector<std::string> fields;        ///< The enum fields.
VAST_END_DECLARE_TYPE

VAST_BEGIN_DECLARE_TYPE(record_type, complex_type)
    std::vector<argument_ptr> args;         ///< The record arguments.
VAST_END_DECLARE_TYPE

VAST_BEGIN_DECLARE_TYPE(vector_type, container_type)
    type_ptr elem_type;                      ///< The vector type
VAST_END_DECLARE_TYPE

VAST_BEGIN_DECLARE_TYPE(set_type, container_type)
    type_ptr elem_type;                      ///< The set (value) type.
VAST_END_DECLARE_TYPE

VAST_BEGIN_DECLARE_TYPE(table_type, container_type)
    type_ptr key_type;                      ///< The key type.
    type_ptr value_type;                    ///< The value type.
VAST_END_DECLARE_TYPE

//
// Free functions for dynamic type casts.
//

/// Determine whether a type is a basic type.
/// @tparam Base The base pointer type.
/// @param base A pointer to a type.
/// @return @c true @e iff the type is a @link basic_type basic type@endlink.
template <typename Base>
bool is_basic_type(Base base)
{
    return std::dynamic_pointer_cast<basic_type>(base).get() != nullptr;
}

/// Determine whether a type is a @link complex_type complex type@endlink.
/// @tparam Base The base pointer type.
/// @param base A pointer to a type.
/// @return @c true @e iff the type is a complex type.
template <typename Base>
bool is_complex_type(Base base)
{
    return std::dynamic_pointer_cast<complex_type>(base).get() != nullptr;
}

/// Determine whether a type is a container type.
/// @tparam Base The base pointer type.
/// @param base A pointer to a type.
/// @return @c true @e iff the type is container type.
template <typename Base>
bool is_container_type(Base base)
{
    return std::dynamic_pointer_cast<container_type>(base).get() != nullptr;
}

/// Determine whether a type is a record type.
/// @tparam Base The base pointer type.
/// @param base A pointer to a type.
/// @return @c true @e iff the type is a record type.
template <typename Base>
bool is_record_type(Base base)
{
    return std::dynamic_pointer_cast<record_type>(base).get() != nullptr;
}

} // namespace meta
} // namespace vast

#endif
