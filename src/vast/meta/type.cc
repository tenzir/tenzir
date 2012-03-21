#include "vast/meta/type.h"

#include <boost/spirit/include/karma.hpp>

namespace vast {
namespace meta {

type::type()
{
    // TODO: compute and save the type checksum.
}

type::~type()
{
}

bool type::operator==(const type& rhs) const
{
    return checksum_ == rhs.checksum_;
}

bool type::is_symbol() const
{
    return ! name_.empty();
}

const std::string& type::name() const
{
    return name_;
}

type_ptr type::symbolize(const std::string& name)
{
    if (is_symbol())
    {
        type* t = clone();
        t->name_ = name;

        return type_ptr(t);
    }

    name_ = name;

    return shared_from_this();
}

std::string type::to_string(bool resolve_symbols) const
{
    return resolve_symbols || ! is_symbol() ?
        to_string_impl(resolve_symbols) : name();
}

basic_type::basic_type()
  : type()
{
}

basic_type::~basic_type()
{
}

complex_type::complex_type()
  : type()
{
}

complex_type::~complex_type()
{
}

container_type::container_type()
  : complex_type()
{
}

container_type::~container_type()
{
}

#define VAST_DEFINE_TYPE(t, parent)                                 \
    t::~t()                                                         \
    {                                                               \
    }                                                               \
                                                                    \
    t::t()                                                          \
      : parent()                                                    \
    {                                                               \
    }                                                               \
                                                                    \
    t* t::clone() const                                             \
    {                                                               \
        return new t(*this);                                        \
    }

VAST_DEFINE_TYPE(unknown_type, type)
VAST_DEFINE_TYPE(addr_type, basic_type)
VAST_DEFINE_TYPE(bool_type, basic_type)
VAST_DEFINE_TYPE(count_type, basic_type)
VAST_DEFINE_TYPE(double_type, basic_type)
VAST_DEFINE_TYPE(int_type, basic_type)
VAST_DEFINE_TYPE(interval_type, basic_type)
VAST_DEFINE_TYPE(file_type, basic_type)
VAST_DEFINE_TYPE(port_type, basic_type)
VAST_DEFINE_TYPE(string_type, basic_type)
VAST_DEFINE_TYPE(subnet_type, basic_type)
VAST_DEFINE_TYPE(time_type, basic_type)
VAST_DEFINE_TYPE(enum_type, complex_type)
VAST_DEFINE_TYPE(record_type, complex_type)
VAST_DEFINE_TYPE(vector_type, container_type)
VAST_DEFINE_TYPE(set_type, container_type)
VAST_DEFINE_TYPE(table_type, container_type)

#define VAST_DEFINE_TYPE_TO_STRING_IMPL(type, str)                         \
    std::string type::to_string_impl(bool resolve_symbols) const           \
    {                                                                      \
        return resolve_symbols || ! is_symbol() ?                          \
            to_string_impl(resolve_symbols) : name();                      \
    }

VAST_DEFINE_TYPE_TO_STRING_IMPL(unknown_type, "[unknown]")
VAST_DEFINE_TYPE_TO_STRING_IMPL(addr_type, "addr")
VAST_DEFINE_TYPE_TO_STRING_IMPL(bool_type, "bool")
VAST_DEFINE_TYPE_TO_STRING_IMPL(count_type, "count")
VAST_DEFINE_TYPE_TO_STRING_IMPL(double_type, "double")
VAST_DEFINE_TYPE_TO_STRING_IMPL(int_type, "int")
VAST_DEFINE_TYPE_TO_STRING_IMPL(interval_type, "interval")
VAST_DEFINE_TYPE_TO_STRING_IMPL(file_type, "file")
VAST_DEFINE_TYPE_TO_STRING_IMPL(port_type, "port")
VAST_DEFINE_TYPE_TO_STRING_IMPL(string_type, "string")
VAST_DEFINE_TYPE_TO_STRING_IMPL(subnet_type, "subnet")
VAST_DEFINE_TYPE_TO_STRING_IMPL(time_type, "time")

std::string enum_type::to_string_impl(bool resolve_symbols) const
{
    using namespace boost::spirit;

    if (is_symbol() && ! resolve_symbols)
        return name();

    std::stringstream ss;

    ss << karma::format(
                 "enum {" << (stream % ", ") << '}',
                 fields);

    return ss.str();
}

std::string record_type::to_string_impl(bool resolve_symbols) const
{
    using namespace boost::spirit;

    if (is_symbol() && ! resolve_symbols)
        return name();

    std::stringstream ss;

    ss << karma::format(
                 "record {" << (stream % ", ") << '}',
                 args);

    return ss.str();
}

std::string vector_type::to_string_impl(bool resolve_symbols) const
{
    if (is_symbol() && ! resolve_symbols)
        return name();

    std::string str("vector of ");
    str += elem_type->to_string();

    return str;
}

std::string set_type::to_string_impl(bool resolve_symbols) const
{
    if (is_symbol() && ! resolve_symbols)
        return name();

    std::string str("set[");
    str += elem_type->to_string();
    str += ']';

    return str;
}

std::string table_type::to_string_impl(bool resolve_symbols) const
{
    if (is_symbol() && ! resolve_symbols)
        return name();

    std::string str("table[");
    str += key_type->to_string();
    str += "] of ";
    str += value_type->to_string();

    return str;
}

} // namespace meta
} // namespace vast
