#ifndef VAST_TYPE_H
#define VAST_TYPE_H

#include "vast/aliases.h"
#include "vast/string.h"
#include "vast/value_type.h"
#include "vast/util/operators.h"
#include "vast/util/variant.h"
#include "vast/util/print.h"

namespace vast {

class type;

#define VAST_DEFINE_BASIC_TYPE(name)                 \
  struct name : util::equality_comparable<name>      \
  {                                                  \
    friend bool operator==(name const&, name const&) \
    {                                                \
      return true;                                   \
    }                                                \
  };

VAST_DEFINE_BASIC_TYPE(invalid_type)
VAST_DEFINE_BASIC_TYPE(bool_type)
VAST_DEFINE_BASIC_TYPE(int_type)
VAST_DEFINE_BASIC_TYPE(uint_type)
VAST_DEFINE_BASIC_TYPE(double_type)
VAST_DEFINE_BASIC_TYPE(time_range_type)
VAST_DEFINE_BASIC_TYPE(time_point_type)
VAST_DEFINE_BASIC_TYPE(string_type)
VAST_DEFINE_BASIC_TYPE(regex_type)
VAST_DEFINE_BASIC_TYPE(address_type)
VAST_DEFINE_BASIC_TYPE(prefix_type)
VAST_DEFINE_BASIC_TYPE(port_type)

#undef VAST_DEFINE_BASIC_TYPE

struct enum_type : util::equality_comparable<enum_type>
{
  enum_type() = default;
  enum_type(std::vector<string> fields);

  std::vector<string> fields;

  friend bool operator==(enum_type const& lhs, enum_type const& rhs);
};

struct vector_type : util::equality_comparable<vector_type>
{
  vector_type() = default;
  vector_type(intrusive_ptr<type> elem);

  intrusive_ptr<type> elem_type;

  friend bool operator==(vector_type const& lhs, vector_type const& rhs);
};

struct set_type : util::equality_comparable<set_type>
{
  set_type() = default;
  set_type(intrusive_ptr<type> elem);

  intrusive_ptr<type> elem_type;

  friend bool operator==(set_type const& lhs, set_type const& rhs);
};

struct table_type : util::equality_comparable<table_type>
{
  table_type() = default;
  table_type(intrusive_ptr<type> key, intrusive_ptr<type> yield);

  intrusive_ptr<type> key_type;
  intrusive_ptr<type> yield_type;

  friend bool operator==(table_type const& lhs, table_type const& rhs);
};

struct argument : util::equality_comparable<argument>,
                  util::printable<argument>
{
  argument() = default;
  argument(std::string name, intrusive_ptr<type> type);

  std::string name;
  intrusive_ptr<type> type;

private:
  friend access;

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    if (! render(out, name + ": "))
      return false;

    return render(out, *type);
  }

  friend bool operator==(argument const& lhs, argument const& rhs);
};

struct record_type : util::equality_comparable<record_type>
{
  record_type() = default;
  record_type(std::vector<argument> args);

  std::vector<argument> args;

  friend bool operator==(record_type const& lhs, record_type const& rhs);
};

struct event_info : util::equality_comparable<event_info>,
                    util::printable<event_info>
{
  event_info() = default;
  event_info(std::string name, std::vector<argument> args);

  std::string name;
  std::vector<argument> args;

private:
  friend access;

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    if (! render(out, name + "("))
      return false;

    auto first = args.begin();
    auto last = args.end();
    while (first != last)
    {
      if (! render(out, *first))
        return false;

      if (++first != last)
        if (! render(out, ", "))
          return false;
    }

    return render(out, ")");
  }

  friend bool operator==(event_info const& lhs, event_info const& rhs);
};

using type_info = util::variant<
    invalid_type,
    bool_type,
    int_type,
    uint_type,
    double_type,
    time_range_type,
    time_point_type,
    string_type,
    regex_type,
    address_type,
    prefix_type,
    port_type,
    enum_type,
    record_type,
    vector_type,
    set_type,
    table_type
>;

/// Converts a ::type_info type into a value_type.
/// @tparam The ::type_info type.
template <typename T>
using to_value_type =
  Conditional<
    std::is_same<T, bool_type>,
    std::integral_constant<value_type, bool_value>,
  Conditional<
    std::is_same<T, int_type>,
    std::integral_constant<value_type, int_value>,
  Conditional<
    std::is_same<T, uint_type>,
    std::integral_constant<value_type, uint_value>,
  Conditional<
    std::is_same<T, double_type>,
    std::integral_constant<value_type, double_value>,
  Conditional<
    std::is_same<T, time_range_type>,
    std::integral_constant<value_type, time_range_value>,
  Conditional<
    std::is_same<T, time_point_type>,
    std::integral_constant<value_type, time_point_value>,
  Conditional<
    std::is_same<T, string_type>,
    std::integral_constant<value_type, string_value>,
  Conditional<
    std::is_same<T, regex_type>,
    std::integral_constant<value_type, string_value>,
  Conditional<
    std::is_same<T, address_type>,
    std::integral_constant<value_type, address_value>,
  Conditional<
    std::is_same<T, prefix_type>,
    std::integral_constant<value_type, prefix_value>,
  Conditional<
    std::is_same<T, port_type>,
    std::integral_constant<value_type, port_value>,
  Conditional<
    std::is_same<T, vector_type>,
    std::integral_constant<value_type, vector_value>,
  Conditional<
    std::is_same<T, set_type>,
    std::integral_constant<value_type, set_value>,
  Conditional<
    std::is_same<T, table_type>,
    std::integral_constant<value_type, table_value>,
  Conditional<
    std::is_same<T, record_type>,
    std::integral_constant<value_type, record_value>,
    std::integral_constant<value_type, invalid_value>
  >>>>>>>>>>>>>>>;

/// Checks whether a ::type_info type is a *basic* type.
/// @tparam The ::type_info type.
template <typename T>
using is_basic_type =
  Conditional<std::is_same<T, bool_type>, Bool<true>,
  Conditional<std::is_same<T, int_type>, Bool<true>,
  Conditional<std::is_same<T, uint_type>, Bool<true>,
  Conditional<std::is_same<T, double_type>, Bool<true>,
  Conditional<std::is_same<T, time_range_type>, Bool<true>,
  Conditional<std::is_same<T, time_point_type>, Bool<true>,
  Conditional<std::is_same<T, string_type>, Bool<true>,
  Conditional<std::is_same<T, regex_type>, Bool<true>,
  Conditional<std::is_same<T, address_type>, Bool<true>,
  Conditional<std::is_same<T, prefix_type>, Bool<true>,
  Conditional<std::is_same<T, port_type>, Bool<true>,
  Conditional<std::is_same<T, table_type>, Bool<true>,
    Bool<false>>>>>>>>>>>>>;

/// Checks whether a ::type_info type is a *container* type.
/// @tparam The ::type_info type.
template <typename T>
using is_container_type =
  Conditional<std::is_same<T, vector_type>, Bool<true>,
  Conditional<std::is_same<T, set_type>, Bool<true>,
  Conditional<std::is_same<T, table_type>, Bool<true>, Bool<false>>>>;

/// Checks whether a ::type_info type is a container type.
/// @tparam The ::type_info type.
template <typename T>
using type_type = value_type_type<to_value_type<T>::value>;

/// Represents meta data of a ::value.
class type : util::intrusive_base<type>,
             util::equality_comparable<type>,
             util::printable<type>
{
public:
  /// Factory-function to create a type.
  /// @param args The arguments to pass to the type.
  /// @returns A pointer to the ::type for `T`.
  template <typename T, typename... Args>
  static intrusive_ptr<type> make(Args&&... args)
  {
    return new type{type_info{T{std::forward<Args>(args)...}}};
  }

  /// Names the type.
  /// @param name The new name for this type.
  /// @returns `true` on success and `false` if the type is already an alias.
  bool name(string name);

  /// Creates a new alias under which this type is accessible.
  /// @param name The alias of the type.
  /// @returns `true` on success and `false` if *name* cannot be used.
  bool alias(string name);

  /// Retrieves the ::type_info instance for this type.
  /// @return The ::type_info insance for this type.
  type_info const& info() const;

  /// Retrieves the name of the type.
  /// @returns The name of the type.
  string const& name() const;

  /// Checks whether this type is compatible to another type. Two types are
  /// compatible if they are *representationally equal*.
  /// @param other The type to compare with.
  /// @returns `true` iff `*this` is compatible to *other*.
  bool compatible_to(type const& other) const;

  /// Retrieves the [value type](::value_type) for this type.
  /// @returns The value type corresponding to this type.
  value_type tag() const;

private:
  /// Default-constructs an ::invalid_type.
  type() = default;

  /// Constructs a type from a ::type_info instance.
  /// @param ti The ::type_info instance.
  explicit type(type_info ti);

  string name_;
  std::vector<string> aliases_;
  type_info info_;

private:
  friend access;

  template <typename Iterator>
  struct printer
  {
    using result_type = bool;

    printer(Iterator& out)
      : out_{out}
    {
    }

#define VAST_DEFINE_BASIC_TYPE_OVERLOAD(type, str)  \
    bool operator()(type const&) const              \
    {                                               \
      return render(out_, str);                     \
    }

VAST_DEFINE_BASIC_TYPE_OVERLOAD(invalid_type, "<invalid>")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(bool_type, "bool")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(int_type, "int")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(uint_type, "count")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(double_type, "double")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(time_range_type, "interval")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(time_point_type, "time")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(string_type, "string")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(regex_type, "pattern")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(address_type, "addr")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(prefix_type, "prefix")
VAST_DEFINE_BASIC_TYPE_OVERLOAD(port_type, "port")

#undef VAST_DEFINE_BASIC_TYPE_OVERLOAD

    bool operator()(enum_type const& t) const
    {
      if (! render(out_, "enum {"))
        return false;

      auto first = t.fields.begin();
      auto last = t.fields.end();
      while (first != last)
      {
        if (! render(out_, *first))
          return false;

        if (++first != last)
          if (! render(out_, ", "))
            return false;
      }

      return render(out_, "}");
    }

    bool operator()(vector_type const& t) const
    {
      if (! render(out_, "vector of "))
        return false;

      return render(out_, *t.elem_type);
    }

    bool operator()(set_type const& t) const
    {
      if (! render(out_, "set["))
        return false;

      if (! render(out_, *t.elem_type))
        return false;

      return render(out_, "]");
    }

    bool operator()(table_type const& t) const
    {
      if (! render(out_, "table["))
        return false;

      if (! render(out_, *t.key_type))
        return false;

      if (! render(out_, "] of "))
        return false;

      return render(out_, *t.yield_type);
    }

    bool operator()(record_type const& t) const
    {
      if (! render(out_, "record {"))
        return false;

      auto first = t.args.begin();
      auto last = t.args.end();
      while (first != last)
      {
        if (! render(out_, *first))
          return false;

        if (++first != last)
          if (! render(out_, ", "))
            return false;
      }

      return render(out_, "}");
    }

    Iterator& out_;
  };

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    if (name_.empty())
      return apply_visitor(printer<Iterator>{out}, info_);
    else
      return render(out, name_);
  }

  friend bool operator==(type const& lhs, type const& rhs);
};

} // namespace vast

#endif
