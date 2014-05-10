#ifndef VAST_TYPE_H
#define VAST_TYPE_H

#include "vast/aliases.h"
#include "vast/key.h"
#include "vast/offset.h"
#include "vast/string.h"
#include "vast/type_tag.h"
#include "vast/util/operators.h"
#include "vast/util/variant.h"
#include "vast/util/alloc.h"

namespace vast {

class type;

extern type_const_ptr const type_invalid;

#define VAST_DEFINE_BASIC_TYPE(name, desc)                \
  struct name : util::totally_ordered<name>               \
  {                                                       \
  private:                                                \
    friend access;                                        \
                                                          \
    void serialize(serializer&) const                     \
    {                                                     \
    }                                                     \
                                                          \
    void deserialize(deserializer&)                       \
    {                                                     \
    }                                                     \
                                                          \
    template <typename Iterator>                          \
    friend trial<void> print(name const&, Iterator&& out) \
    {                                                     \
      return print(desc, out);                            \
    }                                                     \
                                                          \
    friend bool operator==(name const&, name const&)      \
    {                                                     \
      return true;                                        \
    }                                                     \
                                                          \
    friend bool operator<(name const&, name const&)       \
    {                                                     \
      return true;                                        \
    }                                                     \
  };

VAST_DEFINE_BASIC_TYPE(invalid_type, "<invalid>")
VAST_DEFINE_BASIC_TYPE(bool_type, "bool")
VAST_DEFINE_BASIC_TYPE(int_type, "int")
VAST_DEFINE_BASIC_TYPE(uint_type, "count")
VAST_DEFINE_BASIC_TYPE(double_type, "double")
VAST_DEFINE_BASIC_TYPE(time_range_type, "interval")
VAST_DEFINE_BASIC_TYPE(time_point_type, "time")
VAST_DEFINE_BASIC_TYPE(string_type, "string")
VAST_DEFINE_BASIC_TYPE(regex_type, "pattern")
VAST_DEFINE_BASIC_TYPE(address_type, "addr")
VAST_DEFINE_BASIC_TYPE(prefix_type, "subnet")
VAST_DEFINE_BASIC_TYPE(port_type, "port")

#undef VAST_DEFINE_BASIC_TYPE

struct enum_type : util::totally_ordered<enum_type>
{
  enum_type() = default;
  enum_type(std::vector<string> fields);

  std::vector<string> fields;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(enum_type const& e, Iterator&& out)
  {
    auto t = print("enum {", out);
    if (! t)
      return t.error();

    t = util::print_delimited(", ", e.fields.begin(), e.fields.end(), out);
    if (! t)
      return t.error();

    return print('}', out);
  }

  friend bool operator==(enum_type const& lhs, enum_type const& rhs);
  friend bool operator<(enum_type const& lhs, enum_type const& rhs);
};

struct vector_type : util::totally_ordered<vector_type>
{
  vector_type() = default;
  vector_type(type_const_ptr elem);

  type_const_ptr elem_type;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(vector_type const& v, Iterator&& out)
  {
    auto t = print("vector of ", out);
    if (! t)
      return t.error();

    return print(*v.elem_type, out);
  }

  friend bool operator==(vector_type const& lhs, vector_type const& rhs);
  friend bool operator<(vector_type const& lhs, vector_type const& rhs);
};

struct set_type : util::totally_ordered<set_type>
{
  set_type() = default;
  set_type(type_const_ptr elem);

  type_const_ptr elem_type;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(set_type const& s, Iterator&& out)
  {
    auto t = print("set[", out);
    if (! t)
      return t.error();

    t = print(*s.elem_type, out);
    if (! t)
      return t.error();

    return print(']', out);
  }

  friend bool operator==(set_type const& lhs, set_type const& rhs);
  friend bool operator<(set_type const& lhs, set_type const& rhs);
};

struct table_type : util::totally_ordered<table_type>
{
  table_type() = default;
  table_type(type_const_ptr key, type_const_ptr yield);

  type_const_ptr key_type;
  type_const_ptr yield_type;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(table_type const& tt, Iterator&& out)
  {
    auto t = print("table[", out);
    if (! t)
      return t.error();

    t = print(*tt.key_type, out);
    if (! t)
      return t.error();

    t = print("] of ", out);
    if (! t)
      return t.error();

    return print(*tt.yield_type, out);
  }

  friend bool operator==(table_type const& lhs, table_type const& rhs);
  friend bool operator<(table_type const& lhs, table_type const& rhs);
};

struct argument : util::totally_ordered<argument>
{
  argument() = default;
  argument(string name, type_const_ptr type);

  string name;
  type_const_ptr type;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(argument const& a, Iterator&& out)
  {
    auto t = print(a.name + ": ", out);
    if (! t)
      return t.error();

    return print(*a.type, out);
  }

  friend bool operator==(argument const& lhs, argument const& rhs);
  friend bool operator<(argument const& lhs, argument const& rhs);
};

using trace = std::vector<argument const*,
                          util::stack_alloc<argument const*, 32>>;

struct record_type : util::totally_ordered<record_type>
{
  record_type() = default;
  record_type(std::vector<argument> args);

  /// Attemps to resolve a single ::key to an ::offset.
  /// @param k The key to resolve.
  /// @returns The ::offset corresponding to *k*.
  trial<offset> resolve(key const& k) const;

  /// Finds all offset-key pairs for an *exact* key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The offset-key pairs corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find(key const& k) const;

  /// Finds all offset-key pairs for a *prefix* key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The offset-key pairs corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find_prefix(key const& k) const;

  /// Finds all offset-key pairs for a *suffix* key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The offset-key pairs corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find_suffix(key const& k) const;

  /// Recursively flattens the arguments of a record type.
  /// @returns The flattened record type.
  record_type flatten() const;

  /// Undos a flattening operation.
  /// @returns The unflattened record type.
  record_type unflatten() const;

  /// Retrieves the type at a given key.
  /// @param k The key to resolve.
  /// @returns The type at key *k* or `nullptr` if *k* doesn't resolve.
  type_const_ptr at(key const& k) const;

  /// Retrieves the type at a given offset.
  /// @param o The offset to resolve.
  /// @returns The type at offset *o* or `nullptr` if *o* doesn't resolve.
  type_const_ptr at(offset const& o) const;

  /// Recursively applies a function on each contained argument.
  /// @param f The function to invoke on each contained argument.
  trial<void> each(std::function<trial<void>(trace const&)> f) const;

  std::vector<argument> args;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(record_type const& r, Iterator&& out)
  {
    auto t = print("record {", out);
    if (! t)
      return t.error();

    t = util::print_delimited(", ", r.args.begin(), r.args.end(), out);
    if (! t)
      return t.error();

    return print('}', out);
  }

  friend bool operator==(record_type const& lhs, record_type const& rhs);
  friend bool operator<(record_type const& lhs, record_type const& rhs);
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
    vector_type,
    set_type,
    table_type,
    record_type
>;

/// Converts a ::type_info type into a type_tag.
/// @tparam The ::type_info type.
template <typename T>
using to_type_tag =
  Conditional<
    std::is_same<T, bool_type>,
    std::integral_constant<type_tag, bool_value>,
  Conditional<
    std::is_same<T, int_type>,
    std::integral_constant<type_tag, int_value>,
  Conditional<
    std::is_same<T, uint_type>,
    std::integral_constant<type_tag, uint_value>,
  Conditional<
    std::is_same<T, double_type>,
    std::integral_constant<type_tag, double_value>,
  Conditional<
    std::is_same<T, time_range_type>,
    std::integral_constant<type_tag, time_range_value>,
  Conditional<
    std::is_same<T, time_point_type>,
    std::integral_constant<type_tag, time_point_value>,
  Conditional<
    std::is_same<T, string_type>,
    std::integral_constant<type_tag, string_value>,
  Conditional<
    std::is_same<T, regex_type>,
    std::integral_constant<type_tag, string_value>,
  Conditional<
    std::is_same<T, address_type>,
    std::integral_constant<type_tag, address_value>,
  Conditional<
    std::is_same<T, prefix_type>,
    std::integral_constant<type_tag, prefix_value>,
  Conditional<
    std::is_same<T, port_type>,
    std::integral_constant<type_tag, port_value>,
  Conditional<
    std::is_same<T, vector_type>,
    std::integral_constant<type_tag, vector_value>,
  Conditional<
    std::is_same<T, set_type>,
    std::integral_constant<type_tag, set_value>,
  Conditional<
    std::is_same<T, table_type>,
    std::integral_constant<type_tag, table_value>,
  Conditional<
    std::is_same<T, record_type>,
    std::integral_constant<type_tag, record_value>,
    std::integral_constant<type_tag, invalid_value>
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

template <typename T>
using type_type = type_tag_type<to_type_tag<T>::value>;

/// Represents meta data of a ::value.
class type : public std::enable_shared_from_this<type>,
             util::totally_ordered<type>
{
  /// Constructs a type from a ::type_info instance.
  /// @param ti The ::type_info instance.
  explicit type(type_info ti);

public:
  // FIXME: The serialization framework requires a regular type, so we have to
  // provide a public default constructor. We should refactor this class to
  // have value semantics and instead keep the shared type data internally, as
  // an implemenation detail.
  type() = default;

  /// Factory-function to create a type.
  /// @param args The arguments to pass to the type.
  /// @returns A pointer to the ::type for `T`.
  template <typename T, typename... Args>
  static type_ptr make(string name = "", Args&&... args = {})
  {
    auto t = new type{type_info{T{std::forward<Args>(args)...}}};
    if (! name.empty())
      t->name_ = std::move(name);

    return type_ptr{t};
  }

  /// Clones this type.
  /// @param name The new name for this type.
  /// @returns A clone of this type.
  type_ptr clone(string name) const;

  /// Retrieves the ::type_info instance for this type.
  /// @return The ::type_info insance for this type.
  type_info const& info() const;

  /// Retrieves the name of the type.
  /// @returns The name of the type.
  string const& name() const;

  /// Retrieves the type at a given key.
  /// @param k The key to resolve.
  /// @returns The type at key *k* or `nullptr` if *k* doesn't resolve.
  type_const_ptr at(key const& k) const;

  /// Retrieves the type at a given offset.
  /// @param o The offset to resolve.
  /// @returns The type at offset *o* or `nullptr` if *o* doesn't resolve.
  type_const_ptr at(offset const& o) const;

  /// Recursively applies a function on each contained type.
  /// @param f The function to invoke on each contained type.
  void each(std::function<void(key const&, offset const&)> f) const;

  /// Casts a key into an offset.
  /// @param k The key to cast.
  /// @returns The offset corresponding to *k*.
  trial<offset> cast(key const& k) const;

  /// Traces an exact key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The trace corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find(key const& k) const;

  /// Traces a prefix key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The trace corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find_prefix(key const& k) const;

  /// Traces a suffix key in this and nested records.
  /// @param k The key to resolve.
  /// @returns The trace corresponding to the found *k*.
  std::vector<std::pair<offset, key>> find_suffix(key const& k) const;

  /// Checks whether this type is compatible to another type. Two types are
  /// compatible if they are *representationally equal*.
  /// @param other The type to compare with.
  /// @returns `true` iff `*this` is compatible to *other*.
  bool represents(type_const_ptr const& other) const;

  /// Retrieves the [type tag](::type_tag) for this type.
  /// @returns The tag corresponding to this type.
  type_tag tag() const;

private:
  string name_;
  type_info info_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  struct printer
  {
    using result_type = trial<void>;

    printer(Iterator& out)
      : out_{out}
    {
    }

    template <typename T>
    trial<void> operator()(T const& x) const
    {
      return print(x, out_);
    }

    Iterator& out_;
  };

  template <typename Iterator>
  friend trial<void> print(type const& t, Iterator&& out, bool resolve = true)
  {
    if (t.name_.empty() || ! resolve)
      return apply_visitor(printer<Iterator>{out}, t.info_);
    else
      return print(t.name_, out);
  }

  friend bool operator==(type const& lhs, type const& rhs);
  friend bool operator<(type const& lhs, type const& rhs);
};

} // namespace vast

#endif
