#ifndef VAST_TYPE_H
#define VAST_TYPE_H

#include <string>
#include <vector>
#include <map>
#include <memory>
#include "vast/aliases.h"
#include "vast/config.h"
#include "vast/key.h"
#include "vast/none.h"
#include "vast/offset.h"
#include "vast/operator.h"
#include "vast/print.h"
#include "vast/util/intrusive.h"
#include "vast/util/operators.h"
#include "vast/util/range.h"
#include "vast/util/variant.h"
#include "vast/util/hash/xxhash.h"
#include "vast/util/stack/vector.h"

namespace vast {

struct key;
struct offset;

class address;
class subnet;
class pattern;
class vector;
class set;
class table;
class record;
class data;

namespace detail {
struct type_reader;
struct type_writer;
} // namespace detail

/// A type for a data. The type *signature* consists of (i) type name and (ii)
/// all subtypes. Two types are equal if they have the same signature. Each
/// type has a unique hash digest over which defines a total ordering, albeit
/// not consistent with lexicographical string representation.
class type : util::totally_ordered<type>
{
  friend access;

public:
  struct intrusive_info;

  /// A type attrbiute.
  struct attribute : util::equality_comparable<attribute>
  {
    enum key_type : uint16_t
    {
      invalid,
      skip,
      default_
    };

    attribute(key_type k = invalid, std::string v = {})
      : key{k},
        value{std::move(v)}
    {
    }

    key_type key;
    std::string value;

    friend bool operator==(attribute const& lhs, attribute const& rhs)
    {
      return lhs.key == rhs.key && lhs.value == rhs.value;
    }

    template <typename Iterator>
    friend trial<void> print(attribute const& a, Iterator&& out)
    {
      *out++ = '&';
      switch (a.key)
      {
        default:
          return print("invalid", out);
        case skip:
          return print("skip", out);
        case default_:
          {
            auto t = print("default=\"", out);
            if (! t)
              return t;

            t = print(a.value, out);
            if (! t)
              return t;

            return print('"', out);
          }
      }
    }
  };

  using hash_type = util::xxhash;

  /// Derives a type from data.
  /// @param d The data to derive a type from.
  /// @returns The type corresponding to *d*.
  static type derive(data const& d);

  // The base class for type classes.
  template <typename Derived>
  class base : util::totally_ordered<base<Derived>>
  {
    friend access;

  public:
    friend bool operator==(base const& lhs, base const& rhs)
    {
      return lhs.digest() == rhs.digest();
    }

    friend bool operator<(base const& lhs, base const& rhs)
    {
      return lhs.digest() < rhs.digest();
    }

    std::string const& name() const
    {
      return name_;
    }

    bool name(std::string name)
    {
      if (! name_.empty())
        return false;
      name_ = std::move(name);
      return hash_.add(name_.data(), name_.size());
    }

    std::vector<attribute> const& attributes() const
    {
      return attributes_;
    }

    attribute const* find_attribute(attribute::key_type k) const
    {
      auto i = std::find_if(attributes_.begin(),
                            attributes_.end(),
                            [&](attribute const& a) { return a.key == k; });

      return i == attributes_.end() ? nullptr : &*i;
    }

    hash_type::digest_type digest() const
    {
      // FIXME: put the digest somewhere and remove the const_cast.
      return const_cast<hash_type&>(hash_).get();
    }

  protected:
    base(std::vector<attribute> a = {})
      : attributes_(std::move(a))
    {
      for (auto& a : attributes_)
      {
        update(a.key);
        update(a.value.data(), a.value.size());
      }
    }

    template <typename... Ts>
    void update(Ts&&... xs)
    {
      hash_.add(std::forward<Ts>(xs)...);
    }

  private:
    std::string name_;
    std::vector<attribute> attributes_;
    hash_type hash_;
  };

  class enumeration;
  class vector;
  class set;
  class table;
  class record;
  class alias;

#define VAST_DEFINE_BASIC_TYPE(name, desc)                    \
  class name : public base<name>                              \
  {                                                           \
  public:                                                     \
    name(std::vector<attribute> a = {})                       \
      : base<name>(std::move(a))                              \
    {                                                         \
      update(#name, sizeof(#name) - 1);                       \
    }                                                         \
                                                              \
  private:                                                    \
    template <typename Iterator>                              \
    friend trial<void> print(name const& n, Iterator&& out)   \
    {                                                         \
      auto t = print(desc, out);                              \
      if (! t)                                                \
        return t;                                             \
      if (! n.attributes().empty())                           \
      {                                                       \
        *out++ = ' ';                                         \
        return print(n.attributes(), out);                    \
      }                                                       \
      return nothing;                                         \
    }                                                         \
  };

  VAST_DEFINE_BASIC_TYPE(boolean, "bool")
  VAST_DEFINE_BASIC_TYPE(integer, "int")
  VAST_DEFINE_BASIC_TYPE(count, "count")
  VAST_DEFINE_BASIC_TYPE(real, "real")
  VAST_DEFINE_BASIC_TYPE(time_point, "time")
  VAST_DEFINE_BASIC_TYPE(time_interval, "interval")
  VAST_DEFINE_BASIC_TYPE(time_duration, "duration")
  VAST_DEFINE_BASIC_TYPE(time_period, "period")
  VAST_DEFINE_BASIC_TYPE(string, "string")
  VAST_DEFINE_BASIC_TYPE(pattern, "pattern")
  VAST_DEFINE_BASIC_TYPE(address, "addr")
  VAST_DEFINE_BASIC_TYPE(subnet, "subnet")
  VAST_DEFINE_BASIC_TYPE(port, "port")
#undef VAST_DEFINE_BASIC_TYPE

  /// Maps a type to its corresponding data.
  template <typename T>
  using to_data = std::conditional_t<
      std::is_same<T, boolean>::value,
      vast::boolean,
      std::conditional_t<
        std::is_same<T, integer>::value,
        vast::integer,
        std::conditional_t<
          std::is_same<T, count>::value,
          vast::count,
          std::conditional_t<
            std::is_same<T, real>::value,
            vast::real,
            std::conditional_t<
              std::is_same<T, time_point>::value,
              vast::time::point,
              std::conditional_t<
                std::is_same<T, time_interval>::value,
                std::false_type,
                std::conditional_t<
                  std::is_same<T, time_duration>::value,
                  vast::time::duration,
                  std::conditional_t<
                    std::is_same<T, time_period>::value,
                    std::false_type,
                    std::conditional_t<
                      std::is_same<T, string>::value,
                      std::string,
                      std::conditional_t<
                        std::is_same<T, pattern>::value,
                        vast::pattern,
                        std::conditional_t<
                          std::is_same<T, address>::value,
                          vast::address,
                          std::conditional_t<
                            std::is_same<T, subnet>::value,
                            vast::subnet,
                            std::conditional_t<
                              std::is_same<T, port>::value,
                              vast::port,
                              std::conditional_t<
                                std::is_same<T, enumeration>::value,
                                vast::enumeration,
                                std::conditional_t<
                                  std::is_same<T, vector>::value,
                                  vast::vector,
                                  std::conditional_t<
                                    std::is_same<T, set>::value,
                                    vast::set,
                                    std::conditional_t<
                                      std::is_same<T, table>::value,
                                      vast::table,
                                      std::conditional_t<
                                        std::is_same<T, record>::value,
                                        vast::record,
                                        std::false_type
                                      >
                                    >
                                  >
                                >
                              >
                            >
                          >
                        >
                      >
                    >
                  >
                >
              >
            >
          >
        >
      >
    >;

  /// Maps a value to its corresponding type.
  template <typename T>
  using from_data = std::conditional_t<
      std::is_same<T, vast::boolean>::value,
      boolean,
      std::conditional_t<
        std::is_same<T, vast::integer>::value,
        integer,
        std::conditional_t<
          std::is_same<T, vast::count>::value,
          count,
          std::conditional_t<
            std::is_same<T, vast::real>::value,
            real,
            std::conditional_t<
              std::is_same<T, vast::time::point>::value,
              time_point,
              std::conditional_t<
                std::is_same<T, vast::time::duration>::value,
                time_duration,
                std::conditional_t<
                  std::is_same<T, std::string>::value,
                  string,
                  std::conditional_t<
                    std::is_same<T, vast::pattern>::value,
                    pattern,
                    std::conditional_t<
                      std::is_same<T, vast::address>::value,
                      address,
                      std::conditional_t<
                        std::is_same<T, vast::subnet>::value,
                        subnet,
                        std::conditional_t<
                          std::is_same<T, vast::port>::value,
                          port,
                          std::conditional_t<
                            std::is_same<T, vast::enumeration>::value,
                            enumeration,
                            std::conditional_t<
                              std::is_same<T, vast::vector>::value,
                              vector,
                              std::conditional_t<
                                std::is_same<T, vast::set>::value,
                                set,
                                std::conditional_t<
                                  std::is_same<T, vast::table>::value,
                                  table,
                                  std::conditional_t<
                                    std::is_same<T, vast::record>::value,
                                    record,
                                    std::false_type
                                  >
                                >
                              >
                            >
                          >
                        >
                      >
                    >
                  >
                >
              >
            >
          >
        >
      >
    >;

  enum class tag : uint8_t
  {
    none,
    boolean,
    integer,
    count,
    real,
    time_point,
    //time_interval,
    time_duration,
    //time_period,
    string,
    pattern,
    address,
    subnet,
    port,
    enumeration,
    vector,
    set,
    table,
    record,
    alias
  };

  using info = util::basic_variant<
    tag,
    none,
    boolean,
    integer,
    count,
    real,
    time_point,
    //time_interval,
    time_duration,
    //time_period,
    string,
    pattern,
    address,
    subnet,
    port,
    enumeration,
    vector,
    set,
    table,
    record,
    alias
  >;

  /// An enum type.
  class enumeration : public base<enumeration>
  {
    friend access;
    friend info;
    friend detail::type_reader;
    friend detail::type_writer;

  public:
    enumeration(std::vector<std::string> fields, std::vector<attribute> a = {})
      : base<enumeration>{std::move(a)},
        fields_{std::move(fields)}
    {
      static constexpr auto desc = "enumeration";
      update(desc, sizeof(desc));

      for (auto& f : fields_)
        update(f.data(), f.size());
    }

    std::vector<std::string> const& fields() const
    {
      return fields_;
    }

  private:
    enumeration() = default;

    std::vector<std::string> fields_;

    template <typename Iterator>
    friend trial<void> print(enumeration const& e, Iterator&& out)
    {
      auto t = print("enum {", out);
      if (! t)
        return t.error();

      t = util::print_delimited(", ", e.fields_.begin(), e.fields_.end(), out);
      if (! t)
        return t.error();

      t = print('}', out);
      if (! t)
        return t.error();

      if (! e.attributes().empty())
      {
        *out++ = ' ';
        return print(e.attributes(), out);
      }

      return nothing;
    }
  };

  /// Default-constructs an invalid type.
  type();

  /// Construct a given type.
  /// @tparam T the type to construct.
  /// @param x An instance of `T`.
  template <
    typename T,
    typename = std::enable_if_t<
      ! util::is_same_or_derived<type, T>::value
#ifdef VAST_GCC
      && (std::is_convertible<T, none>::value
          || std::is_convertible<T, boolean>::value
          || std::is_convertible<T, integer>::value
          || std::is_convertible<T, count>::value
          || std::is_convertible<T, real>::value
          || std::is_convertible<T, time_point>::value
          || std::is_convertible<T, time_duration>::value
          || std::is_convertible<T, string>::value
          || std::is_convertible<T, pattern>::value
          || std::is_convertible<T, address>::value
          || std::is_convertible<T, subnet>::value
          || std::is_convertible<T, port>::value
          || std::is_convertible<T, enumeration>::value
          || std::is_convertible<T, vector>::value
          || std::is_convertible<T, set>::value
          || std::is_convertible<T, table>::value
          || std::is_convertible<T, record>::value
          || std::is_convertible<T, alias>::value)
#endif
    >
  >
  type(T&& x)
    : info_{util::make_intrusive<intrusive_info>(std::forward<T>(x))}
  {
  }

  explicit type(util::intrusive_ptr<intrusive_info> ii)
    : info_{std::move(ii)}
  {
  }

  friend bool operator==(type const& lhs, type const& rhs);
  friend bool operator<(type const& lhs, type const& rhs);

  /// Assigns a name to the type. This can happen at most once because a name
  /// change modifies the type hash digest.
  /// @param name The new name of the type.
  /// @returns `true` on success.
  bool name(std::string name);

  /// Retrieves the name of the type.
  /// @returns The name of the type.
  std::string const& name() const;

  /// Retrieves the hash digest of this type.
  /// @returns The hash digest of this type.
  hash_type::digest_type digest() const;

  /// Retrieves the type's attributes.
  std::vector<attribute> const& attributes() const;

  /// Looks for a specific attribute.
  /// @param key The attribute key.
  /// @returns A pointer to the attribute if it exists or `nullptr` otherwise.
  attribute const* find_attribute(attribute::key_type key) const;

  /// Checks whether data complies with this type.
  /// @param d The data to check.
  /// @returns `true` if data complies to `*this`.
  bool check(data const& d) const;

  /// Default-constructs data for this given type.
  /// @returns ::data according to this type.
  data make() const;

  //
  // Introspection
  //

  template <typename T>
  using is_basic = std::integral_constant<
      bool,
      std::is_same<T, boolean>::value
        || std::is_same<T, integer>::value
        || std::is_same<T, count>::value
        || std::is_same<T, real>::value
        || std::is_same<T, time_point>::value
        || std::is_same<T, time_interval>::value
        || std::is_same<T, time_duration>::value
        || std::is_same<T, time_period>::value
        || std::is_same<T, string>::value
        || std::is_same<T, pattern>::value
        || std::is_same<T, address>::value
        || std::is_same<T, subnet>::value
        || std::is_same<T, port>::value
    >;

  template <typename T>
  using is_container = std::integral_constant<
      bool,
      std::is_same<T, vector>::value
        || std::is_same<T, set>::value
        || std::is_same<T, table>::value
    >;

  /// Checks whether the type is a basic type.
  /// @returns `true` iff the type is a basic type.
  bool basic() const;

  /// Checks whether the type is a container type.
  /// @returns `true` iff the type is a container type.
  bool container() const;

  /// Checks whether the type is a recursive type.
  /// @returns `true` iff the type is a recursive type.
  bool recursive() const;

private:
  friend info& expose(type& t);
  friend info const& expose(type const& t);

  template <typename Iterator>
  friend trial<void> print(tag t, Iterator&& out)
  {
    switch (t)
    {
      default:
        return print("{invalid}", out);
      case tag::none:
        return print("{none}", out);
      case tag::boolean:
        return print("{bool}", out);
      case tag::integer:
        return print("{int}", out);
      case tag::count:
        return print("{uint}", out);
      case tag::real:
        return print("{real}", out);
      case tag::time_point:
        return print("{time}", out);
      case tag::time_duration:
        return print("{duration}", out);
      case tag::string:
        return print("{string}", out);
      case tag::pattern:
        return print("{pattern}", out);
      case tag::address:
        return print("{address}", out);
      case tag::subnet:
        return print("{subnet}", out);
      case tag::port:
        return print("{port}", out);
      case tag::enumeration:
        return print("{enum}", out);
      case tag::vector:
        return print("{vector}", out);
      case tag::set:
        return print("{set}", out);
      case tag::table:
        return print("{table}", out);
      case tag::record:
        return print("{record}", out);
      case tag::alias:
        return print("{alias}", out);
    }
  }

  template <typename Iterator>
  friend trial<void> print(type const& t, Iterator&& out, bool resolve = true)
  {
    if (t.name().empty() || ! resolve)
      return visit([&out](auto&& x) { return print(x, out); },  t);
    else
      return print(t.name(), out);
  }

  util::intrusive_ptr<intrusive_info> info_;
};

/// Checks whether two types are *congruent* to each other, i.e., whether they
/// are *representationally equal*.
/// @param x The first type.
/// @param y The second type.
/// @returns `true` *iff* *x* and *y* are congruent.
bool congruent(type const& x, type const& y);

/// Checks whether the types of two nodes in a predicate are compatible with
/// each other, i.e., whether operator evaluation for the given types is
/// semantically correct.
/// @note This function assumes the AST has already been normalized with the
///       extractor occurring at the LHS and the value at the RHS.
/// @param lhs The LHS of *op*.
/// @param op The operator under which to compare *lhs* and *rhs*.
/// @param rhs The RHS of *op*.
/// @returns `true` if *lhs* and *rhs* are compatible to each other under *op*.
bool compatible(type const& lhs, relational_operator op, type const& rhs);

class type::vector : public type::base<type::vector>
{
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  vector(type t, std::vector<attribute> a = {})
    : base<vector>{std::move(a)},
      elem_{std::move(t)}
  {
    static constexpr auto desc = "vector";
    update(desc, sizeof(desc));
    update(elem_.digest());
  }

  type const& elem() const
  {
    return elem_;
  }

private:
  vector() = default;

  type elem_;

  template <typename Iterator>
  friend trial<void> print(vector const& v, Iterator&& out)
  {
    auto t = print("vector<", out);
    if (! t)
      return t.error();

    t = print(v.elem_, out);
    if (! t)
      return t.error();

    *out++ = '>';

    if (! v.attributes().empty())
    {
      *out++ = ' ';
      return print(v.attributes(), out);
    }

    return nothing;
  }
};

class type::set : public base<type::set>
{
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  set(type t, std::vector<attribute> a = {})
    : base<set>{std::move(a)},
      elem_{std::move(t)}
  {
    static constexpr auto desc = "set";
    update(desc, sizeof(desc));
    update(elem_.digest());
  }

  type const& elem() const
  {
    return elem_;
  }

private:
  set() = default;

  type elem_;

  template <typename Iterator>
  friend trial<void> print(set const& s, Iterator&& out)
  {
    auto t = print("set<", out);
    if (! t)
      return t.error();

    t = print(s.elem(), out);
    if (! t)
      return t.error();

    *out++ = '>';

    if (! s.attributes().empty())
    {
      *out++ = ' ';
      return print(s.attributes(), out);
    }

    return nothing;
  }
};

class type::table : public type::base<type::table>
{
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  table(type k, type v, std::vector<attribute> a = {})
    : base<table>{std::move(a)},
      key_{std::move(k)},
      value_{std::move(v)}
  {
    static constexpr auto desc = "table";
    update(desc, sizeof(desc));
    update(key_.digest());
    update(value_.digest());
  }

  type const& key() const
  {
    return key_;
  }

  type const& value() const
  {
    return value_;
  }

private:
  table() = default;

  type key_;
  type value_;

  template <typename Iterator>
  friend trial<void> print(table const& tab, Iterator&& out)
  {
    auto t = print("table<", out);
    if (! t)
      return t;

    t = print(tab.key_, out);
    if (! t)
      return t;

    t = print(", ", out);
    if (! t)
      return t;

    t = print(tab.value_, out);
    if (! t)
      return t;

    *out++ = '>';

    if (! tab.attributes().empty())
    {
      *out++ = ' ';
      return print(tab.attributes(), out);
    }

    return nothing;
  }
};

class type::record : public type::base<type::record>
{
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  struct field : util::equality_comparable<field>
  {
    field() = default;

    field(std::string n, type t)
      : name{std::move(n)},
        type{std::move(t)}
    {
    }

    friend bool operator==(field const& lhs, field const& rhs)
    {
      return lhs.name == rhs.name && lhs.type == rhs.type;
    }

    std::string name;
    vast::type type;
  };

  /// Enables recursive record iteration.
  class each : public util::range_facade<each>
  {
  public:
    struct range_state
    {
      vast::key key() const;
      size_t depth() const;

      util::stack::vector<8, field const*> trace;
      vast::offset offset;
    };

    each(record const& r);

  private:
    friend util::range_facade<each>;

    range_state const& state() const
    {
      return state_;
    }

    bool next();

    range_state state_;
    util::stack::vector<8, record const*> records_;
  };

  record(std::initializer_list<field> fields, std::vector<attribute> a = {})
    : base<record>{std::move(a)},
      fields_(fields.begin(), fields.end())
  {
    initialize();
  }

  record(std::vector<field> fields, std::vector<attribute> a = {})
    : base<record>{std::move(a)},
      fields_(std::move(fields))
  {
    initialize();
  }

  /// Retrieves the fields of the record.
  /// @returns The field of the records.
  std::vector<field> const& fields() const
  {
    return fields_;
  }

  /// Attemps to resolve a ::key to an ::offset.
  /// @param k The key to resolve.
  /// @returns The ::offset corresponding to *k*.
  trial<offset> resolve(key const& k) const;

  /// Attemps to resolve an ::offset to a ::key.
  /// @param o The offset to resolve.
  /// @returns The ::key corresponding to *o*.
  trial<key> resolve(offset const& o) const;

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
  record flatten() const;

  /// Undos a flattening operation.
  /// @returns The unflattened record type.
  record unflatten() const;

  /// Retrieves the type at a given key.
  /// @param k The key to resolve.
  /// @returns The type at key *k* or `nullptr` if *k* doesn't resolve.
  type const* at(key const& k) const;

  /// Retrieves the type at a given offset.
  /// @param o The offset to resolve.
  /// @returns The type at offset *o* or `nullptr` if *o* doesn't resolve.
  type const* at(offset const& o) const;

private:
  record() = default;

  void initialize();

  std::vector<field> fields_;

  template <typename Iterator>
  friend trial<void> print(field const& f, Iterator&& out)
  {
    auto t = print(f.name + ": ", out);
    if (! t)
      return t.error();

    return print(f.type, out);
  }

  template <typename Iterator>
  friend trial<void> print(record const& r, Iterator&& out)
  {
    auto t = print("record {", out);
    if (! t)
      return t.error();

    t = util::print_delimited(", ", r.fields_.begin(), r.fields_.end(), out);
    if (! t)
      return t.error();

    *out++ = '}';

    if (! r.attributes().empty())
    {
      *out++ = ' ';
      return print(r.attributes(), out);
    }

    return nothing;
  }
};

class type::alias : public type::base<type::alias>
{
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  alias(vast::type t, std::vector<attribute> a = {})
    : base<alias>{std::move(a)},
      type_{std::move(t)}
  {
    static constexpr auto desc = "alias";
    update(desc, sizeof(desc));
    update(type_.digest());
  }

  vast::type const& type() const
  {
    return type_;
  }

private:
  alias() = default;

  vast::type type_;

  template <typename Iterator>
  friend trial<void> print(alias const& a, Iterator&& out)
  {
    auto t = print(a.type(), out);
    if (! t)
      return t;

    if (! a.attributes().empty())
    {
      *out++ = ' ';
      return print(a.attributes(), out);
    }

    return nothing;
  }
};

struct type::intrusive_info : util::intrusive_base<intrusive_info>, type::info
{
  intrusive_info() = default;

  template <
    typename T,
    typename = util::disable_if_same_or_derived_t<intrusive_info, T>
  >
  intrusive_info(T&& x)
    : type::info{std::forward<T>(x)}
  {
  }

  friend type::info& expose(intrusive_info& i)
  {
    return static_cast<type::info&>(i);
  }

  friend type::info const& expose(intrusive_info const& i)
  {
    return static_cast<type::info const&>(i);
  }
};

template <typename Iterator>
trial<void> print(std::vector<type::attribute> const& attrs, Iterator&& out)
{
  return util::print_delimited(" ", attrs.begin(), attrs.end(), out);
}

} // namespace vast

namespace std {

template <>
struct hash<vast::type>
{
  size_t operator()(vast::type const& t) const
  {
    return t.digest();
  }
};

} // namespace std

#endif
