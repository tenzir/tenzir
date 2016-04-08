#ifndef VAST_TYPE_HPP
#define VAST_TYPE_HPP

#include <string>
#include <vector>
#include <map>
#include <memory>

#include "vast/aliases.hpp"
#include "vast/config.hpp"
#include "vast/key.hpp"
#include "vast/none.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"
#include "vast/time.hpp"
#include "vast/trial.hpp"
#include "vast/util/intrusive.hpp"
#include "vast/util/operators.hpp"
#include "vast/util/range.hpp"
#include "vast/util/variant.hpp"
#include "vast/util/hash/xxhash.hpp"
#include "vast/util/stack/vector.hpp"

namespace vast {

class address;
class subnet;
class port;
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
class type : util::totally_ordered<type> {
  friend access;

public:
  struct intrusive_info;

  /// Additional type property in the form of a key and optional value.
  struct attribute : util::equality_comparable<attribute> {
    enum key_type : uint16_t {
      invalid,
      skip,
      default_
    };

    attribute(key_type k = invalid, std::string v = {});

    friend bool operator==(attribute const& lhs, attribute const& rhs);

    key_type key;
    std::string value;
  };

  using hash_type = util::xxhash64;

  // The base class for type classes.
  class base : util::totally_ordered<base> {
    friend access;

  public:
    friend bool operator==(base const& lhs, base const& rhs);

    friend bool operator<(base const& lhs, base const& rhs);

    std::string const& name() const;

    bool name(std::string name);

    std::vector<attribute> const& attributes() const;

    attribute const* find_attribute(attribute::key_type k) const;

    hash_type::digest_type digest() const;

  protected:
    base(std::vector<attribute> a = {});

    template <typename... Ts>
    void update_digest(void const* bytes, size_t length) {
      digest_ = hash_type::digest_bytes(bytes, length, digest_);
    }

  private:
    std::string name_;
    std::vector<attribute> attributes_;
    hash_type::digest_type digest_ = 0;
  };

  class enumeration;
  class vector;
  class set;
  class table;
  class record;
  class alias;

#define VAST_DEFINE_BASIC_TYPE(name, desc)                                     \
  class name : public base {                                                   \
  public:                                                                      \
    name(std::vector<attribute> a = {})                                        \
      : base(std::move(a)) {                                                   \
      update_digest(#name, sizeof(#name) - 1);                                 \
    }                                                                          \
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

  enum class tag : uint8_t {
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
  class enumeration : public base {
    friend access;
    friend info;
    friend detail::type_reader;
    friend detail::type_writer;

  public:
    enumeration(std::vector<std::string> fields, std::vector<attribute> a = {});

    std::vector<std::string> const& fields() const;

  private:
    enumeration() = default;

    std::vector<std::string> fields_;
  };

  /// Derives a type from data.
  /// @param d The data to derive a type from.
  /// @returns The type corresponding to *d*.
  static type derive(data const& d);

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
    : info_{util::make_intrusive<intrusive_info>(std::forward<T>(x))} {
  }

  explicit type(util::intrusive_ptr<intrusive_info> ii)
    : info_{std::move(ii)} {
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
  using is_arithmetic = std::integral_constant<
      bool,
      std::is_same<T, boolean>::value
        || std::is_same<T, integer>::value
        || std::is_same<T, count>::value
        || std::is_same<T, real>::value
        || std::is_same<T, time_point>::value
        || std::is_same<T, time_interval>::value
        || std::is_same<T, time_duration>::value
        || std::is_same<T, time_period>::value
    >;

  template <typename T>
  using is_basic = std::integral_constant<
      bool,
      is_arithmetic<T>{}
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

  util::intrusive_ptr<intrusive_info> info_;
};

class type::vector : public type::base {
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  vector(type t, std::vector<attribute> a = {});

  type const& elem() const;

private:
  vector() = default;

  type elem_;
};

class type::set : public base {
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  set(type t, std::vector<attribute> a = {});

  type const& elem() const;

private:
  set() = default;

  type elem_;
};

class type::table : public type::base {
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  table(type k, type v, std::vector<attribute> a = {});

  type const& key() const;

  type const& value() const;

private:
  table() = default;

  type key_;
  type value_;
};

class type::record : public type::base {
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;
  friend type::record flatten(type::record const& rec);
  friend type::record unflatten(type::record const& rec);

public:
  struct field : util::equality_comparable<field> {
    field(std::string n = {}, type t = {});

    friend bool operator==(field const& lhs, field const& rhs);

    std::string name;
    vast::type type;
  };

  /// Enables recursive record iteration.
  class each : public util::range_facade<each> {
  public:
    struct range_state {
      vast::key key() const;
      size_t depth() const;

      util::stack::vector<8, field const*> trace;
      vast::offset offset;
    };

    each(record const& r);

  private:
    friend util::range_facade<each>;

    range_state const& state() const {
      return state_;
    }

    bool next();

    range_state state_;
    util::stack::vector<8, record const*> records_;
  };

  record(std::initializer_list<field> fields, std::vector<attribute> a = {});
  record(std::vector<field> fields, std::vector<attribute> a = {});

  /// Retrieves the fields of the record.
  /// @returns The field of the records.
  std::vector<field> const& fields() const;

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
};

/// Recursively flattens the arguments of a record type.
/// @param rec the record to flatten.
/// @returns The flattened record type.
type::record flatten(type::record const& rec);

type flatten(type const& t);

/// Unflattens a flattened record type.
/// @param rec the record to unflatten.
/// @returns The unflattened record type.
type::record unflatten(type::record const& rec);

type unflatten(type const& t);

class type::alias : public type::base {
  friend access;
  friend type::info;
  friend detail::type_reader;
  friend detail::type_writer;

public:
  alias(vast::type t, std::vector<attribute> a = {});

  vast::type const& type() const;

private:
  alias() = default;

  vast::type type_;
};

struct type::intrusive_info : util::intrusive_base<intrusive_info>, type::info {
  intrusive_info() = default;

  template <typename T,
            typename = util::disable_if_same_or_derived_t<intrusive_info, T>>
  intrusive_info(T&& x)
    : type::info{std::forward<T>(x)} {
  }

  friend type::info& expose(intrusive_info& i) {
    return static_cast<type::info&>(i);
  }

  friend type::info const& expose(intrusive_info const& i) {
    return static_cast<type::info const&>(i);
  }
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

} // namespace vast

namespace std {

template <>
struct hash<vast::type> {
  size_t operator()(vast::type const& t) const {
    return t.digest();
  }
};

} // namespace std

#endif
