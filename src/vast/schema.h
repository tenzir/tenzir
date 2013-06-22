#ifndef VAST_SCHEMA_H
#define VAST_SCHEMA_H

#include <functional>
#include <vector>
#include <string>
#include "vast/intrusive.h"
#include "vast/fwd.h"

namespace vast {

// Forward declarations.
class schema;
std::string to_string(schema const&);

class schema
{
  friend std::string to_string(schema const& s);

public:
  struct type : intrusive_base<type>
  {
    type() = default;
#ifdef __clang__
    virtual ~type() = default;
#else
    // TODO: There is a bug in GCC 4.7
    // (http://gcc.gnu.org/bugzilla/show_bug.cgi?id=53613) prevents us from
    // using the =default version of the destructor. We can change this once
    // 4.8 is the new standard.
    virtual ~type() {}
#endif
  };

  struct type_info
  {
    type_info() = default;
    type_info(std::string name, intrusive_ptr<schema::type> t);

    inline explicit operator bool() const
    {
      return type.get() != nullptr;
    }

    std::string name;
    std::vector<std::string> aliases;
    intrusive_ptr<schema::type> type;
  };

  struct basic_type : type { };
  struct complex_type : type { };
  struct container_type : complex_type { };
  struct bool_type : basic_type { };
  struct int_type : basic_type { };
  struct uint_type : basic_type { };
  struct double_type : basic_type { };
  struct time_point_type : basic_type { };
  struct time_frame_type : basic_type { };
  struct string_type : basic_type { };
  struct regex_type : basic_type { };
  struct address_type : basic_type { };
  struct prefix_type : basic_type { };
  struct port_type : basic_type { };

  struct enum_type : complex_type
  {
    std::vector<std::string> fields;
  };

  struct vector_type : complex_type
  {
    type_info elem_type;
  };

  struct set_type : complex_type
  {
    type_info elem_type;
  };

  struct table_type : complex_type
  {
    type_info key_type;
    type_info value_type;
  };

  struct argument
  {
    std::string name;
    type_info type;
    bool optional = false;
    bool indexed = true;
  };

  struct record_type : complex_type
  {
    std::vector<argument> args;
  };

  struct event : record_type
  {
    std::string name;
    bool indexed = true;
  };

  /// Computes the offsets vectors for a given symbol sequence.
  ///
  /// @param record The record to search for symbol types whose name matches
  /// the first element of *ids*.
  ///
  /// @param ids The name sequence to look for in *record*. If
  /// `ids.size() > 1`, the first element represents a symbol of type record
  /// and each subsequent elements of *ids* then represent further argument
  /// names to dereference.
  ///
  /// @return A vector of offset vectors. Each offset vector represents a
  /// sequence of offsets which have to be used in order to get to *name*.
  /// Since *record* may contain multiple arguments of type *name*, the result
  /// is a vector of vectors.
  static std::vector<std::vector<size_t>> symbol_offsets(
      record_type const* record,
      std::vector<std::string> const& ids);

  /// Computes the offsets vector for a given argument name sequence.
  /// @param record The event/record to search.
  /// @param ids The argument names to look for in *record*.
  /// @return A vector of offsets to get to *ids*.
  static std::vector<size_t> argument_offsets(
      record_type const* record,
      std::vector<std::string> const& ids);

  /// Default-constructs a schema.
  schema() = default;

  /// Loads schema contents.
  /// @param contents The contents of a schema file.
  void load(std::string const& contents);

  /// Loads a schema from a file.
  /// @param filename The schema file.
  void read(std::string const& filename);

  /// Saves the schema to a file.
  /// @param filename The schema file.
  void write(std::string const& filename) const;

  /// Retrieves the list of all types in the schema.
  /// @return a vector with type information objects.
  std::vector<type_info> const& types() const;

  /// Retrieves the list of all events in the schema.
  /// @return a vector with event schema objects.
  std::vector<event> const& events() const;

  /// Retrieves the type information for a given type name.
  ///
  /// @param name The name of the type to lookup.
  ///
  /// @return A schema::type_info object for the type *name* or and empty
  /// schema:type_info object if *name* does not reference an existing type.
  type_info info(std::string const& name) const;

  /// Creates a new type information object.
  /// @param name The name of *t*.
  /// @param t The schema type.
  void add_type(std::string name, intrusive_ptr<type> t);

  /// Creates a type alias.
  /// @param type The name of the type to create an alias for.
  /// @param alias Another name for *type*.
  /// @return `true` if aliasing succeeded and `false` if *type* does not exist.
  bool add_type_alias(std::string const& type, std::string const& alias);

  /// Adds an event schema.
  /// @param e The event schema.
  void add_event(event e);

private:
  friend access;
  void serialize(serializer& sink);
  void deserialize(deserializer& source);
  friend bool operator==(schema const& x, schema const& y);
  friend bool operator!=(schema const& x, schema const& y);

  std::vector<type_info> types_;
  std::vector<event> events_;
};

bool operator==(schema::type_info const& x, schema::type_info const& y);
bool operator==(schema::argument const& x, schema::argument const& y);
bool operator==(schema::event const& x, schema::event const& y);

} // namespace vast

namespace std {

template <>
struct hash<vast::schema>
{
  size_t operator()(vast::schema const& sch) const;
};

} // namespace std

#endif
