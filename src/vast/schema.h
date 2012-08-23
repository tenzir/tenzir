#ifndef VAST_SCHEMA_H
#define VAST_SCHEMA_H

#include <vector>
#include <string>
#include "vast/intrusive.h"

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
    virtual ~type() = default;
  };

  struct type_info
  {
    inline explicit operator bool() const
    {
      return type.get() != nullptr;
    }

    std::string name;
    std::vector<std::string> aliases;
    intrusive_ptr<type> type;
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
  };

  struct record_type : complex_type
  {
    std::vector<argument> args;
  };

  struct event
  {
    std::string name;
    std::vector<argument> args;
  };

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
  template <typename Archive>
  friend void serialize(Archive& oa, schema const& s)
  {
    oa << to_string(s);
  }

  template <typename Archive>
  friend void deserialize(Archive& ia, schema& s)
  {
    std::string str;
    ia >> str;
    s.load(str);
  }

  friend bool operator==(schema const& x, schema const& y);

  std::vector<type_info> types_;
  std::vector<event> events_;
};

bool operator==(schema::type_info const& x, schema::type_info const& y);
bool operator==(schema::argument const& x, schema::argument const& y);
bool operator==(schema::event const& x, schema::event const& y);

} // namespace vast

#endif
