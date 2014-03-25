#ifndef VAST_SCHEMA_H
#define VAST_SCHEMA_H

#include <functional>
#include <vector>
#include <string>
#include "vast/fwd.h"
#include "vast/offset.h"
#include "vast/util/intrusive.h"
#include "vast/util/trial.h"

namespace vast {

class schema
{
public:
  struct type;

  struct type_info
  {
    type_info() = default;
    type_info(std::string name, intrusive_ptr<schema::type> t);

    inline explicit operator bool() const
    {
      return type.get() != nullptr;
    }

    bool convert(std::string& to) const;

    std::string name;
    std::vector<std::string> aliases;
    intrusive_ptr<schema::type> type;
  };

  struct type : intrusive_base<type>
  {
    type() = default;
    virtual ~type() = default;
    virtual bool convert(std::string& to) const = 0;
  };

  struct basic_type : type { };

#define VAST_DEFINE_BASIC_TYPE(concrete_type)               \
  struct concrete_type : basic_type                         \
  {                                                         \
    virtual bool convert(std::string& to) const override;   \
  };

  VAST_DEFINE_BASIC_TYPE(bool_type)
  VAST_DEFINE_BASIC_TYPE(int_type)
  VAST_DEFINE_BASIC_TYPE(uint_type)
  VAST_DEFINE_BASIC_TYPE(double_type)
  VAST_DEFINE_BASIC_TYPE(time_frame_type)
  VAST_DEFINE_BASIC_TYPE(time_point_type)
  VAST_DEFINE_BASIC_TYPE(string_type)
  VAST_DEFINE_BASIC_TYPE(regex_type)
  VAST_DEFINE_BASIC_TYPE(address_type)
  VAST_DEFINE_BASIC_TYPE(prefix_type)
  VAST_DEFINE_BASIC_TYPE(port_type)

#undef VAST_DEFINE_BASIC_TYPE

  struct complex_type : type { };
  struct container_type : complex_type { };

  struct enum_type : complex_type
  {
    virtual bool convert(std::string& to) const override;

    std::vector<std::string> fields;
  };

  struct vector_type : complex_type
  {
    virtual bool convert(std::string& to) const override;

    type_info elem_type;
  };

  struct set_type : complex_type
  {
    virtual bool convert(std::string& to) const override;

    type_info elem_type;
  };

  struct table_type : complex_type
  {
    virtual bool convert(std::string& to) const override;

    type_info key_type;
    type_info value_type;
  };

  struct argument
  {
    bool convert(std::string& to) const;

    std::string name;
    type_info type;
    bool optional = false;
    bool indexed = true;
  };

  struct record_type : complex_type
  {
    virtual bool convert(std::string& to) const override;

    std::vector<argument> args;
  };

  struct event : record_type
  {
    virtual bool convert(std::string& to) const override;

    std::string name;
    bool indexed = true;
  };

  /// Loads schema contents.
  /// @param contents The contents of a schema file.
  /// @returns An engaged trial on success.
  static trial<schema> load(std::string const& contents);

  /// Loads a schema from a file.
  /// @param filename The schema file.
  /// @returns An engaged trial on success.
  static trial<schema> read(std::string const& filename);

  /// Finds the offsets vectors for a given symbol sequence.
  ///
  /// @param rec The record to search for symbol types whose name matches
  /// the first element of *ids*.
  ///
  /// @param ids The name sequence to look for in *rec*. If
  /// `ids.size() > 1`, the first element represents a symbol of type record
  /// and each subsequent elements of *ids* then represent further argument
  /// names to dereference.
  ///
  /// @returns A vector of offsets. The size of the vector corresponds to the
  /// number of different offsets that exist in *rec* and resolve for *ids*.
  static trial<std::vector<offset>>
  lookup(record_type const* rec, std::vector<std::string> const& ids);

  /// Resolves a sequence of names into an offset.
  /// @param rec The event/record to search.
  /// @param ids The argument names to look for in *rec*.
  /// @returns The offset corresponding to *ids*.
  static trial<offset>
  resolve(record_type const* rec, std::vector<std::string> const& ids);

  /// Default-constructs a schema.
  schema() = default;

  /// Saves the schema to a file.
  /// @param filename The schema file.
  void write(std::string const& filename) const;

  /// Retrieves the list of all types in the schema.
  /// @returns a vector with type information objects.
  std::vector<type_info> const& types() const;

  /// Retrieves the list of all events in the schema.
  /// @returns a vector with event schema objects.
  std::vector<event> const& events() const;

  /// Retrieves the type information for a given type name.
  ///
  /// @param name The name of the type to lookup.
  ///
  /// @returns A schema::type_info object for the type *name* or and empty
  /// schema:type_info object if *name* does not reference an existing type.
  type_info info(std::string const& name) const;

  /// Creates a new type information object.
  /// @param name The name of *t*.
  /// @param t The schema type.
  /// @returns `true` on success.
  bool add_type(std::string name, intrusive_ptr<type> t);

  /// Creates a type alias.
  /// @param type The name of the type to create an alias for.
  /// @param alias Another name for *type*.
  /// @returns `true` if aliasing succeeded and `false` if *type* does not exist.
  bool add_type_alias(std::string const& type, std::string const& alias);

  /// Adds an event schema.
  /// @param e The event schema.
  void add_event(event e);

private:
  std::vector<type_info> types_;
  std::vector<event> events_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);
  bool convert(std::string& to) const;

  friend bool operator==(schema const& x, schema const& y);
  friend bool operator!=(schema const& x, schema const& y);
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
