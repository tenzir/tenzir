#ifndef VAST_EXCEPTION_H
#define VAST_EXCEPTION_H

#include "vast/exception.h"
#include "vast/config.h"

#include <exception>
#include <string>

namespace vast {

// Forward declarations.
enum boolean_operator : uint8_t;
enum arithmetic_operator : uint8_t;
enum relational_operator : uint8_t;

/// The base class for all exception thrown by VAST. It is never thrown
/// directly but all exceptions thrown in VAST have to derive from it.
class exception : public std::exception
{
public:
  exception() = default;
  exception(char const* msg);
  exception(std::string const& msg);

  virtual char const* what() const noexcept;

protected:
  std::string msg_;
};

/// The namespace for all exceptions.
namespace error {

/// Thrown for network errors.
struct network : exception
{
  network(char const* msg);
};

#ifdef VAST_HAVE_BROCCOLI
/// Thrown for errors with Broccoli.
struct broccoli : network
{
  broccoli(char const* msg);
};
#endif

/// Thrown for errors with the program configuration.
struct config : exception
{
  config(char const* msg, char const* option);
  config(char const* msg, char const* opt1, char const* opt2);
};

/// The base class for all exceptions during the ingestion process.
struct ingest : exception
{
  ingest() = default;
  ingest(char const* msg);
  ingest(std::string const& msg);
};

/// Thrown when a parse error occurs while processing input data.
struct parse : ingest
{
  parse() = default;
  parse(char const* msg);
  parse(char const* msg, size_t line);
};

struct segment : exception
{
  segment(char const* msg);
};

/// Thrown when an error with a query occurs.
struct query : exception
{
  query() = default;
  query(char const* msg);
  query(char const* msg, std::string const& expr);
};

/// Thrown when an error with a schema occurs.
struct schema : exception
{
  schema() = default;
  schema(char const* msg);
};

/// Thrown when an error with an index occurs.
struct index : exception
{
  index() = default;
  index(char const* msg);
};

/// Thrown when an error with an operator occurs.
struct operation : exception
{
  operation() = default;
  operation(char const* msg, arithmetic_operator op);
  operation(char const* msg, boolean_operator op);
  operation(char const* msg, relational_operator op);
};

} // namespace error
} // namespace vast

#endif
