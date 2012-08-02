#ifndef VAST_EXCEPTION_H
#define VAST_EXCEPTION_H

#include "vast/exception.h"

#include <exception>
#include <string>

namespace vast {

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

/// Thrown for errors with the program configuration.
struct config : public exception
{
  config(char const* msg, char const* option);
  config(char const* msg, char const* opt1, char const* opt2);
};

/// The base class for all exceptions during the ingestion process.
struct ingest : public exception
{
  ingest() = default;
  ingest(char const* msg);
  ingest(std::string const& msg);
};

/// Thrown when a parse error occurs while processing input data.
struct parse : public ingest
{
  parse() = default;
  parse(char const* msg);
};

struct segment : public exception
{
  segment(char const* msg);
};

/// Thrown when an error with a query occurs.
struct query : public exception
{
  query() = default;
  query(char const* msg);
};

/// Thrown when a syntax error with a query occurs.
struct syntax : public query
{
  syntax(std::string const& q);
};

/// Thrown when a semantic error with a query occurs.
struct semantic : public query
{
  semantic(std::string const& q);
};

} // namespace error
} // namespace vast

#endif
