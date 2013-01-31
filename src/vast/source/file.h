#ifndef VAST_SOURCE_FILE_H
#define VAST_SOURCE_FILE_H

#include <cassert>
#include <fstream>
#include <ze/fwd.h>
#include <ze/value_type.h>
#include <ze/string.h>
#include "vast/source/synchronous.h"

namespace vast {
namespace source {

/// A file source that transforms file contents into events.
class file : public synchronous
{
public:
  /// Constructs a file source.
  /// @param filename The name of the file to ingest.
  file(std::string const& filename);

protected:
  std::ifstream file_;
};


/// A file source that processes input line by line.
class line : public file
{
public:
  line(std::string const& filename);

protected:
  virtual ze::event extract() override;
  virtual ze::event parse(std::string const& line) = 0;

  /// Retrieves the next line from the file.
  /// @return `true` if extracting was successful.
  bool next();

  size_t current_ = 0;
  std::string line_;
};

/// A generic Bro 2.x log file source.
class bro2 : public line
{
public:
  bro2(std::string const& filename);

private:
  /// Extracts the first `#`-lines of log meta data.
  void parse_header();

  /// Converts a Bro type to a 0event type. Does not support container types.
  ze::value_type bro_to_ze(ze::string const& str);

  virtual ze::event parse(std::string const& line) override;

  ze::string separator_;
  ze::string set_separator_;
  ze::string empty_field_;
  ze::string unset_field_;
  ze::string path_;
  std::vector<ze::string> field_names_;
  std::vector<ze::value_type> field_types_;
  std::vector<ze::value_type> set_types_;
};

/// A Bro 1.5 `conn.log` source.
class bro15conn : public line
{
public:
    bro15conn(std::string const& filename);

private:
  /// Parses a single log line.
  virtual ze::event parse(std::string const& line) override;
};

} // namespace source
} // namespace vast

#endif
