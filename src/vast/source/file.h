#ifndef VAST_SOURCE_FILE_H
#define VAST_SOURCE_FILE_H

#include <cassert>
#include "vast/convert.h"
#include "vast/value_type.h"
#include "vast/string.h"
#include "vast/io/file_stream.h"
#include "vast/source/synchronous.h"

namespace vast {

class event;

namespace source {

/// A file source that transforms file contents into events.
class file : public synchronous
{
public:
  /// Constructs a file source.
  /// @param sink The actor to send the generated events to.
  /// @param filename The name of the file to ingest.
  file(cppa::actor_ptr sink, std::string const& filename);

  /// Implements `synchronous::finished`.
  virtual bool finished() final;

protected:
  vast::file file_handle_;
  io::file_input_stream file_stream_;
  bool finished_ = false;
};


/// A file source that processes input line by line.
class line : public file
{
public:
  line(cppa::actor_ptr sink, std::string const& filename);

protected:
  virtual option<event> extract() override;
  virtual option<event> parse(std::string const& line) = 0;

  /// Retrieves the next line from the file.
  /// @returns `true` if extracting was successful.
  bool next();

  size_t current_ = 0;
  std::string line_;
};

/// A generic Bro 2.x log file source.
class bro2 : public line
{
public:
  bro2(cppa::actor_ptr sink, std::string const& filename);

private:
  /// Extracts the first `#`-lines of log meta data.
  bool parse_header();

  /// Converts a Bro type to a VAST type. Does not support container types.
  value_type bro_to_vast(string const& str);

  virtual option<event> parse(std::string const& line) override;

  string separator_;
  string set_separator_;
  string empty_field_;
  string unset_field_;
  string path_;
  std::vector<string> field_names_;
  std::vector<value_type> field_types_;
  std::vector<value_type> complex_types_;
};

/// A Bro 1.5 `conn.log` source.
class bro15conn : public line
{
public:
  bro15conn(cppa::actor_ptr sink, std::string const& filename);

private:
  /// Parses a single log line.
  virtual option<event> parse(std::string const& line) override;
};

} // namespace source
} // namespace vast

#endif
