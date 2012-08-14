#ifndef VAST_SOURCE_FILE_H
#define VAST_SOURCE_FILE_H

#include <cassert>
#include <fstream>
#include <cppa/cppa.hpp>
#include <ze/forward.h>
#include <ze/value_type.h>
#include <ze/type/string.h>
#include "vast/event_source.h"

namespace vast {
namespace source {

/// A file that transforms file contents into events.
class file : public event_source
{
  friend class cppa::sb_actor<file>;

public:
  /// Constructs a file source.
  /// @param ingestor The ingestor.
  /// @param tracker The event ID tracker.
  /// @param filename The name of the file to ingest.
  file(cppa::actor_ptr ingestor,
       cppa::actor_ptr tracker,
       std::string const& filename);

protected:
  virtual ze::event extract() = 0;

  std::ifstream file_;
};


/// A file source that processes input line by line.
class line : public file
{
public:
  line(cppa::actor_ptr ingestor,
       cppa::actor_ptr tracker,
       std::string const& filename);

protected:
  virtual ze::event extract();
  virtual ze::event parse(std::string const& line) = 0;

  /// Retrieves the next line from the file.
  /// @return `true` if extracting was successful.
  bool next();

  size_t current_line_ = 0;
  std::string line_;
};

/// A generic Bro 2.x log file source.
class bro2 : public line
{
public:
  bro2(cppa::actor_ptr ingestor,
       cppa::actor_ptr tracker,
       std::string const& filename);

private:
  /// Extracts the first `#`-lines of log meta data.
  void parse_header();

  /// Converts a Bro type to a 0event type. Does not support container types.
  ze::value_type bro_to_ze(ze::string const& str);

  virtual ze::event parse(std::string const& line);

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
    bro15conn(cppa::actor_ptr ingestor,
              cppa::actor_ptr tracker,
              std::string const& filename);

private:
  /// Parses a single log line.
  virtual ze::event parse(std::string const& line);
};

} // namespace source
} // namespace vast

#endif
