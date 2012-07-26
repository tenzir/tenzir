#ifndef VAST_INGEST_READER_H
#define VAST_INGEST_READER_H

#include <cassert>
#include <fstream>
#include <cppa/cppa.hpp>
#include <ze/forward.h>
#include <ze/value.h>

namespace vast {
namespace ingest {

/// A reader that transforms file contents into events.
class reader : public cppa::sb_actor<reader>
{
  friend class cppa::sb_actor<reader>;

public:
  /// Constructs a reader.
  /// @param ingestor The ingestor.
  /// @param tracker The event ID tracker.
  /// @param upstream The upstream actor receiving the events.
  /// @param filename The name of the file to ingest.
  reader(cppa::actor_ptr ingestor,
         cppa::actor_ptr tracker,
         cppa::actor_ptr upstream,
         std::string const& filename);

  virtual ~reader() = default;

protected:
  /// Asks the ID tracker for a batch of new IDs.
  /// @param n The number of IDs to request.
  void ask_for_new_ids(size_t n);

  /// Extracts events from a filestream.
  /// @param batch_size The number of events to extract in one run.
  /// @return The vector of extracted events.
  virtual std::vector<ze::event> extract(size_t batch_size) = 0;

  cppa::actor_ptr tracker_;
  cppa::actor_ptr upstream_;
  std::ifstream file_;
  uint64_t next_id_ = 0;
  uint64_t last_id_ = 0;

private:
  cppa::behavior init_state;
  size_t total_events_ = 0;
};


/// A reader that processes line-based input.
class line_reader : public reader
{
public:
  line_reader(cppa::actor_ptr ingestor,
              cppa::actor_ptr tracker,
              cppa::actor_ptr upstream,
              std::string const& filename);

protected:
  /// Parses a single log line.
  virtual ze::event parse(std::string const& line) = 0;

private:
  virtual std::vector<ze::event> extract(size_t batch_size);

  size_t current_line_ = 0;
};

class bro_reader : public line_reader
{
public:
  bro_reader(cppa::actor_ptr ingestor,
             cppa::actor_ptr tracker,
             cppa::actor_ptr upstream,
             std::string const& filename);

  /// Extracts log meta data.
  void parse_header();

protected:
  /// Parses a single log line.
  virtual ze::event parse(std::string const& line);

  ze::string separator_;
  ze::string set_separator_;
  ze::string empty_field_;
  ze::string unset_field_;
  ze::string path_;
  std::vector<ze::string> field_names_;
  std::vector<ze::value_type> field_types_;
  std::vector<ze::value_type> set_types_;

private:
  /// Converts a Bro type to a 0event type. Does not support container types.
  ze::value_type bro_to_ze(ze::string const& str);
};

/// A Bro 1.5 `conn.log` reader.
class bro_15_conn_reader : public line_reader
{
public:
  bro_15_conn_reader(cppa::actor_ptr ingestor,
                     cppa::actor_ptr tracker,
                     cppa::actor_ptr upstream,
                     std::string const& filename);

private:
  /// Parses a single log line.
  virtual ze::event parse(std::string const& line);
};

} // namespace ingest
} // namespace vast

#endif
