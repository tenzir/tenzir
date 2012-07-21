#ifndef VAST_INGEST_READER_H
#define VAST_INGEST_READER_H

#include <fstream>
#include <cppa/cppa.hpp>
#include <ze/forward.h>

namespace vast {
namespace ingest {

/// A reader that transforms file contents into events.
class reader : public cppa::sb_actor<reader>
{
  friend class cppa::sb_actor<reader>;

public:
  /// Constructs a reader.
  /// @param upstream The upstream actor receiving the events.
  reader(cppa::actor_ptr upstream, std::string const& filename);
  virtual ~reader() = default;

protected:
  /// Helper class to create iterator ranges separated by a given separator.
  template <typename Iterator>
  class field_splitter
  {
  public:
    field_splitter(Iterator start, Iterator end, char sep_char = ' ')
      : field_start_(start)
      , field_end_(start)
      , end_(end)
      , sep_char_(sep_char)
    {
      if (field_start_ ==  end_)
        return;

      while (! (*field_end_ == sep_char_ || field_end_ == end_))
        ++field_end_;
    }

    /// Advances the internal iterator range to the next field.
    /// @return `true` *iff* there is more than one field in the range.
    bool advance()
    {
      if (field_end_ == end_)
        return false;

      field_start_ = ++field_end_;
      if (field_end_ == end_)
        return false;

      while (! (*field_end_ == sep_char_ || field_end_ == end_))
        ++field_end_;

      return true;
    }

    Iterator& start()
    {
      return field_start_;
    }

    Iterator& end()
    {
      return field_end_;
    }

    private:
      Iterator field_start_;
      Iterator field_end_;
      Iterator end_;
      char sep_char_;
  };

  /// Extracts events from a filestream.
  /// @param ifs The file stream to extract events from.
  /// @param batch_size The number of events to extract in one run.
  /// @return The vector of extracted events.
  virtual std::vector<ze::event> extract(std::ifstream& ifs,
                                         size_t batch_size) = 0;
  cppa::actor_ptr upstream_;

private:
  cppa::behavior init_state;
  size_t total_events_ = 0;
  std::ifstream file_;
};


/// A reader that processes line-based input.
class line_reader : public reader
{
public:
  line_reader(cppa::actor_ptr upstream, std::string const& filename);

protected:
  /// Parses a single log line.
  virtual ze::event parse(std::string const& line) = 0;

private:
  virtual std::vector<ze::event> extract(std::ifstream& ifs, size_t batch_size);

  size_t current_line_ = 0;
};

/// A Bro 1.5 `conn.log` reader.
class bro_15_conn_reader : public line_reader
{
public:
  bro_15_conn_reader(cppa::actor_ptr upstream, std::string const& filename);

private:
  /// Parses a single log line.
  virtual ze::event parse(std::string const& line);
};

} // namespace ingest
} // namespace vast

#endif
