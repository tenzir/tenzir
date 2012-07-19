#ifndef VAST_INGEST_READER_H
#define VAST_INGEST_READER_H

#include <iosfwd>
#include <cppa/cppa.hpp>

namespace vast {
namespace ingest {

/// A reader that transforms file contents into events.
class reader : public cppa::sb_actor<reader>
{
  friend class cppa::sb_actor<reader>;

public:
  /// Constructs a reader.
  /// @param upstream The upstream actor receiving the events.
  reader(cppa::actor_ptr upstream);
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
  /// @return `true` *iff* the reader successfully ingested the file.
  virtual bool extract(std::ifstream& ifs) = 0;

  cppa::actor_ptr upstream_;
  cppa::behavior init_state;
};


/// A reader for Bro log files.
class bro_reader : public reader
{
public:
  bro_reader(cppa::actor_ptr upstream);

protected:
  virtual bool extract(std::ifstream& ifs);
};

} // namespace ingest
} // namespace vast

#endif
