#ifndef VAST_INGEST_READER_H
#define VAST_INGEST_READER_H

#include <cassert>
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
    field_splitter() = default;

    void split(Iterator start, Iterator end)
    {
      auto begin = start;
      while (start != end)
      {
        while (*start != sep_[0] && start != end)
            ++start;

        if (start == end)
        {
            fields_.emplace_back(begin, end);
            return;
        }

        auto cand_end = start++;
        auto is_end = true;
        for (auto i = 1; i < sep_len_; ++i)
        {
          if (start == end)
          {
            fields_.emplace_back(begin, end);
            return;
          }
          else if (*start == sep_[i])
          {
            ++start;
          }
          else
          {
            is_end = false;
            break;
          }
        }

        if (is_end)
        {
          fields_.emplace_back(begin, cand_end);
          begin = start;
        }
      }
    }

    Iterator start(size_t i) const
    {
      assert(i < fields_.size());
      return fields_[i].first;
    }

    Iterator end(size_t i) const
    {
      assert(i < fields_.size());
      return fields_[i].second;
    }

    void sep(char const* s, size_t len)
    {
      sep_ = s;
      sep_len_ = len;
    }

    size_t fields() const
    {
      return fields_.size();
    }

  private:
    typedef std::pair<Iterator, Iterator> iterator_pair;
    std::vector<iterator_pair> fields_;

    char const* sep_ = " ";
    size_t sep_len_ = 1;
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
