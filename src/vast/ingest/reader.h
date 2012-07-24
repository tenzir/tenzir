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
  /// @param upstream The upstream actor receiving the events.
  reader(cppa::actor_ptr upstream, std::string const& filename);
  virtual ~reader() = default;

protected:
  /// Helper class to create iterator ranges separated by a given character
  /// sequence.
  /// @tparam Iterator A random access iterator.
  template <typename Iterator>
  class field_splitter
  {
    static_assert(
        std::is_same<
          typename std::iterator_traits<Iterator>::iterator_category,
          std::random_access_iterator_tag
        >::value,
        "field splitter requires random access iterator");

  public:
    /// Splits the given range *[start,end)* into fields.
    ///
    /// @param start The first element of the range.
    ///
    /// @param end One element past the last element of the range.
    ///
    /// @param max_fields The maximum number of fields to split. If there
    /// exists more input after the last split operation at position *p*, then
    /// the range *[p, end)* will constitute the final element.
    void split(Iterator start, Iterator end, int max_fields = -1)
    {
      auto begin = start;
      while (start != end)
      {
        while (*start != sep_[0] && start != end)
            ++start;

        if (start == end || --max_fields == 0)
        {
            fields_.emplace_back(begin, end);
            return;
        }

        auto cand_end = start++;
        auto is_end = true;
        for (size_t i = 1; i < sep_len_; ++i)
        {
          printf("sep[%zu]: %c, *start == %c", i, sep_[i], *start);
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

    /// Retrieves the start position of a given field.
    Iterator start(size_t i) const
    {
      assert(i < fields_.size());
      return fields_[i].first;
    }

    /// Retrieves the end position of a given field.
    Iterator end(size_t i) const
    {
      assert(i < fields_.size());
      return fields_[i].second;
    }

    /// Sets the field separator.
    void sep(char const* s, size_t len)
    {
      sep_ = s;
      sep_len_ = len;
    }

    /// Retrieves the number of fields.
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
  /// @param batch_size The number of events to extract in one run.
  /// @return The vector of extracted events.
  virtual std::vector<ze::event> extract(size_t batch_size) = 0;
  cppa::actor_ptr upstream_;
  std::ifstream file_;

private:
  cppa::behavior init_state;
  size_t total_events_ = 0;
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
  virtual std::vector<ze::event> extract(size_t batch_size);

  size_t current_line_ = 0;
};

class bro_reader : public line_reader
{
public:
  bro_reader(cppa::actor_ptr upstream, std::string const& filename);

  /// Extracts log meta data.
  void parse_header();

protected:
  /// Parses a single log line.
  virtual ze::event parse(std::string const& line);

  ze::string escape_;
  ze::string separator_;
  ze::string set_separator_;
  ze::string empty_field_;
  ze::string unset_field_;
  ze::string path_;
  std::vector<ze::string> field_names_;
  std::vector<ze::value_type> field_types_;

private:
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
