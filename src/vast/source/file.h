#ifndef VAST_SOURCE_FILE_H
#define VAST_SOURCE_FILE_H

#include <cassert>
#include "vast/value_type.h"
#include "vast/string.h"
#include "vast/io/file_stream.h"
#include "vast/io/getline.h"
#include "vast/source/synchronous.h"
#include "vast/util/convert.h"

namespace vast {

class event;

namespace source {

/// A file source that transforms file contents into events.
template <typename Derived>
class file : public synchronous<file<Derived>>
{
public:
  /// Constructs a file source.
  /// @param sink The actor to send the generated events to.
  /// @param filename The name of the file to ingest.
  file(cppa::actor_ptr sink, std::string const& filename)
    : synchronous<file<Derived>>{std::move(sink)},
      file_handle_{
          filename == "-"
            ? vast::file{path{filename}, ::fileno(stdin)}
            : vast::file{path{filename}}},
      file_stream_{file_handle_}
  {
    file_handle_.open(vast::file::read_only);
  }

  result<event> extract()
  {
    return static_cast<Derived*>(this)->extract_impl();
  }

  char const* description() const
  {
    return static_cast<Derived const*>(this)->description_impl();
  }

  bool good() const
  {
    return file_handle_.is_open();
  }

private:
  vast::file file_handle_;

protected:
  io::file_input_stream file_stream_;
};


/// A file source that processes input line by line.
template <typename Derived>
class line : public file<line<Derived>>
{
public:
  line(cppa::actor_ptr sink, std::string const& filename)
    : file<line<Derived>>{std::move(sink), filename}
  {
  }

  result<event> extract_impl()
  {
    return static_cast<Derived*>(this)->extract_impl_impl();
  }

  char const* description_impl() const
  {
    return static_cast<Derived const*>(this)->description_impl_impl();
  }

  /// Retrieves the next line from the file.
  ///
  /// @returns A pointer to the next line if extracting was successful and
  /// `nullptr` on failure or EOF.
  std::string const* next()
  {
    if (! this->good())
      return nullptr;

    line_.clear();
    while (line_.empty())
      if (io::getline(this->file_stream_, line_))
        ++current_;
      else
        return nullptr;

    return &line_;
  }

  uint64_t number() const
  {
    return current_;
  }

private:
  uint64_t current_ = 0;
  std::string line_;
};

/// A generic Bro 2.x log file source.
class bro2 : public line<bro2>
{
public:
  bro2(cppa::actor_ptr sink, std::string const& filename,
       int32_t timestamp_field);

  result<event> extract_impl_impl();

  char const* description_impl_impl() const;

private:
  int32_t timestamp_field_ = -1;
  string separator_ = " ";
  string set_separator_;
  string empty_field_;
  string unset_field_;
  string path_;
  std::vector<string> field_names_;
  std::vector<value_type> field_types_;
  std::vector<value_type> complex_types_;
};

} // namespace source
} // namespace vast

#endif
