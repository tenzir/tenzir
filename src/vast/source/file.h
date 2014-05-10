#ifndef VAST_SOURCE_FILE_H
#define VAST_SOURCE_FILE_H

#include <cassert>
#include "vast/type_tag.h"
#include "vast/schema.h"
#include "vast/string.h"
#include "vast/io/file_stream.h"
#include "vast/io/getline.h"
#include "vast/source/synchronous.h"

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
  file(cppa::actor sink, std::string const& filename)
    : synchronous<file<Derived>>{std::move(sink)},
      file_handle_{path{filename}},
      file_stream_{file_handle_}
  {
    file_handle_.open(vast::file::read_only);
  }

  result<event> extract()
  {
    return static_cast<Derived*>(this)->extract_impl();
  }

  bool done() const
  {
    return current_line() == nullptr;
  }

  char const* description() const
  {
    return static_cast<Derived const*>(this)->description_impl();
  }

  /// Retrieves the next non-empty line from the file.
  ///
  /// @returns A pointer to the next line if extracting was successful and
  /// `nullptr` on failure or EOF.
  std::string const* next()
  {
    if (! file_handle_.is_open())
      return nullptr;

    line_.clear();
    while (line_.empty())
      if (io::getline(file_stream_, line_))
        ++current_;
      else
        return nullptr;

    return &line_;
  }

  uint64_t line_number() const
  {
    return current_;
  }

  std::string const* current_line() const
  {
    return line_.empty() ? nullptr : &line_;
  }

private:
  vast::file file_handle_;
  io::file_input_stream file_stream_;
  uint64_t current_ = 0;
  std::string line_;
};


/// A generic Bro 2.x log file source.
class bro2 : public file<bro2>
{
public:
  bro2(cppa::actor sink, std::string const& filename,
       int32_t timestamp_field);

  result<event> extract_impl();

  char const* description_impl() const;

private:
  trial<void> parse_header();

  int32_t timestamp_field_ = -1;
  string separator_ = " ";
  string set_separator_;
  string empty_field_;
  string unset_field_;
  schema schema_;
  type_ptr type_;
  type_ptr flat_type_;
};

} // namespace source
} // namespace vast

#endif
