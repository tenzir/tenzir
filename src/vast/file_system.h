#ifndef VAST_FILE_SYSTEM_H
#define VAST_FILE_SYSTEM_H

#include <functional>
#include <string>
#include "vast/config.h"
#include "vast/fwd.h"
#include "vast/string.h"
#include "vast/util/operators.h"

namespace vast {

/// A filesystem path abstraction.
class path : util::totally_ordered<path>,
             util::addable<path>,
             util::dividable<path>
{
  friend std::string to_string(path const& str);

public:
#ifdef VAST_WINDOWS
  static constexpr char const* separator = "\\";
#else
  static constexpr char const* separator = "/";
#endif

  /// The maximum length of a path.
  static constexpr size_t max_len = 1024;

  /// The type of a file.
  enum type
  {
    unknown,
    regular_file,
    directory,
    symlink,
    block,
    character,
    fifo,
    socket
  };

  /// Retrieves the path of the current directory.
  /// @param The absolute path.
  static path current();

  /// Default-constructs an empty path.
  path() = default;

  /// Constructs a path from a C-string.
  /// @param str The string representing of a path.
  path(char const* str);
  
  /// Constructs a path from a VAST string.
  /// @param str The string representing of a path.
  path(string str);

  path& operator/=(path const& p);
  path& operator+=(path const& p);

  /// Checks whether the path is empty.
  /// @param `true` if the path is empty.
  bool empty() const;

  /// Retrieves the parent directory.
  /// @return The parent of this path.
  path parent() const;

  /// Retrieves the basename of this path.
  /// @return The basename of this path.
  path basename(bool strip_extension = false) const;

  /// Retrieves the extension of this path.
  /// @param The extension including ".".
  path extension() const;

  //
  // Filesystem operations
  //

  /// Retrieves the type of the path.
  type kind() const;

  /// Checks whether the file type is a regular file.
  /// @return `true` if the path points to a regular file.
  bool is_regular_file() const;

  /// Checks whether the file type is a directory.
  /// @return `true` if the path points to a directory.
  bool is_directory() const;

  /// Checks whether the file type is a symlink.
  /// @return `true` if the path is a symlink.
  bool is_symlink() const;

  friend bool operator==(path const& x, path const& y);
  friend bool operator<(path const& x, path const& y);

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  string str_;
};

std::ostream& operator<<(std::ostream& out, path const& p);

/// A file abstraction.
class file
{
  file(file const&) = delete;

public:
  /// The native type of a file.
#ifdef VAST_POSIX
  typedef int native_type;
#else
  typedef void native_type;
#endif

  /// The mode in which to open a file.
  enum open_mode
  {
    read_only,
    write_only,
    read_write,
  };

  /// Default-constructs a file.
  file() = default;

  /// Constructs a file from a path.
  /// @param p The file path.
  file(path p);

  /// Constructs a file from the OS' native file handle type.
  /// @param p The file path.
  /// @param handle The file handle.
  /// @pre The file identified via handle is open.
  file(path p, native_type handle);

  /// Move-construfts a file.
  /// @param other The file to move.
  file(file&& other);

  /// Destroys and closes a file.
  ~file();

  /// Move-Assigns a file to this instance.
  /// @param other The RHS of the assignment.
  file& operator=(file&& other);

  /// Opens the file.
  /// @param mode How to open the file.
  /// @param append If `false`, the first write truncates the file.
  /// @return `true` on success.
  bool open(open_mode mode = read_write, bool append = false);

  /// Closes the file.
  /// @return `true` on success.
  bool close();

  /// Checks whether the file is open.
  /// @return `true` iff the file is open
  bool is_open() const;

  /// Reads a given number of bytes in to a buffer.
  /// @param sink The destination of the read.
  /// @param size The number of bytes to read.
  /// @param got The number of bytes read.
  /// @return `true` on success.
  bool read(void* sink, size_t size, size_t* got = nullptr);

  /// Writes a given number of bytes into a buffer.
  /// @param source The source of the write.
  /// @param size The number of bytes to write.
  /// @param put The number of bytes written.
  /// @return `true` on success.
  bool write(void const* source, size_t size, size_t* put = nullptr);

  /// Seeks the file forward.
  ///
  /// @param bytes The number of bytes to seek forward relative to the current
  /// position.
  ///
  /// @param skipped Set to the number of bytes skipped.
  ///
  /// @return `true` on success.
  bool seek(size_t bytes, size_t* skipped = nullptr);

private:
  native_type handle_;
  bool is_open_ = false;
  bool seek_failed_ = false;
  path path_;
};

/// Checks whether the path exists on the filesystem.
/// @param p The path to check for existance.
/// @return `true` if *p* exists.
bool exists(path const& p);

/// Deletes the path on the filesystem.
/// @param p The path to a directory to delete.
/// @return `true` if *p* has been successfully deleted.
bool rm(path const& p);

/// If the path does not exist, create it as directory.
/// @param p The path to a directory to create.
/// @return `true` on success or if *p* exists already.
bool mkdir(path const& p);

/// Traverses each entry of a directory.
///
/// @param p The path to a directory.
///
/// @param f The function to call for each directory entry. The return value
/// of *f* indicates whether to continue (`true`) or to stop (`false`)
/// iterating.
void traverse(path const& p, std::function<bool(path const&)> f);

} // namespace vast

#endif
