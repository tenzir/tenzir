#ifndef VAST_STRING_H
#define VAST_STRING_H

#include <string>
#include <vector>
#include "vast/fwd.h"
#include "vast/trial.h"
#include "vast/util/operators.h"

namespace vast {

/// An immutable POD string that that internally holds a NUL-terminated
/// C-string. However, the terminating NUL byte does not indicate the string
/// end as NUL bytes may naturally arise in the string.
///
/// The string also allows for storing an extra 7-bit tag in the last byte. If
/// the content is small enough, the internal buffer holds the data in situ. If
/// there is not enough space, the string allocates space on the heap and
/// stores a pointer to the buffer followed by the 32-bit size of the string.
/// The last bit of the tag indicates whether the string uses the heap or
/// stores its characters in situ.
///
///
/// If the string is stack-allocated, it looks schematically like this:
///
///              in_situ_size              8         8       7    1
///     +-----------------------...---+---------+---------+-----+---+
///     |                             |   \0    |   size  | tag | 0 |
///     +-----------------------...---+---------+---------+-----+---+
///
/// If allocated on the heap, it has the following structure:
///
///           32/64           32                             7    1
///     +-----------------+--------+-----------...--------+-----+---+
///     |      ptr        |  size  |                      | tag | 0 |
///     +-----------------+--------+-----------...--------+-----+---+
///
///
/// The minimum space requirements are:
///
///     - 4/8 bytes for the pointer size on 32/64-bit architectures
///     - 4 bytes for the string size
///     - 1 byte for the tag.
///
/// That is, 13 bytes on a 64-bit and 9 bytes on 32-bit machine.
class string : util::totally_ordered<string>
{
public:
  /// The size type of the counter.
  typedef uint32_t size_type;

private:
  // TODO: we currently hard-code the size of the largest value union member
  // in here, which is 16.
  // union member to 8 bytes.
  /// The minimum number of bytes needed.
  static size_type const min_buf_size = 16 + sizeof(size_type) + sizeof(char);

  /// The actual buffer size.
  static size_type const buf_size =
    sizeof(std::aligned_storage<min_buf_size>::type);

  /// The position of the pointer to the heap-allocated buffer.
  static size_type const heap_str_off = 0;

  /// The position of the string size in case the string is heap-allocated.
  static size_type const heap_cnt_off = sizeof(char*);

  /// The position of the tag.
  static size_type const tag_off = buf_size - 1;

  /// The position of the string size in case the string is heap-allocated.
  static size_type const in_situ_cnt_off = tag_off - 1;

  /// The position of the terminating NUL byte.
  static size_type const in_situ_nul_off = in_situ_cnt_off - 1;

public:
  typedef char* iterator;
  typedef char const* const_iterator;

  /// The maximum size of the string when stored in situ.
  static size_type const in_situ_size = buf_size - 3;

  /// The end-of-string indicator.
  static size_type const npos = -1;

  /// Constructs an empty string.
  string();

  /// Constructs a string from a single character.
  /// @param c The character.
  string(char c);

  /// Constructs a string from a NUL-terminated C string.
  /// @param str The C string.
  string(char const* str);

  /// Constructs a string from a C string of a given size.
  /// @param str The C string.
  /// @param size The length of *str*.
  string(char const* str, size_type size);

  /// Construct a string by copying a C++ string.
  /// @param str The C++ string.
  string(std::string const& str);

  /// Constructs a string from an iterator range.
  template <typename Iterator>
  string(Iterator begin, Iterator end)
    : string()
  {
    assign(begin, end);
  }

  /// Copy-constructs a string.
  /// @param other The string to copy.
  string(string const& other);

  /// Move-constructs a string.
  /// @param other The string to move.
  string(string&& other);

  /// Assigns a string.
  /// @param other The string to assign to this instance.
  /// @returns A reference to the LHS of the assignment.
  string& operator=(string other);

  /// Destroys a string. If the string was heap-allocated, it releases its
  /// memory.
  ~string();

  //
  // Access
  //

  /// Retrieves a character at a given position. No bounds check applied.
  ///
  /// @param i The character position.
  ///
  /// @returns The character at position *i*.
  char operator[](size_type i) const;

  const_iterator begin() const;
  const_iterator cbegin() const;
  const_iterator end() const;
  const_iterator cend() const;

  //
  // Inspectors
  //

  /// Retrieves the first character in the string.
  /// @pre `empty() == false`
  char front() const;

  /// Retrieves the last character in the string.
  /// @pre `empty() == false`
  char back() const;

  /// Determines whether the string is heap-allocated.
  /// @returns `true` if the string lives on heap, and `false` if the string
  /// resides on the stack.
  bool is_heap_allocated() const;

  /// Retrieves a pointer to the underlying character array.
  /// @returns A const pointer to the beginning of the string.
  char const* data() const;

  /// Retrieves the string size.
  /// @returns The number of characters of the string.
  size_type size() const;

  /// Tests whether the string is empty.
  /// @returns `true` iff the string has zero length.
  bool empty() const;

  //
  // Algorithms
  //

  /// Concatenates two strings.
  /// @param x The first string.
  /// @param y The second string.
  /// @param The concatenation of *x* and *y*.
  friend string operator+(string const& x, string const& y);

  /// Concatenates two strings.
  /// @param x The first string.
  /// @param y The second string.
  /// @param The concatenation of *x* and *y*.
  friend string operator+(const char* x, string const& y);

  /// Concatenates two strings.
  /// @param x The first string.
  /// @param y The second string.
  /// @param The concatenation of *x* and *y*.
  friend string operator+(string const& x, char const* y);

  /// Retrieves a substring.
  ///
  /// @param pos The offset where to start.
  ///
  /// @param length The number of characters.
  ///
  /// @returns The substring starting at *pos* of size *length*.
  string substr(size_type pos, size_type length = npos) const;

  /// Substitutes a replacement string for the first occurrence of a
  /// pattern.
  ///
  /// @param pat The pattern to find in this string.
  ///
  /// @param repl The string to substitute for the first occurrence of *pat*.
  ///
  /// @returns A new string with the first occurrence of *pat* replaced with
  /// *repl* or a copy of `*this` if *pat* does not exist.
  string sub(string const& pat, string const& repl) const;

  /// Substitutes a replacement string for the all occurrences of a
  /// pattern.
  ///
  /// @param pat The pattern to find in this string.
  ///
  /// @param repl The string to substitute for each occurrence of *pat*.
  ///
  /// @returns A new string with each occurrence of *pat* replaced with *repl*
  /// or a copy of `*this` if *pat* does not exist.
  string gsub(string const& pat, string const& repl) const;

  /// Splits a string into a vector of iterator pairs representing the
  /// *[start, end)* range of each element.
  ///
  /// @param sep The split to split.
  ///
  /// @param esc The escape string. If *esc* occurrs in front of *sep*, then
  /// *sep* will not count as a separator.
  ///
  /// @param max_splits The maximum number of splits to perform.
  ///
  /// @param include_sep If `true`, also include the separator after each
  /// match.
  ///
  /// @returns A vector of iterator pairs each of which delimit a single field
  /// with a range *[start, end)*.
  ///
  /// @todo Implement regex-based splitting. At this point the parameter
  /// *include_sep* has not much significance.
  std::vector<std::pair<const_iterator, const_iterator>>
  split(string const& sep,
        string const& esc = "",
        int max_splits = -1,
        bool include_sep = false) const;

  /// Determines whether a given string occurs at the beginning of this string.
  ///
  /// @param str The substring to test.
  ///
  /// @returns `true` iff *str* occurs at the beginning of this string.
  bool starts_with(string const& str) const;

  /// Determines whether a given string occurs at the end of this string.
  ///
  /// @param str The substring to test.
  ///
  /// @returns `true` iff *str* occurs at the end of this string.
  bool ends_with(string const& str) const;

  /// Tries to find a substring from a given position.
  ///
  /// @param needle The substring to look for.
  ///
  /// @param pos The position where to start looking.
  ///
  /// @returns If *needle* lies within the string the position of *needle*, and
  /// `string::npos` otherwise.
  size_type find(string const& needle, size_type pos = npos) const;

  /// Tries to find a substring from a given position looking backwards.
  ///
  /// @param needle The substring to look for.
  ///
  /// @param pos The position where to start looking backwards.
  ///
  /// @returns If *needle* lies within the string the position of *needle*, and
  /// `string::npos` otherwise.
  size_type rfind(string const& needle, size_type pos = npos) const;

  /// Trims a string sequence from both ends.
  ///
  /// @param str The string to remove from both ends. This may occur mulitple
  /// times until it is no longer possible ot remove *str* from the ends.
  ///
  /// @returns The trimmed string.
  string trim(string const& str = " ") const;

  /// Trims a string sequences from beginning and end.
  ///
  /// @param left The string to remove from the beginning of the string. This
  /// may occur mulitple times until it is no longer possible ot remove *left*
  /// from the beginng.
  ///
  /// @param right The string to remove from the end of the string. This may
  /// occur mulitple times until it is no longer possible ot remove *right*
  /// from the end.
  ///
  /// @returns The trimmed string.
  string trim(string const& left, string const& right) const;

  /// Trims a string from both ends and removes non-escaped occurrences
  /// inside. For example, invoking `thin("/", "\\")` on the string
  /// `/foo\/bar/baz/` results in the string `foo/barbaz`, whereas
  /// `thin("/")` generates `foo\barbaz`.
  ///
  /// @param str The string to remove from both ends. This may occur mulitple
  /// times until it is no longer possible ot remove *str* from the ends.
  ///
  /// @param esc If non-empty, treated as an escape character. The function
  /// does not thin Escaped occurrences of `str`; it only removes the escape
  /// string itself.
  ///
  /// @returns The thinned string.
  string thin(string const& str, string const& esc = "") const;

  /// Escapes all non-printable characters in the string.
  /// @param all If `true`, escapes every single character in the string.
  /// @returns The escaped string.
  string escape(bool all = false) const;

  /// Unescapes all escaped characters in the string.
  /// @returns The unescaped string.
  string unescape() const;

  /// Checks whether a given iterator points to an escape sequence of the
  /// form `\x##` where `#` is a hexadecimal character.
  ///
  /// @param i The iterator to test.
  ///
  /// @returns `true` if *i* points to an escape sequence.
  bool is_escape_seq(const_iterator i) const;

  //
  // Modifiers
  //

  /// Resets the string to the empty string.
  void clear();

  /// Swaps two strings.
  /// @param x The first string.
  /// @param y The second string.
  friend void swap(string& x, string& y);

  //
  // Tagging
  //

  /// Retrieves the string tag.
  /// @returns The new tag byte.
  uint8_t tag() const;

  /// Sets the string tag.
  /// @param t the new tag.
  void tag(uint8_t t);

private:
  template <typename Iterator>
  void assign(Iterator begin, Iterator end)
  {
    auto str = prepare(std::distance(begin, end));
    std::copy(begin, end, str);
  }

  char* prepare(size_type size);
  char* heap_str();
  char const* heap_str() const;

  char buf_[buf_size];

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(string const& str, Iterator&& out)
  {
    auto esc = str.escape();
    out = std::copy(esc.begin(), esc.end(), out);
    return nothing;
  }

  template <typename Iterator>
  friend trial<void> parse(string& x, Iterator& begin, Iterator end)
  {
    x = string{begin, end}.unescape();
    begin = end;
    return nothing;
  }

  friend bool operator==(string const& x, string const& y);
  friend bool operator<(string const& x, string const& y);
};

} // namespace vast

namespace std {

template <>
struct hash<vast::string>
{
  size_t operator()(vast::string const& str) const
  {
    size_t x = 0;
    for (auto byte : str)
      x ^= static_cast<size_t>(byte) + 0x9e3779b9 + (x << 6) + (x >> 2);
    return x;
  }
};

} // namespace std

#endif
