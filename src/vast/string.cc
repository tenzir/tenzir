#include "vast/string.h"

#include <cstring>
#include "vast/logger.h"
#include "vast/io/serialization.h"

namespace vast {

static char const hex[] = "0123456789abcdef";

string::size_type const string::npos;

string::string()
{
  std::memset(buf_, 0, buf_size);
}

string::string(char const* str)
  : string(str, str + std::strlen(str))
{
}

string::string(char const* str, size_type size)
  : string()
{
  if (size > 0)
    assign(str, str + size);
}

string::string(std::string const& str)
  : string(str.data(), str.size())
{
}

string::string(string const& other)
  : string()
{
  assign(other.data(), other.data() + other.size());
}

string::string(string&& other)
{
  std::memcpy(buf_, other.buf_, buf_size);
  std::memset(other.buf_, 0, buf_size);
}

string::~string()
{
  if (is_heap_allocated())
    delete[] heap_str();
}

string& string::operator=(string other)
{
  using std::swap;
  swap(*this, other);
  return *this;
}

char string::operator[](size_type i) const
{
  return *(data() + i);
}

string::const_iterator string::begin() const
{
  return data();
}

string::const_iterator string::cbegin() const
{
  return begin();
}

string::const_iterator string::end() const
{
  return data() + size();
}

string::const_iterator string::cend() const
{
  return end();
}

char string::front() const
{
  return *begin();
}

char string::back() const
{
  return *(begin() + size() - 1);
}

bool string::is_heap_allocated() const
{
  return buf_[tag_off] & 1;
}

char const* string::data() const
{
  return is_heap_allocated() ? heap_str() : buf_;
}

string::size_type string::size() const
{
  if (is_heap_allocated())
  {
    auto size = reinterpret_cast<size_type const*>(&buf_[cnt_off]);
    return *size;
  }
  else
    return buf_[in_situ_len];
}

bool string::empty() const
{
  return size() == 0;
}

string operator+(string const& x, string const& y)
{
  string concat;
  auto data = concat.prepare(x.size() + y.size());
  std::copy(x.begin(), x.end(), data);
  std::copy(y.begin(), y.end(), data + x.size());
  return concat;
}

string operator+(char const* x, string const& y)
{
  string concat;
  auto xlen = std::strlen(x);
  auto data = concat.prepare(xlen + y.size());
  std::copy(x, x + xlen, data);
  std::copy(y.begin(), y.end(), data + xlen);
  return concat;
}

string operator+(string const& x, char const* y)
{
  string concat;
  auto ylen = std::strlen(y);
  auto data = concat.prepare(x.size() + ylen);
  std::copy(x.begin(), x.end(), data);
  std::copy(y, y + ylen, data + x.size());
  return concat;
}

string string::substr(size_type pos, size_type length) const
{
  if (empty() || pos >= size())
    return "";
  return {begin() + pos, begin() + pos + std::min(length, size() - pos)};
}

std::vector<std::pair<string::const_iterator, string::const_iterator>>
string::split(string const& sep,
              string const& esc,
              int max_splits,
              bool include_sep) const
{
  assert(! sep.empty());
  std::vector<std::pair<const_iterator, const_iterator>> pos;
  int splits = 0;
  auto i = begin();
  auto prev = i;
  while (i != end())
  {
    // Find a separator that fits in the string.
    if (*i != sep[0] || i + sep.size() > end())
    {
      ++i;
      continue;
    }
    // Check remaining separator characters.
    size_type j = 1;
    auto s = i;
    while (j < sep.size())
      if (*++s != sep[j])
        break;
      else
        ++j;
    // No separator match.
    if (j != sep.size())
    {
      ++i;
      continue;
    }
    // Make sure it's not an escaped match.
    if (! esc.empty() && esc.size() < static_cast<size_type>(i - begin()))
    {
      auto escaped = true;
      auto esc_start = i - esc.size();
      for (size_type j = 0; j < esc.size(); ++j)
        if (esc_start[j] != esc[j])
        {
          escaped = false;
          break;
        }
      if (escaped)
      {
        ++i;
        continue;
      }
    }

    if (++splits == max_splits)
      break;

    pos.emplace_back(prev, i);
    if (include_sep)
      pos.emplace_back(i, i + sep.size());

    i += sep.size();
    prev = i;
  }

  if (prev != end())
    pos.emplace_back(prev, end());

  return pos;
}

bool string::starts_with(string const& str) const
{
  if (str.size() > size())
    return false;

  auto s = begin();
  auto t = str.begin();
  while (t != str.end())
    if (*s++ != *t++)
      return false;

  return true;
}

bool string::ends_with(string const& str) const
{
  if (str.size() > size())
    return false;

  auto s = end() - str.size();
  auto t = str.begin();
  while (t != str.end())
    if (*s++ != *t++)
      return false;

  return true;
}

string::size_type string::find(string const& needle, size_type pos) const
{
  if (pos == npos)
    pos = 0;
  if (empty() || needle.empty() || pos + needle.size() > size())
    return npos;

  const_iterator sub, cand;
  while (pos < size())
  {
    sub = needle.begin();
    cand = begin() + pos;
    while (*sub == *cand && sub != needle.end())
    {
      ++sub;
      ++cand;
    }
    if (sub == needle.end())
      return pos;
    ++pos;
  }
  return npos;
}

string::size_type string::rfind(string const& needle, size_type pos) const
{
  if (pos == npos)
    pos = size();
  if (empty() || needle.empty() || needle.size() > pos || pos == 0 || pos > size())
    return npos;

  const_iterator rsub, rcand;
  while (pos)
  {
    rsub = needle.end() - 1;
    rcand = begin() + pos - 1;
    while (*rsub == *rcand && rsub != needle.begin())
    {
      --rsub;
      --rcand;
    }
    if (*rsub == *rcand && rsub == needle.begin())
      return pos - needle.size();
    --pos;
  }
  return npos;
}

string string::trim(string const& str) const
{
  return trim(str, str);
}

string string::trim(string const& left, string const& right) const
{
  string s;
  auto front = begin();
  auto back = end();
  auto l = left.size();
  auto r = right.size();
  while (front + l < back && std::equal(left.begin(), left.end(), front))
    front += l;

  while (front + r < back && std::equal(right.begin(), right.end(), back - r))
    back -= r;

  return {front, back};
}

string string::thin(string const& str, string const& esc) const
{
  string skinny;
  auto pos = split(str, "", -1, true);
  decltype(pos) thin_pos;
  size_type new_size = 0;
  for (size_type i = 0; i < pos.size(); i += 2)
  {
    auto start = pos[i].first;
    auto end = pos[i].second;
    if (start == end)
      continue;

    string s{start, end};
    if (! esc.empty() && s.ends_with(esc))
      end -= esc.size();

    thin_pos.emplace_back(start, end);
    new_size += end - start;

    // Add separator if it was escaped.
    if (end != pos[i].second)
    {
      thin_pos.emplace_back(pos[i + 1]);
      new_size += pos[i + 1].second - pos[i + 1].first;
    }
  }

  auto data = skinny.prepare(new_size);
  for (auto p : thin_pos)
  {
    std::copy(p.first, p.second, data);
    data += p.second - p.first;
  }

  return skinny;
}

string string::escape(bool all) const
{
  if (empty())
    return *this;

  std::vector<const_iterator> positions;
  for (auto i = begin(); i != end(); ++i)
    if (all || ! std::isprint(*i) || is_escape_seq(i))
      positions.push_back(i);

  string esc;
  auto new_size = (size() - positions.size()) + positions.size() * 4;
  auto data = esc.prepare(new_size);
  const_iterator prev = begin();
  for (auto pos : positions)
  {
    std::copy(prev, pos, data);
    data += pos - prev;
    *data++ = '\\';
    *data++ = 'x';
    *data++ = hex[(*pos & 0xf0) >> 4];
    *data++ = hex[*pos & 0x0f];
    prev = pos + 1;
  }
  std::copy(prev, end(), data);

  return esc;
}

string string::unescape() const
{
  std::vector<const_iterator> positions;
  for (auto i = begin(); i != end(); ++i)
  {
    if (is_escape_seq(i))
    {
      positions.push_back(i);
      i += 3;
    }
  }

  if (positions.empty())
    return *this;

  string unesc;
  auto new_size = (size() + positions.size()) - positions.size() * 4;
  auto data = unesc.prepare(new_size);
  const_iterator prev = begin();
  for (auto pos : positions)
  {
    std::copy(prev, pos, data);
    data += pos - prev;
    char h = *(pos + 2);
    char l = *(pos + 3);
    char byte = (h > '9' ? h - 'a' + 10 : h - '0') << 4;
    byte |= (l > '9' ? l - 'a' + 10 : l - '0');
    *data++ = byte;
    prev = pos + 4;
  }
  std::copy(prev, end(), data);

  return unesc;
}

bool string::is_escape_seq(const_iterator i) const
{
  return end() - i > 3 &&
    *i == '\\' &&
    *(i + 1) == 'x' &&
    std::isxdigit(*(i + 2)) &&
    std::isxdigit(*(i + 3));
}

void string::clear()
{
  if (is_heap_allocated())
    delete heap_str();

  std::memset(buf_, 0, buf_size);
}

void swap(string& x, string& y)
{
  using std::swap;
  swap(x.buf_, y.buf_);
}

char string::tag() const
{
  return buf_[tag_off] >> 1;
}

void string::tag(char t)
{
  buf_[tag_off] = (t << 1) | (buf_[tag_off] & 1);
}

void string::serialize(io::serializer& sink)
{
  VAST_ENTER(VAST_THIS);
  sink.begin_sequence(size());
  if (! empty())
    sink.write_raw(data(), size());
  sink.end_sequence();
}

void string::deserialize(io::deserializer& source)
{
  VAST_ENTER();
  uint64_t size;
  source.begin_sequence(size);
  if (size > 0)
  {
    if (size > std::numeric_limits<size_type>::max())
      throw error::io("size too large for architecture");
    auto data = prepare(size);
    source.read_raw(data, size);
  }
  source.end_sequence();
  VAST_LEAVE(VAST_THIS);
}

char* string::prepare(size_type size)
{
  char* str;
  if (size > in_situ_len)
  {
    str = new char[size];
    auto p = reinterpret_cast<char**>(&buf_[str_off]);
    *p = str;
    auto q = reinterpret_cast<size_type*>(&buf_[cnt_off]);
    *q = size;
    buf_[tag_off] |= 0x1;
  }
  else
  {
    str = buf_;
    buf_[in_situ_len] = static_cast<char>(size);
    buf_[tag_off] &= ~0x1;
  }

  return str;
}

char* string::heap_str()
{
  auto str = reinterpret_cast<char**>(&buf_[str_off]);
  return *str;
}

char const* string::heap_str() const
{
  auto str = reinterpret_cast<char const* const*>(&buf_[str_off]);
  return *str;
}

bool operator==(string const& x, string const& y)
{
  if (x.size() != y.size())
    return false;

  return std::equal(x.begin(), x.end(), y.begin());
}

bool operator<(string const& x, string const& y)
{
  return std::lexicographical_compare(
      x.begin(), x.end(),
      y.begin(), y.end());
}

std::string to_string(string const& str)
{
  return {str.begin(), str.end()};
}

std::ostream& operator<<(std::ostream& out, string const& str)
{
  auto esc = str.escape();
  //    out << '"';
  std::copy(esc.begin(), esc.end(), std::ostreambuf_iterator<char>(out));
  //    out << '"';
  return out;
}

} // namespace vast
