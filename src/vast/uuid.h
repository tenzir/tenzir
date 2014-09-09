#ifndef VAST_UUID_H
#define VAST_UUID_H

#include <array>
#include <functional>
#include "vast/fwd.h"
#include "vast/trial.h"
#include "vast/util/coding.h"
#include "vast/util/operators.h"

namespace vast {

class uuid : util::totally_ordered<uuid>
{
public:
  using value_type = uint8_t;
  using reference = uint8_t&;
  using const_reference = uint8_t const&;
  using iterator = uint8_t*;
  using const_iterator = uint8_t const*;
  using size_type = size_t;

  static uuid random();
  static uuid nil();

  uuid() = default;

  iterator begin();
  iterator end();
  const_iterator begin() const;
  const_iterator end() const;
  size_type size() const;

  void swap(uuid& other);

private:
  std::array<uint8_t, 16> id_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(uuid const& u, Iterator&& out)
  {
    for (size_t i = 0; i < 16; ++i)
    {
      auto& byte = u.id_[i];
      *out++ = util::byte_to_char((byte >> 4) & 0x0f);
      *out++ = util::byte_to_char(byte & 0x0f);
      if (i == 3 || i == 5 || i == 7 || i == 9)
        *out++ = '-';
    }

    return nothing;
  }

  template <typename Iterator>
  friend trial<void> parse(uuid& u, Iterator& begin, Iterator)
  {
    auto c = *begin++;
    auto braced = false;
    if (c == '{')
    {
      braced = true;
      c = *begin++;
    }

    auto with_dashes = false;
    auto i = 0;
    for (auto& byte : u)
    {
      if (i != 0)
        c = *begin++;

      if (i == 4 && c == '-')
      {
        with_dashes = true;
        c = *begin++;
      }

      if (with_dashes)
      {
        if (i == 6 || i == 8 || i == 10)
        {
          if (c == '-')
            c = *begin++;
          else
            return error{"invalid dashes in UUID string"};
        }
      }

      byte = lookup(c);
      c = *begin++;
      byte <<= 4;
      byte |= lookup(c);
      ++i;
    }

    if (braced)
    {
      c = *begin++;
      if (c == '}')
        return error{"missing closing brace in UUID string"};
    }

    return nothing;
  }

  friend bool operator==(uuid const& x, uuid const& y);
  friend bool operator<(uuid const& x, uuid const& y);

  static uint8_t lookup(char c)
  {
    static constexpr auto digits = "0123456789abcdefABCDEF";
    static constexpr uint8_t values[] = {
      0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,10,11,12,13,14,15, 0xff
    };
    // TODO: use a static table as opposed to searching in the vector.
    return values[std::find(digits, digits + 22, c) - digits];
  }
};

} // namespace vast

namespace std {

template <>
struct hash<vast::uuid>
{
  size_t operator()(vast::uuid const& u) const
  {
    size_t x = 0;
    for (auto& byte : u)
      x ^= static_cast<size_t>(byte) + 0x9e3779b9 + (x << 6) + (x >> 2);
    return x;
  }
};

} // namespace std

#endif
