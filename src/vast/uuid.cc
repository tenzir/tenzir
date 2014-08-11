#include "vast/uuid.h"

#include <random>
#include "vast/logger.h"
#include "vast/serialization.h"

namespace vast {
namespace {

class random_generator
{
  using number_type = unsigned long;
  using distribution = std::uniform_int_distribution<number_type>;

public:
  random_generator()
    : unif_{std::numeric_limits<number_type>::min(),
            std::numeric_limits<number_type>::max()}
  {
  }

  uuid operator()()
  {
    uuid u;
    auto r = unif_(rd_);
    int i = 0;
    for (auto& byte : u)
    {
      if (i == sizeof(number_type))
      {
        r = unif_(rd_);
        i = 0;
      }
      byte = (r >> (i * 8)) & 0xff;
      ++i;
    }

    // Set the variant to 0b10xxxxxx
    *(u.begin() + 8) &= 0xbf;
    *(u.begin() + 8) |= 0x80;

    // Set the version to 0b0100xxxx
    *(u.begin() + 6) &= 0x4f; //0b01001111
    *(u.begin() + 6) |= 0x40; //0b01000000

    return u;
  }

private:
  std::random_device rd_;
  distribution unif_;
};

class string_generator
{
public:
  template <typename Iterator>
  uuid operator()(Iterator begin, Iterator end) const
  {
    auto c = advance_char(begin, end);
    auto braced = false;
    if (c == '{')
    {
      braced = true;
      c = advance_char(begin, end);
    }

    uuid u;
    auto with_dashes = false;
    auto i = 0;
    for (auto& byte : u)
    {
      if (i != 0)
        c = advance_char(begin, end);

      if (i == 4 && c == '-')
      {
        with_dashes = true;
        c = advance_char(begin, end);
      }

      if (with_dashes)
      {
        if (i == 6 || i == 8 || i == 10)
        {
          if (c == '-')
            c = advance_char(begin, end);
          else
            // FIXME: get rid of exception.
            throw std::runtime_error("invalid dashes in UUID string");
        }
      }

      byte = lookup(c);
      c = advance_char(begin, end);
      byte <<= 4;
      byte |= lookup(c);
      ++i;
    }

    if (braced)
    {
      c = advance_char(begin, end);
      if (c == '}')
        // FIXME: get rid of exception.
        throw std::runtime_error("missing closing brace in UUID string");
    }

    return u;
  }

private:
  uint8_t lookup(char c) const
  {
    static constexpr auto digits = "0123456789abcdefABCDEF";
    static constexpr uint8_t values[] = {
      0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,10,11,12,13,14,15, 0xff
    };
    // TODO: use a static table as opposed to searching in the vector.
    return values[std::find(digits, digits + 22, c) - digits];
  }

  template <typename Iterator>
  typename std::iterator_traits<Iterator>::value_type
  advance_char(Iterator& begin, Iterator end) const
  {
    if (begin == end)
        // FIXME: get rid of exception.
      throw std::runtime_error("invalid UUID");
    return *begin++;
  }
};

} // namespace <anonymous>

uuid uuid::random()
{
  return random_generator()();
}

uuid uuid::nil()
{
  uuid u;
  u.id_.fill(0);
  return u;
}

uuid::uuid(std::string const& str)
{
  auto u = string_generator()(str.begin(), str.end());
  swap(u);
}

uuid::iterator uuid::begin()
{
  return id_.begin();
}

uuid::iterator uuid::end()
{
  return id_.end();
}

uuid::const_iterator uuid::begin() const
{
  return id_.begin();
}

uuid::const_iterator uuid::end() const
{
  return id_.end();
}

uuid::size_type uuid::size() const
{
  return 16;
}

void uuid::swap(uuid& other)
{
  std::swap_ranges(begin(), end(), other.begin());
}

void uuid::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink.write_raw(&id_, sizeof(id_));
}

void uuid::deserialize(deserializer& source)
{
  VAST_ENTER();
  source.read_raw(&id_, sizeof(id_));
  VAST_LEAVE(VAST_THIS);
}

bool operator==(uuid const& x, uuid const& y)
{
  return std::equal(x.begin(), x.end(), y.begin());
}

bool operator<(uuid const& x, uuid const& y)
{
  return std::lexicographical_compare(x.begin(), x.end(), y.begin(), y.end());
}

} // namespace vast

