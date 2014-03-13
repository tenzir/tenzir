#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/bitmap.h"
#include "vast/container.h"
#include "vast/operator.h"
#include "vast/logger.h"
#include "vast/value.h"
#include "vast/util/dictionary.h"
#include "vast/util/operators.h"
#include "vast/util/trial.h"

namespace vast {

/// The base class for bitmap indexes.
template <typename Derived>
class bitmap_index : util::equality_comparable<bitmap_index<Derived>>
{
public:
  /// Appends a single value.
  /// @param val The value to add to the index.
  /// @param id The ID to associate with *val*.
  /// @returns `true` if appending succeeded.
  bool push_back(value const& val, event_id id = 0)
  {
    if (id > 0)
    {
      if (id < size())
      {
        VAST_LOG_ERROR("got value " << val << " incompatible ID " << id <<
                       " (required: ID > " << size() << ')');
        return false;
      }

      auto delta = id - size();
      if (delta > 1)
        if (! append(delta, false))
          return false;
    }

    return val ? derived()->push_back_impl(val) : append(1, false);
  }

  /// Appends a sequence of bits.
  /// @param n The number of elements to append.
  /// @param bit The value of the bits to append.
  /// @returns `true` on success.
  bool append(size_t n, bool bit)
  {
    return derived()->append_impl(n, bit);
  }

  /// Looks up a value given a relational operator.
  /// @param op The relation operator.
  /// @param val The value to lookup.
  trial<bitstream> lookup(relational_operator op, value const& val) const
  {
    return derived()->lookup_impl(op, val);
  }

  /// Retrieves the number of elements in the bitmap index.
  /// @returns The number of rows, i.e., values in the bitmap.
  uint64_t size() const
  {
    return derived()->size_impl();
  }

  /// Checks whether the bitmap is empty.
  /// @returns `true` if `size() == 0`.
  bool empty() const
  {
    return size() == 0;
  }

  /// Retrieves the number of bits appended since the last call to ::checkpoint.
  /// @returns Then number of bits since the last checkpoint.
  uint64_t appended() const
  {
    return size() - checkpoint_size_;
  }

  /// Performs a checkpoint of the number of bits appended.
  void checkpoint()
  {
    checkpoint_size_ = size();
  }

private:
  uint64_t checkpoint_size_ = 0;

private:
  friend access;

  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }

  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }

  void serialize(serializer& sink) const
  {
    sink << checkpoint_size_;
  }

  void deserialize(deserializer& source)
  {
    source >> checkpoint_size_;
  }

  friend bool operator==(bitmap_index const& x, bitmap_index const& y)
  {
    return x.checkpoint_size_ == y.checkpoint_size_;
  }
};

/// A bitmap index for arithmetic value types.
template <typename Bitstream, value_type T>
class arithmetic_bitmap_index
  : public bitmap_index<arithmetic_bitmap_index<Bitstream, T>>,
    util::equality_comparable<arithmetic_bitmap_index<Bitstream, T>>
{
  using super = bitmap_index<arithmetic_bitmap_index<Bitstream, T>>;
  friend super;

  using underlying_value_type = underlying_value_type<T>;

  using bitmap_type =
    typename std::conditional<
      T == time_range_type || T == time_point_type,
      time_range::rep,
      typename std::conditional<
        T == bool_type || T == int_type || T == uint_type || T == double_type,
        underlying_value_type,
        std::false_type
      >::type
    >::type;

  template <typename U>
  using bitmap_binner =
    typename std::conditional<
      T == double_type || T == time_range_type || T == time_point_type,
      precision_binner<U>,
      null_binner<U>
    >::type;

  template <typename B, typename U>
  using bitmap_coder =
    typename std::conditional<
      T == bool_type,
      equality_coder<B, U>,
      typename std::conditional<
        std::is_arithmetic<bitmap_type>::value,
        range_bitslice_coder<B, U>,
        std::false_type
      >::type
    >::type;

public:
  arithmetic_bitmap_index() = default;

  template <
    typename U = bitmap_type,
    typename... Args,
    typename = EnableIf<
      std::is_same<bitmap_binner<U>, precision_binner<U>>>
  >
  explicit arithmetic_bitmap_index(Args&&... args)
    : bitmap_{{std::forward<Args>(args)...}}
  {
  }

private:
  static bitmap_type extract(value const& val)
  {
    switch (val.which())
    {
      default:
        return val.get<bitmap_type>();
      case time_range_type:
        return val.get<time_range>().count();
      case time_point_type:
        return val.get<time_point>().since_epoch().count();
    }
  }

  bool push_back_impl(value const& val)
  {
    return bitmap_.push_back(extract(val));
  }

  bool append_impl(size_t n, bool bit)
  {
    return bitmap_.append(n, bit);
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (op == in || op == not_in)
      return error{"unsupported relational operator: " + to<std::string>(op)};

    if (bitmap_.empty())
      return {Bitstream{}};

    auto r = bitmap_.lookup(op, extract(val));
    if (r)
      return {std::move(*r)};
    else
      return r.failure();
  };

  uint64_t size_impl() const
  {
    return bitmap_.size();
  }

  bitmap<bitmap_type, Bitstream, bitmap_coder, bitmap_binner> bitmap_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << bitmap_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> bitmap_;
  }

  friend bool operator==(arithmetic_bitmap_index const& x,
                         arithmetic_bitmap_index const& y)
  {
    return static_cast<super const&>(x) == static_cast<super const&>(y)
        && x.bitmap_ == y.bitmap_;
  }
};

/// A bitmap index for strings. It uses a @link dictionary
/// vast::util::dictionary@endlink to map each string to a unique numeric value
/// to be used by the bitmap.
template <typename Bitstream>
class string_bitmap_index : public bitmap_index<string_bitmap_index<Bitstream>>
{
  using super = bitmap_index<string_bitmap_index<Bitstream>>;
  friend super;

public:
  string_bitmap_index() = default;
  using super::size;

private:
  static uint8_t byte_at(string const& s, size_t i)
  {
    return static_cast<uint8_t>(s[i]);
  }

  bool push_back_impl(value const& val)
  {
    auto& str = val.get<string>();

    if (! size_.push_back(str.size()))
      return false;

    if (str.empty())
    {
      for (auto& bm : bitmaps_)
        if (! bm.append(1, 0))
          return false;

      return true;
    }

    if (str.size() > bitmaps_.size())
    {
      auto current = size() - 1;
      auto fresh = str.size() - bitmaps_.size();
      bitmaps_.resize(str.size());
      if (current > 0)
        for (size_t i = bitmaps_.size() - fresh; i < bitmaps_.size(); ++i)
          if (! bitmaps_[i].append(current, 0))
            return false;
    }

    for (size_t i = 0; i < str.size(); ++i)
      if (! bitmaps_[i].push_back(byte_at(str, i)))
        return false;

    for (size_t i = str.size(); i < bitmaps_.size(); ++i)
      if (! bitmaps_[i].append(1, 0))
        return false;

    return true;
  }

  bool append_impl(size_t n, bool bit)
  {
    for (auto& bm : bitmaps_)
      if (! bm.append(n, bit))
        return false;

    return size_.append(n, bit);
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    assert(val.which() == string_type);

    auto str = val.get<string>();

    switch (op)
    {
      default:
        return error{"unsupported relational operator " + to<std::string>(op)};
      case equal:
      case not_equal:
        {
          if (str.empty())
          {
            if (auto s = size_.lookup(equal, 0))
              return {std::move(op == equal ? *s : s->flip())};
            else
              return s.failure();
          }

          if (str.size() > bitmaps_.size())
            return {Bitstream{size(), op == not_equal}};

          auto r = size_.lookup(less_equal, str.size());
          if (! r)
            return r.failure();

          if (r->find_first() == Bitstream::npos)
            return {Bitstream{size(), op == not_equal}};

          for (size_t i = 0; i < str.size(); ++i)
          {
            auto b = bitmaps_[i].lookup(equal, byte_at(str, i));
            if (! b)
              return b.failure();

            if (b->find_first() != Bitstream::npos)
              *r &= *b;
            else
              return {Bitstream{size(), op == not_equal}};
          }

          return {std::move(op == equal ? *r : r->flip())};
        }
      case ni:
      case not_ni:
        {
          if (str.empty())
            return {Bitstream{size(), op == ni}};

          if (str.size() > bitmaps_.size())
            return {Bitstream{size(), op == not_ni}};

          // Iterate through all k-grams.
          Bitstream r{size(), 0};
          for (size_t i = 0; i < bitmaps_.size() - str.size() + 1; ++i)
          {
            Bitstream substr{size(), 1};;
            auto skip = false;
            for (size_t j = 0; j < str.size(); ++j)
            {
              auto bs = bitmaps_[i + j].lookup(equal, str[j]);
              if (! bs)
                return bs.failure();

              if (bs->find_first() == Bitstream::npos)
              {
                skip = true;
                break;
              }

              substr &= *bs;
            }

            if (! skip)
              r |= substr;
          }

          return {std::move(op == ni ? r : r.flip())};
        }
    }
  }

  uint64_t size_impl() const
  {
    return size_.size();
  }

  std::vector<bitmap<uint8_t, Bitstream, binary_bitslice_coder>> bitmaps_;
  bitmap<string::size_type, Bitstream, range_bitslice_coder> size_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << size_ << bitmaps_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> size_ >> bitmaps_;
  }

  friend bool operator==(string_bitmap_index const& x,
                         string_bitmap_index const& y)
  {
    return static_cast<super const&>(x) == static_cast<super const&>(y)
        && x.bitmaps_ == y.bitmaps_;
  }
};

/// A bitmap index for IP addresses.
template <typename Bitstream>
class address_bitmap_index
  : public bitmap_index<address_bitmap_index<Bitstream>>
{
  using super = bitmap_index<address_bitmap_index<Bitstream>>;
  friend super;

public:
  address_bitmap_index() = default;
  using super::size;

private:
  bool push_back_impl(value const& val)
  {
    auto& addr = val.get<address>();
    auto& bytes = addr.data();
    size_t const start = addr.is_v4() ? 12 : 0;

    if (! v4_.push_back(start == 12))
      return false;

    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]))
        return false;

    return true;
  }

  bool append_impl(size_t n, bool bit)
  {
    bool success = true;
    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].append(n, bit))
        success = false;
    return v4_.append(n, bit) && success;
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (! (op == equal || op == not_equal || op == in || op == not_in))
      return error{"unsupported relational operator " + to<std::string>(op)};

    if (v4_.empty())
      return {Bitstream{}};

    switch (val.which())
    {
      default:
        return error{"invalid value type"};
      case address_type:
        return lookup_impl(op, val.get<address>());
      case prefix_type:
        return lookup_impl(op, val.get<prefix>());
    }
  }

  trial<bitstream> lookup_impl(relational_operator op, address const& addr) const
  {
    auto& bytes = addr.data();
    auto is_v4 = addr.is_v4();
    auto r = is_v4 ? v4_ : Bitstream{size(), true};

    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
    {
      auto bs = bitmaps_[i][bytes[i]];
      if (! bs)
        return bs.failure();

      if (bs->find_first() != Bitstream::npos)
        r &= *bs;
      else
        return {Bitstream{size(), op == not_equal}};
    }

    return {std::move(op == equal ? r : r.flip())};
  }

  trial<bitstream> lookup_impl(relational_operator op, prefix const& pfx) const
  {
    if (! (op == in || op == not_in))
      return error{"unsupported relational operator " + to<std::string>(op)};

    auto topk = pfx.length();
    if (topk == 0)
      return error{"invalid IP prefix length: " + to<std::string>(topk)};

    auto net = pfx.network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      return lookup_impl(op == in ? equal : not_equal, pfx.network());

    auto r = is_v4 ? v4_ : Bitstream{size(), true};
    auto bit = topk;
    auto& bytes = net.data();
    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
      for (size_t j = 8; j --> 0; )
      {
        auto& bs = bitmaps_[i].coder().get(j);
        r &= ((bytes[i] >> j) & 1) ? bs : ~bs;

        if (! --bit)
        {
          if (op == not_in)
            r.flip();
          return {std::move(r)};
        }
      }

    return {Bitstream{size(), false}};
  }

  uint64_t size_impl() const
  {
    return v4_.size();
  }

  std::array<bitmap<uint8_t, Bitstream, binary_bitslice_coder>, 16> bitmaps_;
  Bitstream v4_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << v4_ << bitmaps_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> v4_ >> bitmaps_;
  }

  friend bool operator==(address_bitmap_index const& x,
                         address_bitmap_index const& y)
  {
    return static_cast<super const&>(x) == static_cast<super const&>(y)
        && x.bitmaps_ == y.bitmaps_;
  }
};

/// A bitmap index for transport-layer ports.
template <typename Bitstream>
class port_bitmap_index : public bitmap_index<port_bitmap_index<Bitstream>>
{
  using super = bitmap_index<port_bitmap_index<Bitstream>>;
  friend super;

public:
  port_bitmap_index() = default;
  using super::size;

private:
  bool push_back_impl(value const& val)
  {
    auto& p = val.get<port>();
    return num_.push_back(p.number()) && proto_.push_back(p.type());
  }

  bool append_impl(size_t n, bool bit)
  {
    return num_.append(n, bit) && proto_.append(n, bit);
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    assert(val.which() == port_type);

    if (op == in || op == not_in)
      return error{"unsupported relational operator " + to<std::string>(op)};

    if (num_.empty())
      return {Bitstream{size(), false}};

    auto& p = val.get<port>();
    auto n = num_.lookup(op, p.number());
    if (! n)
      return n.failure();

    if (n->find_first() == Bitstream::npos)
      return {Bitstream(size(), false)};

    if (p.type() != port::unknown)
    {
      auto t = proto_[p.type()];
      if (! t)
        return t.failure();

      *n &= *t;
    }

    return {std::move(*n)};
  }

  uint64_t size_impl() const
  {
    return proto_.size();
  }

  bitmap<port::number_type, Bitstream, range_bitslice_coder> num_;
  bitmap<std::underlying_type<port::port_type>::type, Bitstream> proto_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << num_ << proto_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> num_ >> proto_;
  }

  friend bool operator==(port_bitmap_index const& x, port_bitmap_index const& y)
  {
    return static_cast<super const&>(x) == static_cast<super const&>(y)
        && x.num_ == y.num_
        && x.proto_ == y.proto_;
  }
};

} // namespace vast

#endif
