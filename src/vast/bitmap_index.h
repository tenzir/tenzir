#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/bitmap.h"
#include "vast/container.h"
#include "vast/operator.h"
#include "vast/value.h"
#include "vast/util/dictionary.h"
#include "vast/util/operators.h"
#include "vast/util/trial.h"

namespace vast {

/// The abstract base class for bitmap indexes.
class bitmap_index : util::equality_comparable<bitmap_index>
{
public:
  /// Destroys a bitmap index.
  virtual ~bitmap_index() = default;

  /// Appends a single value.
  /// @param val The value to add to the index.
  /// @param id The ID to associate with *val*.
  /// @returns `true` if appending succeeded.
  bool push_back(value const& val, event_id id = 0);

  /// Appends a sequence of bits.
  /// @param n The number of elements to append.
  /// @param bit The value of the bits to append.
  /// @returns `true` on success.
  virtual bool append(size_t n, bool bit) = 0;

  /// Looks up a value given a relational operator.
  /// @param op The relation operator.
  /// @param val The value to lookup.
  virtual trial<bitstream>
  lookup(relational_operator op, value const& val) const = 0;

  /// Retrieves the number of elements in the bitmap index.
  /// @returns The number of rows, i.e., values in the bitmap.
  virtual uint64_t size() const = 0;

  /// Checks whether the bitmap is empty.
  /// @returns `true` if `size() == 0`.
  bool empty() const;

  /// Retrieves the number of bits appended since the last call to ::checkpoint.
  /// @returns Then number of bits since the last checkpoint.
  uint64_t appended() const;

  /// Performs a checkpoint of the number of bits appended.
  void checkpoint();

private:
  virtual bool push_back_impl(value const& val) = 0;
  virtual bool equals(bitmap_index const& other) const = 0;

  uint64_t checkpoint_size_ = 0;

private:
  friend access;

  virtual void serialize(serializer& sink) const = 0;
  virtual void deserialize(deserializer& source) = 0;
  virtual bool convert(std::string& str) const = 0;

  friend bool operator==(bitmap_index const& x, bitmap_index const& y);
};

/// A bitmap index for arithmetic value types.
template <typename Bitstream, value_type T>
class arithmetic_bitmap_index : public bitmap_index
{
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

  virtual bool append(size_t n, bool bit) override
  {
    return bitmap_.append(n, bit);
  }

  virtual trial<bitstream>
  lookup(relational_operator op, value const& val) const override
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

  virtual uint64_t size() const override
  {
    return bitmap_.size();
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

  virtual bool push_back_impl(value const& val) override
  {
    return bitmap_.push_back(extract(val));
  }

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<arithmetic_bitmap_index<Bitstream, T> const&>(other);
    return bitmap_ == o.bitmap_;
  }

  bitmap<bitmap_type, Bitstream, bitmap_coder, bitmap_binner> bitmap_;

private:
  friend access;

  virtual void serialize(serializer& sink) const override
  {
    sink << bitmap_;
  }

  virtual void deserialize(deserializer& source) override
  {
    source >> bitmap_;
    checkpoint();
  }

  virtual bool convert(std::string& str) const override
  {
    using vast::convert;
    return convert(bitmap_, str);
  }
};

/// A bitmap index for strings. It uses a @link dictionary
/// vast::util::dictionary@endlink to map each string to a unique numeric value
/// to be used by the bitmap.
template <typename Bitstream>
class string_bitmap_index : public bitmap_index
{
  static uint8_t byte_at(string const& s, size_t i)
  {
    return static_cast<uint8_t>(s[i]);
  }

public:
  virtual bool append(size_t n, bool bit) override
  {
    for (auto& bm : bitmaps_)
      if (! bm.append(n, bit))
        return false;

    return size_.append(n, bit);
  }

  virtual trial<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
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

  virtual uint64_t size() const override
  {
    return size_.size();
  }

private:
  virtual bool push_back_impl(value const& val) override
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

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<string_bitmap_index const&>(other);
    return bitmaps_ == o.bitmaps_;
  }

  std::vector<bitmap<uint8_t, Bitstream, binary_bitslice_coder>> bitmaps_;
  bitmap<string::size_type, Bitstream, range_bitslice_coder> size_;

private:
  friend access;

  virtual void serialize(serializer& sink) const override
  {
    sink << size_ << bitmaps_;
  }

  virtual void deserialize(deserializer& source) override
  {
    source >> size_ >> bitmaps_;
    checkpoint();
  }

  virtual bool convert(std::string& str) const override
  {
    str = "not yet implemented"; // TODO
    return false;
  }
};

/// A bitmap index for IP addresses.
template <typename Bitstream>
class address_bitmap_index : public bitmap_index
{
public:
  virtual bool append(size_t n, bool bit) override
  {
    bool success = true;
    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].append(n, bit))
        success = false;
    return v4_.append(n, bit) && success;
  }

  virtual trial<bitstream>
  lookup(relational_operator op, value const& val) const override
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

  virtual uint64_t size() const override
  {
    return v4_.size();
  }

private:
  virtual bool push_back_impl(value const& val) override
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

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<address_bitmap_index const&>(other);
    return bitmaps_ == o.bitmaps_ && v4_ == o.v4_;
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

  std::array<bitmap<uint8_t, Bitstream, binary_bitslice_coder>, 16> bitmaps_;
  Bitstream v4_;

private:
  friend access;

  virtual void serialize(serializer& sink) const override
  {
    sink << bitmaps_ << v4_;
  }

  virtual void deserialize(deserializer& source) override
  {
    source >> bitmaps_ >> v4_;
    checkpoint();
  }

  virtual bool convert(std::string& str) const override
  {
    std::vector<Bitstream> v;
    v.reserve(128);
    for (size_t i = 0; i < 128; ++i)
      v.emplace_back(bitmaps_[i / 8].coder().get(7 - i % 8));

    auto i = std::back_inserter(str);
    return render(i, v);
  }
};

/// A bitmap index for transport-layer ports.
template <typename Bitstream>
class port_bitmap_index : public bitmap_index
{
public:
  virtual bool append(size_t n, bool bit) override
  {
    auto success = num_.append(n, bit);
    return proto_.append(n, bit) && success;
  }

  virtual trial<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
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

  virtual uint64_t size() const override
  {
    return proto_.size();
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    auto& p = val.get<port>();
    num_.push_back(p.number());
    proto_.push_back(p.type());
    return true;
  }

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<port_bitmap_index const&>(other);
    return num_ == o.num_ && proto_ == o.proto_;
  }

  bitmap<port::number_type, Bitstream, range_bitslice_coder> num_;
  bitmap<std::underlying_type<port::port_type>::type, Bitstream> proto_;

private:
  friend access;

  virtual void serialize(serializer& sink) const override
  {
    sink << num_ << proto_;
  }

  virtual void deserialize(deserializer& source) override
  {
    source >> num_ >> proto_;
    checkpoint();
  }

  virtual bool convert(std::string& str) const override
  {
    using vast::convert;
    return convert(num_, str);
  }
};

/// Factory function to construct a bitmap index from a given value type.
///
/// @param t The value type to create an index for.
///
/// @param args The arguments to forward to the corresponding bitmap index
/// constructor.
///
/// @returns A bitmap index for type *t*.
template <typename Bitstream, typename... Args>
trial<std::unique_ptr<bitmap_index>>
make_bitmap_index(value_type t, Args&&... args)
{
  std::unique_ptr<bitmap_index> bmi;
  switch (t)
  {
    default:
      return error{"unsupported bitmap index type: " + to_string(t)};
    case bool_type:
      bmi = make_unique<arithmetic_bitmap_index<Bitstream, bool_type>>(
          std::forward<Args>(args)...);
      break;
    case int_type:
      bmi = make_unique<arithmetic_bitmap_index<Bitstream, int_type>>(
          std::forward<Args>(args)...);
      break;
    case uint_type:
      bmi = make_unique<arithmetic_bitmap_index<Bitstream, uint_type>>(
          std::forward<Args>(args)...);
      break;
    case double_type:
      bmi = make_unique<arithmetic_bitmap_index<Bitstream, double_type>>(
          std::forward<Args>(args)...);
      break;
    case time_range_type:
      bmi = make_unique<arithmetic_bitmap_index<Bitstream, time_range_type>>(
          std::forward<Args>(args)...);
      break;
    case time_point_type:
      bmi = make_unique<arithmetic_bitmap_index<Bitstream, time_point_type>>(
          std::forward<Args>(args)...);
      break;
    case string_type:
    case regex_type:
      bmi = make_unique<string_bitmap_index<Bitstream>>(
          std::forward<Args>(args)...);
      break;
    case address_type:
      bmi = make_unique<address_bitmap_index<Bitstream>>(
          std::forward<Args>(args)...);
      break;
    case port_type:
      bmi = make_unique<port_bitmap_index<Bitstream>>(
          std::forward<Args>(args)...);
      break;
  }

  return std::move(bmi);
}

} // namespace vast

#endif
