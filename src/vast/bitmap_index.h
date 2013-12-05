#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/bitmap.h"
#include "vast/container.h"
#include "vast/operator.h"
#include "vast/optional.h"
#include "vast/value.h"
#include "vast/util/dictionary.h"
#include "vast/util/operators.h"

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
  bool push_back(value const& val, uint64_t id = 0);

  /// Appends a sequence of bits.
  /// @param n The number of elements to append.
  /// @param bit The value of the bits to append.
  /// @returns `true` on success.
  virtual bool append(size_t n, bool bit) = 0;

  /// Looks up a value given a relational operator.
  /// @param op The relation operator.
  /// @param val The value to lookup.
  virtual optional<bitstream>
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

protected:
  using super = bitmap_index;

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

/// A bitmap index for arithmetic types.
template <typename Bitstream, value_type T>
class arithmetic_bitmap_index : public bitmap_index
{
  using underlying_value_type = underlying_value_type<T>;
  using bitmap_type =
    typename std::conditional<
      std::is_same<underlying_value_type, bool>::value,
      bitmap<bool, Bitstream>,
      typename std::conditional<
        std::is_same<underlying_value_type, double>::value,
        bitmap<double, Bitstream, range_coder, precision_binner>,
        typename std::conditional<
          std::is_integral<underlying_value_type>::value,
          bitmap<underlying_value_type, Bitstream, range_coder>,
          std::false_type
        >::type
      >::type
    >::type;

public:
  arithmetic_bitmap_index() = default;

  template <
    typename U = underlying_value_type,
    typename = EnableIf<std::is_same<U, double>>
  >
  explicit arithmetic_bitmap_index(int precision)
    : bitmap_({precision}, {})
  {
  }

  virtual bool append(size_t n, bool bit) override
  {
    return bitmap_.append(n, bit);
  }

  virtual optional<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (op == in || op == not_in)
        throw std::runtime_error(
            "unsupported relational operator: " + to<std::string>(op));
    if (bitmap_.empty())
      return {};
    auto result = bitmap_.lookup(op, val.get<underlying_value_type>());
    if (! result)
      return {};
    return {std::move(*result)};
  };

  virtual uint64_t size() const override
  {
    return bitmap_.size();
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    return bitmap_.push_back(val.get<underlying_value_type>());
  }

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<arithmetic_bitmap_index<Bitstream, T> const&>(other);
    return bitmap_ == o.bitmap_;
  }

  bitmap_type bitmap_;

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

/// A bitmap index for time range and time point types.
template <typename Bitstream>
class time_bitmap_index : public bitmap_index
{
public:
  /// Constructs a time bitmap index.
  /// @param precision The granularity of the index. Defaults to seconds.
  explicit time_bitmap_index(int precision = 7)
    : bitmap_({precision}, {})
  {
  }

  virtual bool append(size_t n, bool bit) override
  {
    return bitmap_.append(n, bit);
  }

  virtual optional<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (op == in || op == not_in)
      throw std::runtime_error(
          "unsupported relational operator " + to<std::string>(op));
    if (bitmap_.empty())
      return {};
    auto result = bitmap_.lookup(op, extract(val));
    if (! result)
      return {};
    return {std::move(*result)};
  }

  virtual uint64_t size() const override
  {
    return bitmap_.size();
  }

private:
  static time_range::rep extract(value const& val)
  {
    switch (val.which())
    {
      default:
        throw std::runtime_error("value not a time type");
      case time_range_type:
        return val.get<time_range>().count();
      case time_point_type:
        return val.get<time_point>().since_epoch().count();
    }
  }

  virtual bool push_back_impl(value const& val) override
  {
    bitmap_.push_back(extract(val));
    return true;
  }

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<time_bitmap_index const&>(other);
    return bitmap_ == o.bitmap_;
  }

  bitmap<time_range::rep, Bitstream, range_coder, precision_binner> bitmap_;

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
  using dictionary_codomain = uint64_t;

public:
  virtual bool append(size_t n, bool bit) override
  {
    return bitmap_.append(n, bit);
  }

  virtual optional<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (! (op == equal || op == not_equal))
      throw std::runtime_error("unsupported relational operator " +
                               to<std::string>(op));

    auto i = dictionary_[to<std::string>(val.get<string>())];
    if (! i)
    {
      if (op == not_equal)
        return {bitmap_.valid()};
      else
        return {};
    }

    auto bs = bitmap_[*i];
    if (! bs)
    {
      if (op == not_equal)
        return {bitmap_.valid()};
      else
        return {};
    }

    return {std::move(op == equal ? *bs : bs->flip())};
  }

  virtual uint64_t size() const override
  {
    return bitmap_.size();
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    auto str = to<std::string>(val.get<string>());
    auto i = dictionary_[str];
    if (! i)
      i = dictionary_.insert(str);
    if (! i)
      return false;
    return bitmap_.push_back(*i);
  }

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<string_bitmap_index const&>(other);
    return bitmap_ == o.bitmap_;
  }

  bitmap<dictionary_codomain, Bitstream> bitmap_;
  util::map_dictionary<dictionary_codomain> dictionary_;

private:
  friend access;

  virtual void serialize(serializer& sink) const override
  {
    sink << dictionary_ << bitmap_;
  }

  virtual void deserialize(deserializer& source) override
  {
    source >> dictionary_ >> bitmap_;
    checkpoint();
  }

  virtual bool convert(std::string& str) const override
  {
    using vast::convert;
    return convert(bitmap_, str);
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

  virtual optional<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (! (op == equal || op == not_equal || op == in || op == not_in))
      throw std::runtime_error("unsupported relational operator " +
                               to<std::string>(op));
    if (v4_.empty())
      return {};

    switch (val.which())
    {
      default:
        throw std::runtime_error("invalid value type");
      case address_type:
        return lookup(val.get<address>(), op);
      case prefix_type:
        return lookup(val.get<prefix>(), op);
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
    auto success = v4_.push_back(start == 12);
    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]))
        success = false;
    return success;
  }

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<address_bitmap_index const&>(other);
    return bitmaps_ == o.bitmaps_ && v4_ == o.v4_;
  }

  optional<bitstream> lookup(address const& addr, relational_operator op) const
  {
    auto& bytes = addr.data();
    auto is_v4 = addr.is_v4();
    optional<bitstream> result;
    result = bitstream{is_v4 ? v4_ : Bitstream{v4_.size(), true}};
    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
      if (auto bs = bitmaps_[i][bytes[i]])
        *result &= *bs;
      else if (op == not_equal)
        return bitstream{Bitstream{v4_.size(), true}};
      else
        return {};

    if (op == not_equal)
      result->flip();
    return result;
  }

  optional<bitstream> lookup(prefix const& pfx, relational_operator op) const
  {
    if (! (op == in || op == not_in))
      throw std::runtime_error("unsupported relational operator " +
                               to<std::string>(op));
    auto topk = pfx.length();
    if (topk == 0)
      throw std::runtime_error("invalid IP prefix length");

    auto net = pfx.network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      return lookup(pfx.network(), op == in ? equal : not_equal);

    optional<bitstream> result;
    result = bitstream{is_v4 ? v4_ : Bitstream{v4_.size(), true}};
    auto bit = topk;
    auto& bytes = net.data();
    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
      for (size_t j = 8; j --> 0; )
      {
        if (auto bs = bitmaps_[i].lookup_raw(j))
          *result &= ((bytes[i] >> j) & 1) ? *bs : ~*bs;
        else
          throw std::runtime_error("corrupt index: bit must exist");

        if (! --bit)
        {
          if (op == not_in)
            result->flip();
          return result;
        }
      }

    return {};
  }

  std::array<bitmap<uint8_t, Bitstream, binary_coder>, 16> bitmaps_;
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
      v.emplace_back(*bitmaps_[i / 8].lookup_raw(7 - i % 8));

    auto i = std::back_inserter(str);
    return render(i, v);
  }
};

/// A bitmap index for transport-layer ports.
template <typename Bitstream>
class port_bitmap_index : public bitmap_index
{
  using proto_type = std::underlying_type<port::port_type>::type;

public:
  virtual bool append(size_t n, bool bit) override
  {
    auto success = num_.append(n, bit);
    return proto_.append(n, bit) && success;
  }

  virtual optional<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (op == in || op == not_in)
      throw std::runtime_error("unsupported relational operator " +
                               to<std::string>(op));
    if (num_.empty())
      return {};
    auto& p = val.get<port>();
    auto nbs = num_.lookup(op, p.number());
    if (! nbs)
      return {};
    if (p.type() != port::unknown)
      if (auto tbs = num_[p.type()])
          *nbs &= *tbs;
    return {std::move(*nbs)};
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
    proto_.push_back(static_cast<proto_type>(p.type()));
    return true;
  }

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<port_bitmap_index const&>(other);
    return num_ == o.num_ && proto_ == o.proto_;
  }

  bitmap<uint16_t, Bitstream, range_coder> num_;
  bitmap<proto_type, Bitstream> proto_;

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

/// Factory function to construct a bitmap index for a given value type.
/// @param t The value type to create an index for.
/// @returns A bitmap index suited for type *t*.
template <typename Bitstream>
std::unique_ptr<bitmap_index> make_bitmap_index(value_type t)
{
  switch (t)
  {
    default:
      throw std::runtime_error("unsupported bitmap index type");
    case bool_type:
      return make_unique<arithmetic_bitmap_index<Bitstream, bool_type>>();
    case int_type:
      return make_unique<arithmetic_bitmap_index<Bitstream, int_type>>();
    case uint_type:
      return make_unique<arithmetic_bitmap_index<Bitstream, uint_type>>();
    case double_type:
      return make_unique<arithmetic_bitmap_index<Bitstream, double_type>>();
    case time_range_type:
    case time_point_type:
      return make_unique<time_bitmap_index<Bitstream>>();
    case string_type:
    case regex_type:
      return make_unique<string_bitmap_index<Bitstream>>();
    case address_type:
      return make_unique<address_bitmap_index<Bitstream>>();
    case port_type:
      return make_unique<port_bitmap_index<Bitstream>>();
  }
}

} // namespace vast

#endif
