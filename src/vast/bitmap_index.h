#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/bitmap.h"
#include "vast/operator.h"
#include "vast/logger.h"
#include "vast/value.h"
#include "vast/util/dictionary.h"
#include "vast/util/operators.h"
#include "vast/util/trial.h"

namespace vast {

/// The base class for bitmap indexes.
template <typename Derived>
class bitmap_index_base : util::equality_comparable<bitmap_index_base<Derived>>
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
        if (! stretch(delta))
          return false;
    }

    // TODO: add explicit indexing support for empty values by putting them
    // into a separate bitmap, as opposed to stretching the bitmap index.
    return val ? derived()->push_back_impl(val) : stretch(1);
  }

  /// Appends 0-bits to the index.
  /// @param n The number of zeros to append.
  /// @returns `true` on success.
  bool stretch(size_t n)
  {
    return derived()->stretch_impl(n);
  }

  /// Looks up a value given a relational operator.
  /// @param op The relation operator.
  /// @param val The value to lookup.
  template <typename Hack = Derived>
  auto lookup(relational_operator op, value const& val) const
    -> decltype(std::declval<Hack>().lookup_impl(op, val))
  {
    static_assert(std::is_same<Hack, Derived>(), ":-P");

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
};

namespace detail {

/// The concept for bitmap indexes.
template <typename Bitstream>
struct bitmap_index_concept
{
  bitmap_index_concept() = default;
  virtual ~bitmap_index_concept() = default;

  virtual bool push_back_impl(value const& val) = 0;
  virtual bool stretch_impl(size_t n) = 0;
  virtual trial<Bitstream> lookup_impl(relational_operator op,
                                       value const& val) const = 0;
  virtual uint64_t size_impl() const = 0;

  virtual std::unique_ptr<bitmap_index_concept> copy() const = 0;
  virtual bool equals(bitmap_index_concept const& other) const = 0;
  virtual void serialize(serializer& sink) const = 0;
  virtual void deserialize(deserializer& source) = 0;
};

/// A concrete bitmap index model.
template <typename BitmapIndex>
struct bitmap_index_model
  : bitmap_index_concept<typename BitmapIndex::bitstream_type>,
    util::equality_comparable<bitmap_index_model<BitmapIndex>>
{
  using bitstream_type = typename BitmapIndex::bitstream_type;
  using bmi_concept = bitmap_index_concept<bitstream_type>;

  bitmap_index_model() = default;

  bitmap_index_model(BitmapIndex bmi)
    : bmi_{std::move(bmi)}
  {
  }

  virtual bool push_back_impl(value const& val) final
  {
    return bmi_.push_back_impl(val);
  }

  virtual bool stretch_impl(size_t n) final
  {
    return bmi_.stretch_impl(n);
  }

  virtual trial<bitstream_type>
  lookup_impl(relational_operator op, value const& val) const final
  {
    return bmi_.lookup_impl(op, val);
  }

  virtual uint64_t size_impl() const final
  {
    return bmi_.size_impl();
  }

  BitmapIndex const& cast(bmi_concept const& c) const
  {
    if (typeid(c) != typeid(*this))
      throw std::bad_cast();

    return static_cast<bitmap_index_model const&>(c).bmi_;
  }

  BitmapIndex& cast(bmi_concept& c)
  {
    if (typeid(c) != typeid(*this))
      throw std::bad_cast();

    return static_cast<bitmap_index_model&>(c).bmi_;
  }

  friend bool operator==(bitmap_index_model const& x, bitmap_index_model const& y)
  {
    return x.bmi_ == y.bmi_;
  }

  virtual std::unique_ptr<bmi_concept> copy() const final
  {
    return std::make_unique<bitmap_index_model>(*this);
  }

  virtual bool equals(bmi_concept const& other) const final
  {
    return bmi_ == cast(other);
  }

  virtual void serialize(serializer& sink) const final
  {
    sink << bmi_;
  }

  virtual void deserialize(deserializer& source) final
  {
    source >> bmi_;
  }

  BitmapIndex bmi_;
};

} // namespace detail

/// A polymorphic bitmap index with value semantics.
template <typename Bitstream>
class bitmap_index : public bitmap_index_base<bitmap_index<Bitstream>>,
                     util::equality_comparable<bitmap_index<Bitstream>>
{
  using super = bitmap_index_base<bitmap_index<Bitstream>>;
  friend super;

public:
  bitmap_index() = default;

  bitmap_index(bitmap_index const& other)
    : concept_{other.concept_ ? other.concept_->copy() : nullptr}
  {
  }

  bitmap_index(bitmap_index&& other)
    : concept_{std::move(other.concept_)}
  {
  }

  template <
    typename BitmapIndex,
    typename = util::disable_if_same_or_derived_t<bitmap_index, BitmapIndex>
  >
  bitmap_index(BitmapIndex&& bmi)
    : concept_{
        new detail::bitmap_index_model<std::decay_t<BitmapIndex>>{
            std::forward<BitmapIndex>(bmi)}}
  {
  }

  bitmap_index& operator=(bitmap_index const& other)
  {
    concept_ = other.concept_ ? other.concept_->copy() : nullptr;
    return *this;
  }

  bitmap_index& operator=(bitmap_index&& other)
  {
    concept_ = std::move(other.concept_);
    return *this;
  }

  explicit operator bool() const
  {
    return concept_ != nullptr;
  }

private:
  bool push_back_impl(value const& val)
  {
    assert(concept_);
    return concept_->push_back_impl(val);
  }

  bool stretch_impl(size_t n)
  {
    assert(concept_);
    return concept_->stretch_impl(n);
  }

  trial<Bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    assert(concept_);
    return concept_->lookup_impl(op, val);
  }

  uint64_t size_impl() const
  {
    assert(concept_);
    return concept_->size_impl();
  }

private:
  std::unique_ptr<detail::bitmap_index_concept<Bitstream>> concept_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    if (concept_)
      sink << true << concept_;
    else
      sink << false;
  }

  void deserialize(deserializer& source)
  {
    bool valid;
    source >> valid;
    if (valid)
      source >> concept_;
  }

  friend bool operator==(bitmap_index const& x, bitmap_index const& y)
  {
    assert(x.concept_);
    assert(y.concept_);
    return x.concept_->equals(*y.concept_);
  }
};

/// A bitmap index for arithmetic values.
template <typename Bitstream, type_tag T>
class arithmetic_bitmap_index
  : public bitmap_index_base<arithmetic_bitmap_index<Bitstream, T>>,
    util::equality_comparable<arithmetic_bitmap_index<Bitstream, T>>
{
  friend bitmap_index_base<arithmetic_bitmap_index<Bitstream, T>>;

  template <typename>
  friend struct detail::bitmap_index_model;

  using bitmap_type =
    std::conditional_t<
      T == time_range_value || T == time_point_value,
      time_range::rep,
      std::conditional_t<
        T == bool_value || T == int_value || T == uint_value || T == double_value,
        type_tag_type<T>,
        std::false_type
      >
    >;

  template <typename U>
  using bitmap_binner =
    std::conditional_t<
      T == double_value || T == time_range_value || T == time_point_value,
      precision_binner<U>,
      null_binner<U>
    >;

  template <typename B, typename U>
  using bitmap_coder =
    std::conditional_t<
      T == bool_value,
      equality_coder<B, U>,
      std::conditional_t<
        std::is_arithmetic<bitmap_type>::value,
        range_bitslice_coder<B, U>,
        std::false_type
      >
    >;

public:
  using bitstream_type = Bitstream;

  arithmetic_bitmap_index() = default;

  template <typename... Ts>
  void binner(Ts&&... xs)
  {
    bitmap_.binner(std::forward<Ts>(xs)...);
  }

private:
  static bitmap_type extract(value const& val)
  {
    switch (val.which())
    {
      default:
        return val.get<bitmap_type>();
      case time_range_value:
        return val.get<time_range>().count();
      case time_point_value:
        return val.get<time_point>().since_epoch().count();
    }
  }

  bool push_back_impl(value const& val)
  {
    return bitmap_.push_back(extract(val));
  }

  bool stretch_impl(size_t n)
  {
    return bitmap_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (op == in || op == not_in)
      return error{"unsupported relational operator:", op};

    if (bitmap_.empty())
      return Bitstream{};

    auto r = bitmap_.lookup(op, extract(val));
    if (r)
      return std::move(*r);
    else
      return r.error();
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
    sink << bitmap_;
  }

  void deserialize(deserializer& source)
  {
    source >> bitmap_;
  }

  friend bool operator==(arithmetic_bitmap_index const& x,
                         arithmetic_bitmap_index const& y)
  {
    return x.bitmap_ == y.bitmap_;
  }
};

/// A bitmap index for strings.
template <typename Bitstream>
class string_bitmap_index
  : public bitmap_index_base<string_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<string_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  string_bitmap_index() = default;

private:
  static uint8_t byte_at(string const& s, size_t i)
  {
    return static_cast<uint8_t>(s[i]);
  }

  bool push_back_impl(value const& val)
  {
    auto& str = val.get<string>();

    if (str.size() > bitmaps_.size())
      bitmaps_.resize(str.size());

    for (size_t i = 0; i < str.size(); ++i)
    {
      assert(this->size() >= bitmaps_[i].size());
      auto delta = this->size() - bitmaps_[i].size();
      if (delta > 0)
        if (! bitmaps_[i].append(delta, false))
          return false;

      if (! bitmaps_[i].push_back(byte_at(str, i)))
        return false;
    }

    return size_.push_back(str.size());
  }

  bool stretch_impl(size_t n)
  {
    return size_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    assert(val.which() == string_value);

    auto str = val.get<string>();

    switch (op)
    {
      default:
        return error{"unsupported relational operator", op};
      case equal:
      case not_equal:
        {
          if (str.empty())
          {
            if (auto s = size_.lookup(equal, 0))
              return std::move(op == equal ? *s : s->flip());
            else
              return s.error();
          }

          if (str.size() > bitmaps_.size())
            return Bitstream{this->size(), op == not_equal};

          auto r = size_.lookup(less_equal, str.size());
          if (! r)
            return r.error();

          if (r->find_first() == Bitstream::npos)
            return Bitstream{this->size(), op == not_equal};

          for (size_t i = 0; i < str.size(); ++i)
          {
            auto b = bitmaps_[i].lookup(equal, byte_at(str, i));
            if (! b)
              return b.error();

            if (b->find_first() != Bitstream::npos)
              *r &= *b;
            else
              return Bitstream{this->size(), op == not_equal};
          }

          return std::move(op == equal ? *r : r->flip());
        }
      case ni:
      case not_ni:
        {
          if (str.empty())
            return Bitstream{this->size(), op == ni};

          if (str.size() > bitmaps_.size())
            return Bitstream{this->size(), op == not_ni};

          // Iterate through all k-grams.
          Bitstream r{this->size(), 0};
          for (size_t i = 0; i < bitmaps_.size() - str.size() + 1; ++i)
          {
            Bitstream substr{this->size(), 1};;
            auto skip = false;
            for (size_t j = 0; j < str.size(); ++j)
            {
              auto bs = bitmaps_[i + j].lookup(equal, str[j]);
              if (! bs)
                return bs.error();

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

          return std::move(op == ni ? r : r.flip());
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
    sink << size_ << bitmaps_;
  }

  void deserialize(deserializer& source)
  {
    source >> size_ >> bitmaps_;
  }

  friend bool operator==(string_bitmap_index const& x,
                         string_bitmap_index const& y)
  {
    return x.bitmaps_ == y.bitmaps_;
  }
};

/// A bitmap index for IP addresses.
template <typename Bitstream>
class address_bitmap_index
  : public bitmap_index_base<address_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<address_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  address_bitmap_index() = default;

private:
  bool push_back_impl(value const& val)
  {
    address const* addr;

    if (val.which() == address_value)
      addr = &val.get<address>();
    else if (val.which() == prefix_value)
      addr = &val.get<prefix>().network();
    else
      return false;

    auto& bytes = addr->data();
    size_t const start = addr->is_v4() ? 12 : 0;

    if (! v4_.push_back(start == 12))
      return false;

    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]))
        return false;

    return true;
  }

  bool stretch_impl(size_t n)
  {
    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].append(n, false))
        return false;

    return v4_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (! (op == equal || op == not_equal || op == in || op == not_in))
      return error{"unsupported relational operator", op};

    if (v4_.empty())
      return Bitstream{};

    switch (val.which())
    {
      default:
        return error{"invalid type tag"};
      case address_value:
        return lookup_impl(op, val.get<address>());
      case prefix_value:
        return lookup_impl(op, val.get<prefix>());
    }
  }

  trial<Bitstream>
  lookup_impl(relational_operator op, address const& addr) const
  {
    auto& bytes = addr.data();
    auto is_v4 = addr.is_v4();
    auto r = is_v4 ? v4_ : Bitstream{this->size(), true};

    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
    {
      auto bs = bitmaps_[i][bytes[i]];
      if (! bs)
        return bs.error();

      if (bs->find_first() != Bitstream::npos)
        r &= *bs;
      else
        return Bitstream{this->size(), op == not_equal};
    }

    return std::move(op == equal ? r : r.flip());
  }

  trial<Bitstream> lookup_impl(relational_operator op, prefix const& pfx) const
  {
    if (! (op == in || op == not_in))
      return error{"unsupported relational operator", op};

    auto topk = pfx.length();
    if (topk == 0)
      return error{"invalid IP prefix length:", topk};

    auto net = pfx.network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      return lookup_impl(op == in ? equal : not_equal, pfx.network());

    auto r = is_v4 ? v4_ : Bitstream{this->size(), true};
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
          return std::move(r);
        }
      }

    return Bitstream{this->size(), false};
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
    sink << v4_ << bitmaps_;
  }

  void deserialize(deserializer& source)
  {
    source >> v4_ >> bitmaps_;
  }

  friend bool operator==(address_bitmap_index const& x,
                         address_bitmap_index const& y)
  {
    return x.bitmaps_ == y.bitmaps_;
  }
};

/// A bitmap index for IP prefixes.
template <typename Bitstream>
class prefix_bitmap_index
  : public bitmap_index_base<prefix_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<prefix_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  prefix_bitmap_index() = default;

private:
  bool push_back_impl(value const& val)
  {
    return network_.push_back(val)
      && length_.push_back(val.get<prefix>().length());
  }

  bool stretch_impl(size_t n)
  {
    return network_.stretch(n) && length_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (! (op == equal || op == not_equal))
      return error{"unsupported relational operator", op};

    auto pfx = val.get<prefix>();

    auto bs = network_.lookup(equal, pfx.network());
    if (! bs)
      return bs;

    auto n = network_.lookup(equal, pfx.network());
    if (! n)
      return n;

    auto r = Bitstream{*bs & *n};
    return std::move(op == equal ? r : r.flip());
  }

  uint64_t size_impl() const
  {
    return length_.size();
  }

  address_bitmap_index<Bitstream> network_;
  bitmap<uint8_t, Bitstream, range_bitslice_coder> length_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << network_ << length_;
  }

  void deserialize(deserializer& source)
  {
    source >> network_ >> length_;
  }

  friend bool operator==(prefix_bitmap_index const& x,
                         prefix_bitmap_index const& y)
  {
    return x.network_ == y.network_ && x.length_ == y.length_;
  }
};

/// A bitmap index for transport-layer ports.
template <typename Bitstream>
class port_bitmap_index
  : public bitmap_index_base<port_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<port_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  port_bitmap_index() = default;

private:
  bool push_back_impl(value const& val)
  {
    auto& p = val.get<port>();
    return num_.push_back(p.number()) && proto_.push_back(p.type());
  }

  bool stretch_impl(size_t n)
  {
    return num_.append(n, false) && proto_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    assert(val.which() == port_value);

    if (op == in || op == not_in)
      return error{"unsupported relational operator", op};

    if (num_.empty())
      return Bitstream{};

    auto& p = val.get<port>();
    auto n = num_.lookup(op, p.number());
    if (! n)
      return n.error();

    if (n->find_first() == Bitstream::npos)
      return Bitstream{this->size(), false};

    if (p.type() != port::unknown)
    {
      auto t = proto_[p.type()];
      if (! t)
        return t.error();

      *n &= *t;
    }

    return std::move(*n);
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
    sink << num_ << proto_;
  }

  void deserialize(deserializer& source)
  {
    source >> num_ >> proto_;
  }

  friend bool operator==(port_bitmap_index const& x, port_bitmap_index const& y)
  {
    return x.num_ == y.num_ && x.proto_ == y.proto_;
  }
};

template <typename Bitstream, typename... Args>
trial<bitmap_index<Bitstream>> make_bitmap_index(type_tag t, Args&&... args);

/// A bitmap index for sets, vectors, and tuples.
template <typename Bitstream>
class sequence_bitmap_index : public bitmap_index_base<sequence_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<sequence_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  sequence_bitmap_index() = default;

  sequence_bitmap_index(type_tag t)
    : elem_type_{t}
  {
  }

private:
  bool push_back_impl(value const& val)
  {
    if (val.which() == set_value)
      return push_back_container<set>(val);
    else if (val.which() == vector_value)
      return push_back_container<vector>(val);
    else
      return false;
  }

  bool stretch_impl(size_t n)
  {
    return size_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (op == ni)
      op = in;
    else if (op == not_ni)
      op = not_in;

    if (! (op == in || op == not_in))
      return error{"unsupported relational operator", op};

    if (this->empty())
      return Bitstream{};

    Bitstream r;
    auto t = bmis_.front().lookup(equal, val);
    if (t)
      r |= *t;
    else
      return t;

    for (size_t i = 1; i < bmis_.size(); ++i)
    {
      auto bs = bmis_[i].lookup(equal, val);
      if (bs)
        r |= *bs;
      else
        return bs;
    }

    if (r.size() < this->size())
      r.append(this->size() - r.size(), false);

    if (op == not_in)
      r.flip();

    return std::move(r);
  }

  uint64_t size_impl() const
  {
    return size_.size();
  }

  type_tag elem_type_;
  std::vector<bitmap_index<Bitstream>> bmis_;
  bitmap<uint32_t, Bitstream, range_bitslice_coder> size_;

private:
  template <typename C>
  bool push_back_container(value const& val)
  {
    auto& c = val.get<C>();

    if (c.empty())
      return size_.append(1, false);

    if (bmis_.size() < c.size())
    {
      auto old = bmis_.size();
      bmis_.resize(c.size());
      for (auto i = old; i < bmis_.size(); ++i)
      {
        auto bmi = make_bitmap_index<Bitstream>(elem_type_);
        if (! bmi)
          return false;

        bmis_[i] = std::move(*bmi);
      }
    }

    for (size_t i = 0; i < c.size(); ++i)
    {
      auto delta = this->size() - bmis_[i].size();
      if (delta > 0)
        if (! bmis_[i].stretch(delta))
          return false;

      if (! bmis_[i].push_back(c[i]))
        return false;
    }

    return size_.push_back(c.size());
  }

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << elem_type_ << bmis_ << size_;
  }

  void deserialize(deserializer& source)
  {
    source >> elem_type_ >> bmis_ >> size_;
  }

  friend bool
  operator==(sequence_bitmap_index const& x, sequence_bitmap_index const& y)
  {
    return x.elem_type_ == y.elem_type_
        && x.bmis_ == y.bmis_
        && x.size_ == y.size_;
  }
};

/// Factory to construct a bitmap index based on a given type tag.
template <typename Bitstream, typename... Args>
trial<bitmap_index<Bitstream>> make_bitmap_index(type_tag t, Args&&... args)
{
  switch (t)
  {
    default:
      return error{"unspported type tag:", t};
    case bool_value:
      return {arithmetic_bitmap_index<Bitstream, bool_value>(std::forward<Args>(args)...)};
    case int_value:
      return {arithmetic_bitmap_index<Bitstream, int_value>(std::forward<Args>(args)...)};
    case uint_value:
      return {arithmetic_bitmap_index<Bitstream, uint_value>(std::forward<Args>(args)...)};
    case double_value:
      return {arithmetic_bitmap_index<Bitstream, double_value>(std::forward<Args>(args)...)};
    case time_range_value:
      return {arithmetic_bitmap_index<Bitstream, time_range_value>(std::forward<Args>(args)...)};
    case time_point_value:
      return {arithmetic_bitmap_index<Bitstream, time_point_value>(std::forward<Args>(args)...)};
    case string_value:
      return {string_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
    case address_value:
      return {address_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
    case prefix_value:
      return {prefix_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
    case port_value:
      return {port_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
  }
}

} // namespace vast

#endif
