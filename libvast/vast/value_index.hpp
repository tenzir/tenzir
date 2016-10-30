#ifndef VAST_VALUE_INDEX_HPP
#define VAST_VALUE_INDEX_HPP

#include <algorithm>
#include <memory>
#include <type_traits>

#include "vast/ewah_bitmap.hpp"
#include "vast/bitmap.hpp"
#include "vast/bitmap_index.hpp"
#include "vast/data.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/operator.hpp"
#include "vast/detail/assert.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/maybe.hpp"
#include "vast/type.hpp"

namespace vast {

/// The base class for value indexes.
class value_index {
public:
  using size_type = typename bitmap::size_type;

  /// Constructs a value index from a given type. All
  static std::unique_ptr<value_index> make(type const& t);

  /// Appends a data value.
  /// @param x The data to append to the index.
  /// @returns `true` if appending succeeded.
  bool push_back(data const& x);

  /// Appends a data value.
  /// @param x The data to append to the index.
  /// @param id The positional identifier of *x*.
  /// @returns `true` if appending succeeded.
  bool push_back(data const& x, event_id id);

  /// Looks up data under a relational operator.
  /// @param op The relation operator.
  /// @param x The value to lookup.
  /// @returns The result of the lookup or an error upon failure.
  maybe<bitmap> lookup(relational_operator op, data const& x) const;

  /// Merges another value index with this one.
  /// @param other The value index to merge.
  /// @returns `true` on success.
  //bool merge(value_index const& other);

  /// Retrieves the ID of the last ::push_back operation.
  /// @returns The largest ID in the index.
  size_type offset() const;

  template <class Inspector>
  friend auto inspect(Inspector& f, value_index& vi) {
    return f(vi.mask_, vi.none_);
  }

protected:
  value_index() = default;

private:
  virtual bool push_back_impl(data const& x, event_id id) = 0;

  virtual maybe<bitmap>
  lookup_impl(relational_operator op, data const& x) const = 0;

  ewah_bitmap mask_;
  ewah_bitmap none_;
};

/// An index for arithmetic values.
template <class T, class Binner = void>
class arithmetic_index : public value_index {
public:
  using value_type =
    std::conditional_t<
      std::is_same<T, timestamp>{} || std::is_same<T, interval>{},
      interval::rep,
      std::conditional_t<
        std::is_same<T, boolean>{}
          || std::is_same<T, integer>{}
          || std::is_same<T, count>{}
          || std::is_same<T, real>{},
        T,
        std::false_type
      >
    >;

  static_assert(!std::is_same<value_type, std::false_type>{},
                "invalid type T for arithmetic_index");

  using coder_type =
    std::conditional_t<
      std::is_same<T, boolean>{},
      singleton_coder<bitmap>,
      multi_level_coder<range_coder<bitmap>>
    >;

  using binner_type =
    std::conditional_t<
      std::is_void<Binner>{},
      // Choose a space-efficient binner if none specified.
      std::conditional_t<
        std::is_same<T, timestamp>{} || std::is_same<T, interval>{},
        decimal_binner<9>, // nanoseconds -> seconds
        std::conditional_t<
          std::is_same<T, real>{},
          precision_binner<10>, // no fractional part
          identity_binner
        >
      >,
      Binner
    >;

  using bitmap_index_type = bitmap_index<value_type, coder_type, binner_type>;

  template <
    class... Ts,
    class = std::enable_if_t<std::is_constructible<bitmap_index_type, Ts...>{}>
  >
  explicit arithmetic_index(Ts&&... xs) : bmi_{std::forward<Ts>(xs)...} {
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, arithmetic_index& idx) {
    return f(static_cast<value_index&>(idx), idx.bmi_);
  }

private:
  struct appender {
    appender(bitmap_index_type& idx, size_type skip) : bmi_{idx}, skip_{skip} {
    }

    template <class U>
    bool operator()(U const&) const {
      return false;
    }

    bool operator()(value_type x) const {
      bmi_.push_back(x, skip_);
      return true;
    }

    bool operator()(timestamp x) const {
      return (*this)(x.time_since_epoch().count());
    }

    bool operator()(interval x) const {
      return (*this)(x.count());
    }

    bitmap_index_type& bmi_;
    size_type skip_;
  };

  struct searcher {
    searcher(bitmap_index_type const& idx, relational_operator op)
      : bmi_{idx}, op_{op} {
    }

    template <class U>
    auto operator()(U const& x) const
    -> std::enable_if_t<!std::is_arithmetic<U>{}, maybe<bitmap>> {
      return fail<ec::type_clash>(value_type{}, x);
    }

    maybe<bitmap> operator()(boolean x) const {
      // Boolean indexes support only equality
      if (!(op_ == equal || op_ == not_equal))
        return fail<ec::unsupported_operator>(op_);
      return bmi_.lookup(op_, x);
    }

    template <class U>
    auto operator()(U x) const
    -> std::enable_if_t<std::is_arithmetic<U>{}, maybe<bitmap>> {
      // No operator constraint on arithmetic type.
      return bmi_.lookup(op_, x);
    }

    maybe<bitmap> operator()(timestamp x) const {
      return (*this)(x.time_since_epoch().count());
    }

    maybe<bitmap> operator()(interval x) const {
      return (*this)(x.count());
    }

    bitmap_index_type const& bmi_;
    relational_operator op_;
  };

  bool push_back_impl(data const& x, size_type skip) override {
    return visit(appender{bmi_, skip}, x);
  }

  maybe<bitmap>
  lookup_impl(relational_operator op, data const& x) const override {
    return visit(searcher{bmi_, op}, x);
  };

  bitmap_index_type bmi_;
};

/// An index for strings.
class string_index : public value_index {
public:
  /// Constructs a string index.
  /// @param max_length The maximum string length to support. Longer strings
  ///                   will be chopped to this size.
  explicit string_index(size_t max_length = 1024);

  template <class Inspector>
  friend auto inspect(Inspector& f, string_index& idx) {
    return f(static_cast<value_index&>(idx), idx.length_, idx.chars_);
  }

private:
  /// The index which holds each character.
  using char_bitmap_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;

  /// The index which holds the string length.
  using length_bitmap_index =
    bitmap_index<uint32_t, multi_level_coder<range_coder<bitmap>>>;

  void init();

  bool push_back_impl(data const& x, size_type skip) override;

  maybe<bitmap>
  lookup_impl(relational_operator op, data const& x) const override;

  size_t max_length_;
  length_bitmap_index length_;
  std::vector<char_bitmap_index> chars_;
};

/// An index for IP addresses.
class address_index : public value_index {
public:
  using byte_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;
  using type_index = bitmap_index<bool, singleton_coder<ewah_bitmap>>;

  address_index() = default;

  template <class Inspector>
  friend auto inspect(Inspector& f, address_index& idx) {
    return f(static_cast<value_index&>(idx), idx.bytes_, idx.v4_);
  }

private:
  void init();

  bool push_back_impl(data const& x, size_type skip) override;

  maybe<bitmap>
  lookup_impl(relational_operator op, data const& x) const override;

  std::array<byte_index, 16> bytes_;
  type_index v4_;
};

/// An index for subnets.
class subnet_index : public value_index {
public:
  using prefix_index = bitmap_index<uint8_t, equality_coder<ewah_bitmap>>;

  subnet_index() = default;

  template <class Inspector>
  friend auto inspect(Inspector& f, subnet_index& idx) {
    return f(static_cast<value_index&>(idx), idx.network_, idx.length_);
  }

private:
  void init();

  bool push_back_impl(data const& x, size_type skip) override;

  maybe<bitmap>
  lookup_impl(relational_operator op, data const& x) const override;

  address_index network_;
  prefix_index length_;
};

/// An index for ports.
class port_index : public value_index {
public:
  using number_index =
    bitmap_index<
      port::number_type,
      multi_level_coder<range_coder<ewah_bitmap>>
    >;

  using protocol_index =
    bitmap_index<
      std::underlying_type<port::port_type>::type,
      equality_coder<ewah_bitmap>
    >;

  port_index() = default;

  template <class Inspector>
  friend auto inspect(Inspector& f, port_index& idx) {
    return f(static_cast<value_index&>(idx), idx.num_, idx.proto_);
  }

private:
  void init();

  bool push_back_impl(data const& x, size_type skip) override;

  maybe<bitmap>
  lookup_impl(relational_operator op, data const& x) const override;

  number_index num_;
  protocol_index proto_;
};

/// An index for vectors and sets.
class sequence_index : public value_index {
public:
  /// Constructs a sequence index of a given type.
  /// @param t The element type of the sequence.
  /// @param max_size The maximum number of elements permitted per sequence.
  ///                 Longer sequences will be trimmed at the end.
  sequence_index(vast::type t = {}, size_t max_size = 128);

  /// The bitmap index holding the sequence size.
  using size_bitmap_index =
    bitmap_index<uint32_t, multi_level_coder<range_coder<bitmap>>>;

  friend void serialize(caf::serializer& sink, sequence_index const& idx);
  friend void serialize(caf::deserializer& source, sequence_index& idx);

private:
  void init();

  template <class Container>
  bool push_back_ctnr(Container& c, size_type skip) {
    init();
    auto seq_size = c.size();
    if (seq_size > max_size_)
      seq_size = max_size_;
    if (seq_size > elements_.size()) {
      auto old = elements_.size();
      elements_.resize(seq_size);
      for (auto i = old; i < elements_.size(); ++i) {
        elements_[i] = value_index::make(value_type_);
        VAST_ASSERT(elements_[i]);
      }
    }
    auto id = offset() + skip;
    auto x = c.begin();
    for (auto i = 0u; i < seq_size; ++i) {
      VAST_ASSERT(offset() >= elements_[i]->offset());
      elements_[i]->push_back(*x++, id);
    }
    size_.push_back(seq_size, skip);
    return true;
  }

  bool push_back_impl(data const& x, size_type skip) override;

  maybe<bitmap>
  lookup_impl(relational_operator op, data const& x) const override;

  std::vector<std::unique_ptr<value_index>> elements_;
  size_bitmap_index size_;
  size_t max_size_;
  vast::type value_type_;
};

namespace detail {

struct value_index_inspect_helper {
  const vast::type& type;
  std::unique_ptr<value_index>& idx;

  template <class Inspector>
  struct down_cast {
    using result_type = typename Inspector::result_type;

    down_cast(value_index& idx, Inspector& f) : idx_{idx}, f_{f} {
      // nop
    }

    template <class T>
    result_type operator()(T const&) const {
      die("invalid type");
    }

    result_type operator()(boolean_type const&) const {
      return f_(static_cast<arithmetic_index<boolean>&>(idx_));
    }

    result_type operator()(integer_type const&) const {
      return f_(static_cast<arithmetic_index<integer>&>(idx_));
    }

    result_type operator()(count_type const&) const {
      return f_(static_cast<arithmetic_index<count>&>(idx_));
    }

    result_type operator()(real_type const&) const {
      return f_(static_cast<arithmetic_index<real>&>(idx_));
    }

    result_type operator()(interval_type const&) const {
      return f_(static_cast<arithmetic_index<interval>&>(idx_));
    }

    result_type operator()(timestamp_type const&) const {
      return f_(static_cast<arithmetic_index<timestamp>&>(idx_));
    }

    result_type operator()(string_type const&) const {
      return f_(static_cast<string_index&>(idx_));
    }

    result_type operator()(address_type const&) const {
      return f_(static_cast<address_index&>(idx_));
    }

    result_type operator()(subnet_type const&) const {
      return f_(static_cast<subnet_index&>(idx_));
    }

    result_type operator()(port_type const&) const {
      return f_(static_cast<port_index&>(idx_));
    }

    result_type operator()(vector_type const&) const {
      return f_(static_cast<sequence_index&>(idx_));
    }

    result_type operator()(set_type const&) const {
      return f_(static_cast<sequence_index&>(idx_));
    }

    result_type operator()(alias_type const& t) const {
      return visit(*this, t.value_type);
    }

    value_index& idx_;
    Inspector& f_;
  };

  struct default_construct {
    using result_type = std::unique_ptr<value_index>;

    template <class T>
    result_type operator()(T const&) const {
      die("invalid type");
    }

    result_type operator()(boolean_type const&) const {
      return std::make_unique<arithmetic_index<boolean>>();
    }

    result_type operator()(integer_type const&) const {
      return std::make_unique<arithmetic_index<integer>>();
    }

    result_type operator()(count_type const&) const {
      return std::make_unique<arithmetic_index<count>>();
    }

    result_type operator()(real_type const&) const {
      return std::make_unique<arithmetic_index<real>>();
    }

    result_type operator()(interval_type const&) const {
      return std::make_unique<arithmetic_index<interval>>();
    }

    result_type operator()(timestamp_type const&) const {
      return std::make_unique<arithmetic_index<timestamp>>();
    }

    result_type operator()(string_type const&) const {
      return std::make_unique<string_index>();
    }

    result_type operator()(address_type const&) const {
      return std::make_unique<address_index>();
    }

    result_type operator()(subnet_type const&) const {
      return std::make_unique<subnet_index>();
    }

    result_type operator()(port_type const&) const {
      return std::make_unique<port_index>();
    }

    result_type operator()(vector_type const&) const {
      return std::make_unique<sequence_index>();
    }

    result_type operator()(set_type const&) const {
      return std::make_unique<sequence_index>();
    }

    result_type operator()(alias_type const& t) const {
      return visit(*this, t.value_type);
    }
  };

  template <class Inspector>
  friend auto inspect(Inspector& f, value_index_inspect_helper& helper) {
    if (Inspector::writes_state)
      helper.idx = visit(default_construct{}, helper.type);
    VAST_ASSERT(helper.idx);
    return visit(down_cast<Inspector>{*helper.idx, f}, helper.type);
  }
};

} // namespace detail
} // namespace vast

#endif
