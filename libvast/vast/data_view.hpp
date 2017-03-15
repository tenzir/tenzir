#ifndef VAST_DATA_VIEW_HPP
#define VAST_DATA_VIEW_HPP

#include <cstdint>
#include <cstddef>
#include <functional>
#include <stdexcept>

#include <flatbuffers/flatbuffers.h>

#include "vast/data.hpp"
#include "vast/chunk.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/data_generated.h"

namespace vast {

class data_view;
class subnet_view;

class bytes_view {
public:
  bytes_view() = default;

  const uint8_t* data() const;

  size_t size() const;

protected:
  explicit bytes_view(chunk_ptr chk, const flatbuffers::Vector<uint8_t>* bytes);

  const flatbuffers::Vector<uint8_t>* bytes_;
  chunk_ptr chunk_;
};

class string_view : public bytes_view {
  friend data_view; // construction
  template <class F>
  friend auto visit(F&& f, data_view x);

public:
  string_view() = default;

private:
  explicit string_view(chunk_ptr chk, const flatbuffers::Vector<uint8_t>* str);
};

std::string unpack(string_view view);

class pattern_view : public bytes_view {
  friend data_view; // construction
  friend pattern unpack(pattern_view view);
  template <class F>
  friend auto visit(F&& f, data_view x);

public:
  pattern_view() = default;

private:
  explicit pattern_view(chunk_ptr chk, const flatbuffers::Vector<uint8_t>* str);
};

pattern unpack(pattern_view view);

class address_view : public bytes_view {
  friend data_view; // construction
  friend subnet_view;
  template <class F>
  friend auto visit(F&& f, data_view x);

public:
  address_view() = default;

private:
  explicit address_view(chunk_ptr chk,
                        const flatbuffers::Vector<uint8_t>* addr);
};

address unpack(address_view view);

class subnet_view {
  friend data_view; // construction
  template <class F>
  friend auto visit(F&& f, data_view x);

public:
  subnet_view() = default;

  address_view network() const;

  uint8_t length() const;

private:
  explicit subnet_view(chunk_ptr chk, const flatbuffers::Vector<uint8_t>* addr,
                       count length);

  const flatbuffers::Vector<uint8_t>* addr_ = nullptr;
  uint8_t length_;
  chunk_ptr chunk_;
};

subnet unpack(subnet_view view);

class vector_view {
  friend data_view; // construction
  friend vector unpack(vector_view view);
  template <class F>
  friend auto visit(F&& f, data_view x);

public:
  vector_view() = default;

private:
  explicit vector_view(
    chunk_ptr chk,
    const flatbuffers::Vector<flatbuffers::Offset<detail::Data>>* xs);

  const flatbuffers::Vector<flatbuffers::Offset<detail::Data>>* xs_;
  chunk_ptr chunk_;
};

vector unpack(vector_view view);

class set_view {
  friend data_view; // construction
  friend set unpack(set_view view);
  template <class F>
  friend auto visit(F&& f, data_view x);

public:
  set_view() = default;


private:
  explicit set_view(
    chunk_ptr chk,
    const flatbuffers::Vector<flatbuffers::Offset<detail::Data>>* xs);

  const flatbuffers::Vector<flatbuffers::Offset<detail::Data>>* xs_;
  chunk_ptr chunk_;
};

set unpack(set_view view);

class table_view {
  friend data_view; // construction
  friend table unpack(table_view view);
  template <class F>
  friend auto visit(F&& f, data_view x);

public:
  table_view() = default;

private:
  explicit table_view(
    chunk_ptr chk,
    const flatbuffers::Vector<flatbuffers::Offset<detail::MapEntry>>* xs);

  const flatbuffers::Vector<flatbuffers::Offset<detail::MapEntry>>* xs_;
  chunk_ptr chunk_;
};


table unpack(table_view view);

/// A view of ::data.
class data_view {
  friend vector unpack(vector_view view); // construction
  friend set unpack(set_view view); // construction
  friend table unpack(table_view view); // construction

public:
  data_view() = default;

  /// Constructs a data view from a chunk.
  /// @param chk The chunk to construct a data view from.
  explicit data_view(chunk_ptr chk);

  // single dispatch
  template <class F>
  friend auto visit(F&& f, data_view x) {
    VAST_ASSERT(x.data_ != nullptr);
    switch (x.data_->which()) {
      case detail::DataType::NoneType:
        return f(nil);
      case detail::DataType::BooleanType:
        return f(x.data_->integer() == 1);
      case detail::DataType::IntegerType:
        return f(x.data_->integer());
      case detail::DataType::CountType:
        return f(x.data_->count());
      case detail::DataType::RealType:
        return f(x.data_->real());
      case detail::DataType::TimestampType:
        return f(timestamp{timespan{x.data_->integer()}});
      case detail::DataType::TimespanType:
        return f(timespan{x.data_->integer()});
      case detail::DataType::EnumerationType:
        return f(static_cast<enumeration>(x.data_->integer()));
      case detail::DataType::PortType: {
        auto type = static_cast<port::port_type>(x.data_->integer());
        auto num = static_cast<port::number_type>(x.data_->count());
        return f(port{num, type});
      }
      case detail::DataType::StringType:
        return f(string_view{x.chunk_, x.data_->bytes()});
      case detail::DataType::PatternType:
        return f(pattern_view{x.chunk_, x.data_->bytes()});
      case detail::DataType::AddressType:
        return f(address_view{x.chunk_, x.data_->bytes()});
      case detail::DataType::SubnetType:
        return f(subnet_view{x.chunk_, x.data_->bytes(), x.data_->count()});
      case detail::DataType::VectorType:
        return f(vector_view{x.chunk_, x.data_->vector()});
      case detail::DataType::SetType:
        return f(set_view{x.chunk_, x.data_->vector()});
      case detail::DataType::MapType:
        return f(table_view{x.chunk_, x.data_->map()});
    }
    VAST_ASSERT(!"should never be reached");
    return f(nil);
  }

  // double dispatch
  template <class F>
  friend auto visit(F&& f, data_view x, data_view y) {
    VAST_ASSERT(x.data_ != nullptr);
    using std::placeholders::_1;
    switch (x.data_->which()) {
      default: // TODO
        VAST_ASSERT(!"not yet implemented");
        return visit(std::bind(f, nil, _1), y);
      case detail::DataType::NoneType:
        return visit(std::bind(f, nil, _1), y);
    }
  }

  template <class T>
  struct getter {
    optional<T> operator()(none) const {
      return nil;
    }

    optional<T> operator()(T x) const {
      return x;
    }

    template <class U>
    optional<T> operator()(U) const {
      return {};
    }
  };

  template <class T>
  struct checker {
    auto operator()(T) const {
      return true;
    }

    template <class U>
    auto operator()(U) const {
      return false;
    }
  };

private:
  data_view(chunk_ptr chk, const detail::Data* ptr);

  const detail::Data* data_ = nullptr;
  chunk_ptr chunk_;
};

template <class T>
auto get_if(data_view x) {
  return visit(data_view::getter<T>{}, x);
}

template <class T>
auto get(data_view x) {
  if (auto o = get_if<T>(x))
    return *o;
  throw std::runtime_error{"vast::get<T>(data_view)"};
}

template <class T>
bool is(data_view x) {
  return visit(data_view::checker<T>{}, x);
}

flatbuffers::Offset<detail::Data>
build(flatbuffers::FlatBufferBuilder& builder, const data& x);

data unpack(data_view view);

} // namespace vast

#endif
