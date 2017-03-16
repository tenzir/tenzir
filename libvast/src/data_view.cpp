#include "vast/data_view.hpp"

namespace vast {

const uint8_t* bytes_view::data() const {
  VAST_ASSERT(bytes_ != nullptr);
  return bytes_->data();
}

size_t bytes_view::size() const {
  VAST_ASSERT(bytes_ != nullptr);
  return bytes_->size();
}

bytes_view::bytes_view(chunk_ptr chk, const flatbuffers::Vector<uint8_t>* bytes)
  : bytes_{bytes},
    chunk_{chk} {
}

string_view::string_view(chunk_ptr chk, const flatbuffers::Vector<uint8_t>* str)
  : bytes_view{chk, str} {
}

std::string unpack(string_view view) {
  return std::string(reinterpret_cast<const char*>(view.data()), view.size());
}

pattern_view::pattern_view(chunk_ptr chk,
                           const flatbuffers::Vector<uint8_t>* str)
  : bytes_view{chk, str} {
}

pattern unpack(pattern_view view) {
  auto s = std::string(reinterpret_cast<const char*>(view.data()), view.size());
  return pattern{std::move(s)};
}

address_view::address_view(chunk_ptr chk, const detail::Data* data)
  : data_{data},
    chunk_{chk} {
}

address unpack(address_view view) {
  VAST_ASSERT(view.data_ != nullptr);
  if (view.data_->bytes() != nullptr) {
    auto data = reinterpret_cast<const uint32_t*>(view.data_->bytes()->data());
    return {data, address::ipv6, address::network};
  } else {
    auto data = static_cast<const uint32_t>(view.data_->count());
    return {&data, address::ipv4, address::network};
  }
}

subnet_view::subnet_view(chunk_ptr chk, const detail::Data* data)
  : data_{data},
    chunk_{chk} {
}

address_view subnet_view::network() const {
  return address_view{chunk_, data_};
}

uint8_t subnet_view::length() const {
  return static_cast<uint8_t>(data_->integer());
}

subnet unpack(subnet_view view) {
  return {unpack(view.network()), view.length()};
}


vector_view::vector_view(
  chunk_ptr chk,
  const flatbuffers::Vector<flatbuffers::Offset<detail::Data>>* xs)
  : xs_{xs},
    chunk_{chk} {
}

vector unpack(vector_view view) {
  VAST_ASSERT(view.xs_ != nullptr);
  vector xs;
  xs.reserve(view.xs_->size());
  auto f = [&](auto x) { return unpack(data_view{view.chunk_, x}); };
  std::transform(view.xs_->begin(), view.xs_->end(), std::back_inserter(xs), f);
  return xs;
}

set_view::set_view(
  chunk_ptr chk,
  const flatbuffers::Vector<flatbuffers::Offset<detail::Data>>* xs)
  : xs_{xs},
    chunk_{chk} {
}

set unpack(set_view view) {
  VAST_ASSERT(view.xs_ != nullptr);
  set xs;
  auto f = [&](auto x) { return unpack(data_view{view.chunk_, x}); };
  std::transform(view.xs_->begin(), view.xs_->end(),
                 std::inserter(xs, xs.end()), f);
  return xs;
}

table_view::table_view(
  chunk_ptr chk,
  const flatbuffers::Vector<flatbuffers::Offset<detail::MapEntry>>* xs)
  : xs_{xs},
    chunk_{chk} {
}

table unpack(table_view view) {
  VAST_ASSERT(view.xs_ != nullptr);
  table xs;
  auto f = [&](auto x) {
    auto key = unpack(data_view{view.chunk_, x->key()});
    auto val = unpack(data_view{view.chunk_, x->value()});
    return std::make_pair(key, val);
  };
  std::transform(view.xs_->begin(), view.xs_->end(),
                 std::inserter(xs, xs.end()), f);
  return xs;
}

data_view::data_view(chunk_ptr chk)
  : data_{detail::GetData(chk->data())},
    chunk_{chk} {
}

data_view::data_view(chunk_ptr chk, const detail::Data* ptr)
  : data_{ptr},
    chunk_{chk} {
  // TODO: verify that ptr lives within the chunk.
}

flatbuffers::Offset<detail::Data>
build(flatbuffers::FlatBufferBuilder& builder, const data& x) {
  struct converter {
    converter(flatbuffers::FlatBufferBuilder& builder) : builder_{builder} {
    }
    auto operator()(none) {
      detail::DataBuilder db{builder_};
      return db.Finish();
    }
    auto operator()(boolean x) {
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::BooleanType);
      db.add_integer(x ? 1 : 0);
      return db.Finish();
    }
    auto operator()(integer x) {
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::IntegerType);
      db.add_integer(x);
      return db.Finish();
    }
    auto operator()(count x) {
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::CountType);
      db.add_count(x);
      return db.Finish();
    }
    auto operator()(real x) {
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::RealType);
      db.add_real(x);
      return db.Finish();
    }
    auto operator()(timestamp x) {
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::TimestampType);
      db.add_integer(x.time_since_epoch().count());
      return db.Finish();
    }
    auto operator()(timespan x) {
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::TimespanType);
      db.add_integer(x.count());
      return db.Finish();
    }
    auto operator()(const enumeration& x) {
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::EnumerationType);
      db.add_integer(x);
      return db.Finish();
    }
    auto operator()(const std::string& x) {
      auto ptr = reinterpret_cast<const uint8_t*>(x.data());
      auto bytes = builder_.CreateVector<uint8_t>(ptr, x.size());
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::StringType);
      db.add_bytes(bytes);
      return db.Finish();
    }
    auto operator()(const pattern& x) {
      auto ptr = reinterpret_cast<const uint8_t*>(x.string().data());
      auto bytes = builder_.CreateVector<uint8_t>(ptr, x.string().size());
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::PatternType);
      db.add_bytes(bytes);
      return db.Finish();
    }
    auto operator()(const address& x) {
      if (x.is_v4()) {
        detail::DataBuilder db{builder_};
        db.add_which(detail::DataType::AddressType);
        db.add_count(*reinterpret_cast<const count*>(x.data().data() + 12));
        return db.Finish();
      } else {
        auto bytes = builder_.CreateVector<uint8_t>(x.data().data(), 16);
        detail::DataBuilder db{builder_};
        db.add_which(detail::DataType::AddressType);
        db.add_bytes(bytes);
        return db.Finish();
      }
    }
    auto operator()(const subnet& x) {
      if (x.network().is_v4()) {
        detail::DataBuilder db{builder_};
        db.add_which(detail::DataType::AddressType);
        auto ptr = x.network().data().data() + 12;
        db.add_count(*reinterpret_cast<const count*>(ptr));
        db.add_integer(static_cast<integer>(x.length()));
        return db.Finish();
      } else {
        auto ptr = x.network().data().data();
        auto bytes = builder_.CreateVector<uint8_t>(ptr, 16);
        detail::DataBuilder db{builder_};
        db.add_which(detail::DataType::AddressType);
        db.add_bytes(bytes);
        db.add_integer(static_cast<integer>(x.length()));
        return db.Finish();
      }
    }
    auto operator()(const port& x) {
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::PortType);
      db.add_integer(static_cast<integer>(x.type()));
      db.add_count(static_cast<count>(x.number()));
      return db.Finish();
    }
    auto operator()(const vector& xs) {
      std::vector<flatbuffers::Offset<detail::Data>> offsets;
      offsets.reserve(xs.size());
      std::transform(xs.begin(), xs.end(), std::back_inserter(offsets),
                     [&](auto& x) { return visit(*this, x); });
      auto v = builder_.CreateVector(offsets);
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::VectorType);
      db.add_vector(v);
      return db.Finish();
    }
    auto operator()(const set& xs) {
      std::vector<flatbuffers::Offset<detail::Data>> offsets;
      offsets.reserve(xs.size());
      std::transform(xs.begin(), xs.end(), std::back_inserter(offsets),
                     [&](auto& x) { return visit(*this, x); });
      auto v = builder_.CreateVector(offsets);
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::SetType);
      db.add_vector(v);
      return db.Finish();
    }
    auto operator()(const table& xs) {
      std::vector<flatbuffers::Offset<detail::MapEntry>> offsets;
      auto f = [&](auto& x) {
        auto key = visit(*this, x.first);
        auto val = visit(*this, x.second);
        return detail::CreateMapEntry(builder_, key, val);
      };
      std::transform(xs.begin(), xs.end(), std::back_inserter(offsets), f);
      auto v = builder_.CreateVector(offsets);
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::MapType);
      db.add_map(v);
      return db.Finish();
    }
    flatbuffers::FlatBufferBuilder& builder_;
  };
  return visit(converter{builder}, x);
}

template <class T>
auto unpack(T x)
-> std::enable_if_t<
  std::is_arithmetic<T>::value
  || std::is_same<T, timestamp>::value
  || std::is_same<T, timespan>::value
  || std::is_same<T, port>::value
  || std::is_same<T, none>::value,
  data
> {
  return x;
}

data unpack(data_view view) {
  return visit([](auto x) -> data { return unpack(x); }, view);
}

} // namespace vast
