#include "vast/data_view.hpp"

namespace vast {

string_view::string_view(chunk_ptr chk, const flatbuffers::String* str)
  : bytes_view<char>{chk, str} {
}

std::string unpack(string_view view) {
  return std::string(view.data(), view.size());
}

pattern_view::pattern_view(chunk_ptr chk, const flatbuffers::String* str)
  : bytes_view<char>{chk, str} {
}

pattern unpack(pattern_view view) {
  return pattern{std::string(view.data(), view.size())};
}

address_view::address_view(chunk_ptr chk,
                           const flatbuffers::Vector<uint8_t>* addr)
  : bytes_view<uint8_t>{chk, addr} {
}

address unpack(address_view view) {
  auto data = reinterpret_cast<const uint32_t*>(view.data());
  auto family = view.size() == 4 ? address::ipv4 : address::ipv6;
  return {data, family, address::network};
}


subnet_view::subnet_view(chunk_ptr chk, const detail::Subnet* sn)
  : subnet_{sn},
    chunk_{chk} {
}

address_view subnet_view::network() const {
  VAST_ASSERT(subnet_ != nullptr);
  return address_view{chunk_, subnet_->network()};
}

uint8_t subnet_view::length() const {
  VAST_ASSERT(subnet_ != nullptr);
  return subnet_->length();
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
      db.add_boolean(x);
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
      auto str = builder_.CreateString(x);
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::StringType);
      db.add_str(str);
      return db.Finish();
    }
    auto operator()(const pattern& x) {
      auto str = builder_.CreateString(x.string());
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::PatternType);
      db.add_str(str);
      return db.Finish();
    }
    auto operator()(const address& x) {
      auto bytes = x.is_v4()
        ? builder_.CreateVector<uint8_t>(x.data().data() + 12, 4)
        : builder_.CreateVector<uint8_t>(x.data().data(), 16);
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::AddressType);
      db.add_address(bytes);
      return db.Finish();
    }
    auto operator()(const subnet& x) {
      auto bytes = x.network().is_v4()
        ? builder_.CreateVector<uint8_t>(x.network().data().data() + 12, 4)
        : builder_.CreateVector<uint8_t>(x.network().data().data(), 16);
      auto sn = detail::CreateSubnet(builder_, bytes, x.length());
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::SubnetType);
      db.add_subnet(sn);
      return db.Finish();
    }
    auto operator()(const port& x) {
      auto t = detail::PortType::Unknown;
      switch (x.type()) {
        default:
          break;
        case port::tcp:
          t = detail::PortType::TCP;
          break;
        case port::udp:
          t = detail::PortType::UDP;
          break;
        case port::icmp:
          t = detail::PortType::ICMP;
          break;
      }
      auto p = detail::Port{x.number(), t};
      detail::DataBuilder db{builder_};
      db.add_which(detail::DataType::PortType);
      db.add_port(&p);
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
