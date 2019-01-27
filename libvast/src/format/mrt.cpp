#include "vast/format/mrt.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/error.hpp"
#include "vast/si_literals.hpp"

namespace vast {
namespace format {
namespace mrt {

reader::factory::factory(reader& parent, uint32_t ts)
  : parent_(parent), produced_(0) {
  using namespace std::chrono;
  auto since_epoch = duration<uint32_t>{ts};
  timestamp_ = timestamp{duration_cast<timespan>(since_epoch)};
}

caf::error reader::factory::operator()(caf::none_t) const {
  return caf::none;
}

caf::error reader::factory::operator()(table_dump_v2::peer_index_table& x) {
  auto bptr = parent_.builder(parent_.types_.table_dump_v2_peer_entry_type);
  VAST_ASSERT(bptr != nullptr);
  for (auto i = 0u; i < x.peer_count; ++i)
    if (auto err = produce(bptr, i, x.peer_entries[i].peer_bgp_id,
                           x.peer_entries[i].peer_ip_address,
                           x.peer_entries[i].peer_as))
      return err;
  return caf::none;
}

caf::error reader::factory::operator()(table_dump_v2::rib_afi_safi& x) {
  auto bptr = parent_.builder(parent_.types_.table_dump_v2_rib_entry_type);
  VAST_ASSERT(bptr != nullptr);
  for (size_t i = 0; i < x.entries.size(); ++i) {
    // Extract paths to ASes.
    std::vector<vast::data> as_path;
    for (auto as : x.entries[i].bgp_attributes.as_path)
      as_path.emplace_back(as);
    if (as_path.empty())
      return make_error(ec::parse_error, "origin AS missing");
    auto origin_as = caf::get<count>(as_path.back());
    // Extract communities.
    std::vector<vast::data> communities;
    for (auto community : x.entries[i].bgp_attributes.communities)
      communities.emplace_back(community);
    // Put it all together.
    if (auto err = produce(bptr, x.entries[i].peer_index, x.header.prefix[0],
                           std::move(as_path), origin_as,
                           x.entries[i].bgp_attributes.origin,
                           x.entries[i].bgp_attributes.next_hop,
                           x.entries[i].bgp_attributes.local_pref,
                           x.entries[i].bgp_attributes.multi_exit_disc,
                           communities,
                           x.entries[i].bgp_attributes.atomic_aggregate,
                           x.entries[i].bgp_attributes.aggregator_as,
                           x.entries[i].bgp_attributes.aggregator_ip))
      return err;
  }
  return caf::none;
}

caf::error reader::factory::operator()(bgp4mp::state_change& x) {
  return produce(parent_.types_.bgp4mp_state_change_type, x.peer_ip_address,
                 x.peer_as_number, x.old_state, x.new_state);
}

caf::error reader::factory::operator()(bgp4mp::message& x) {
  if (auto open = caf::get_if<bgp::open>(&x.message.message)) {
    return produce(parent_.types_.bgp4mp_open_type, open->version,
                   open->my_autonomous_system, open->hold_time,
                   open->bgp_identifier);
  } else if (auto update = caf::get_if<bgp::update>(&x.message.message)) {
    // Extract paths to ASes.
    std::vector<vast::data> as_path;
    for (size_t i = 0; i < update->path_attributes.as_path.size(); ++i)
      as_path.emplace_back(update->path_attributes.as_path[i]);
    if (as_path.empty())
      return make_error(ec::parse_error, "origin AS missing");
    auto origin_as = caf::get<count>(as_path.back());
    // Extract communities.
    std::vector<vast::data> communities;
    for (size_t i = 0; i < update->path_attributes.communities.size(); ++i)
      communities.emplace_back(update->path_attributes.communities[i]);
    // Check for update withdraw events.
    if (update->withdrawn_routes.size() > 0) {
      auto bptr = parent_.builder(parent_.types_.bgp4mp_update_withdraw_type);
      VAST_ASSERT(bptr != nullptr);
      for (size_t i = 0; i < update->withdrawn_routes.size(); ++i)
        if (auto err = produce(bptr, x.peer_ip_address, x.peer_as_number,
                               update->withdrawn_routes[i]))
          return err;
    }
    // Check for announcement updates.
    if (update->path_attributes.mp_reach_nlri.size() > 0) {
      auto bptr = parent_.builder(
        parent_.types_.bgp4mp_update_announcement_type);
      VAST_ASSERT(bptr != nullptr);
      for (auto& element : update->path_attributes.mp_reach_nlri)
        if (auto err = produce(bptr, x.peer_ip_address, x.peer_as_number,
                               element, as_path, origin_as,
                               update->path_attributes.origin,
                               update->path_attributes.next_hop,
                               update->path_attributes.local_pref,
                               update->path_attributes.multi_exit_disc,
                               communities,
                               update->path_attributes.atomic_aggregate,
                               update->path_attributes.aggregator_as,
                               update->path_attributes.aggregator_ip))
          return err;
    }
    // Check for withdrawal updates.
    if (update->path_attributes.mp_unreach_nlri.size() > 0) {
      auto bptr = parent_.builder(parent_.types_.bgp4mp_update_withdraw_type);
      VAST_ASSERT(bptr != nullptr);
      for (auto& element : update->path_attributes.mp_unreach_nlri)
        if (auto err = produce(bptr, x.peer_ip_address, x.peer_as_number,
                               element))
          return err;
    }
    // Check for reachability updates.
    if (update->network_layer_reachability_information.size() > 0) {
      auto bptr = parent_.builder(
        parent_.types_.bgp4mp_update_announcement_type);
      VAST_ASSERT(bptr != nullptr);
      for (auto& element : update->network_layer_reachability_information)
        if (auto err = produce(bptr, x.peer_ip_address, x.peer_as_number,
                               element, as_path, origin_as,
                               update->path_attributes.origin,
                               update->path_attributes.next_hop,
                               update->path_attributes.local_pref,
                               update->path_attributes.multi_exit_disc,
                               communities,
                               update->path_attributes.atomic_aggregate,
                               update->path_attributes.aggregator_as,
                               update->path_attributes.aggregator_ip))
          return err;
    }
  } else if (auto nf = caf::get_if<bgp::notification>(&x.message.message)) {
    return produce(parent_.types_.bgp4mp_notification_type, nf->error_code,
                   nf->error_subcode);
  }
  return caf::none;
}

caf::error reader::factory::operator()(bgp4mp::message_as4& x) {
  if (auto open = caf::get_if<bgp::open>(&x.message.message)) {
    if (auto err = produce(parent_.types_.bgp4mp_open_type, open->version,
                           open->my_autonomous_system, open->hold_time,
                           open->bgp_identifier))
      return err;
  } else if (auto update = caf::get_if<bgp::update>(&x.message.message)) {
    // Extract paths to ASes.
    std::vector<vast::data> as_path;
    for (auto as : update->path_attributes.as_path)
      as_path.emplace_back(as);
    if (as_path.empty())
      return make_error(ec::parse_error, "origin AS missing");
    auto origin_as = caf::get<count>(as_path.back());
    // Extract communities.
    std::vector<vast::data> communities;
    for (auto community : update->path_attributes.communities)
      communities.emplace_back(community);
    // Check for route withdrawals.
    if (update->withdrawn_routes.size() > 0) {
      auto bptr = parent_.builder(parent_.types_.bgp4mp_update_withdraw_type);
      VAST_ASSERT(bptr != nullptr);
      for (auto& withdrawn_route : update->withdrawn_routes)
        if (auto err = produce(bptr, x.peer_ip_address, x.peer_as_number,
                               withdrawn_route))
          return err;
    }
    // Check for announcement updates.
    if (update->path_attributes.mp_reach_nlri.size() > 0) {
      auto bptr = parent_.builder(
        parent_.types_.bgp4mp_update_announcement_type);
      VAST_ASSERT(bptr != nullptr);
      for (auto& element : update->path_attributes.mp_reach_nlri)
        if (auto err = produce(bptr, x.peer_ip_address, x.peer_as_number,
                               element, as_path, origin_as,
                               update->path_attributes.origin,
                               update->path_attributes.next_hop,
                               update->path_attributes.local_pref,
                               update->path_attributes.multi_exit_disc,
                               communities,
                               update->path_attributes.atomic_aggregate,
                               update->path_attributes.aggregator_as,
                               update->path_attributes.aggregator_ip))
          return err;
    }
    // Check for withdrawal updates.
    if (update->path_attributes.mp_unreach_nlri.size() > 0) {
      auto bptr = parent_.builder(
        parent_.types_.bgp4mp_update_withdraw_type);
      VAST_ASSERT(bptr != nullptr);
      for (auto& element :update->path_attributes.mp_unreach_nlri)
        if (auto err = produce(bptr, x.peer_ip_address, x.peer_as_number,
                               element))
          return err;
    }
    if (update->network_layer_reachability_information.size() > 0) {
      auto bptr = parent_.builder(
        parent_.types_.bgp4mp_update_announcement_type);
      VAST_ASSERT(bptr != nullptr);
      for (auto& element :  update->network_layer_reachability_information)
        if (auto err = produce(bptr, x.peer_ip_address, x.peer_as_number,
                               element, as_path, origin_as,
                               update->path_attributes.origin,
                               update->path_attributes.next_hop,
                               update->path_attributes.local_pref,
                               update->path_attributes.multi_exit_disc,
                               communities,
                               update->path_attributes.atomic_aggregate,
                               update->path_attributes.aggregator_as,
                               update->path_attributes.aggregator_ip))
          return err;
    }
  } else if (auto nf = caf::get_if<bgp::notification>(&x.message.message)) {
    return produce(parent_.types_.bgp4mp_notification_type, nf->error_code,
                   nf->error_subcode);
  }
  return caf::none;
}

caf::error reader::factory::operator()(bgp4mp::state_change_as4& x) {
  return produce(parent_.types_.bgp4mp_state_change_type, x.peer_ip_address,
                 x.peer_as_number, x.old_state, x.new_state);
}

reader::reader(caf::atom_value table_slice_type)
  : super(table_slice_type),
    max_slice_size_(0),
    current_consumer_(nullptr) {
  types_.table_dump_v2_peer_entry_type = record_type{{
    {"index", count_type{}},
    {"bgp_id", count_type{}},
    {"ip_address", address_type{}},
    {"as", count_type{}},
  }}.name("mrt::table_dump_v2::peer_entry");
  types_.table_dump_v2_rib_entry_type = record_type{{
    {"peer_index", count_type{}},
    {"prefix", subnet_type{}},
    {"as_path", vector_type{count_type{}}},
    {"origin_as", count_type{}},
    {"origin", string_type{}.attributes({{"skip"}})},
    {"nexthop", address_type{}},
    {"local_pref", count_type{}},
    {"med", count_type{}},
    {"community", vector_type{count_type{}}},
    {"atomic_aggregate", boolean_type{}},
    {"aggregator_as", count_type{}},
    {"aggregator_ip", address_type{}},
  }}.name("mrt::table_dump_v2::rib_entry");
  types_.bgp4mp_open_type = record_type{{
    {"version", count_type{}},
    {"my_autonomous_system", count_type{}},
    {"hold_time", count_type{}},
    {"bgp_identifier", count_type{}},
  }}.name("mrt::bgp4mp::open");
  types_.bgp4mp_update_announcement_type = record_type{{
    {"source_ip", address_type{}},
    {"source_as", count_type{}},
    {"prefix", subnet_type{}},
    {"as_path", vector_type{count_type{}}},
    {"origin_as", count_type{}},
    {"origin", string_type{}.attributes({{"skip"}})},
    {"nexthop", address_type{}},
    {"local_pref", count_type{}},
    {"med", count_type{}},
    {"community", vector_type{count_type{}}},
    {"atomic_aggregate", boolean_type{}},
    {"aggregator_as", count_type{}},
    {"aggregator_ip", address_type{}},
  }}.name("mrt::bgp4mp::update::announcement");
  types_.bgp4mp_update_withdraw_type = record_type{{
    {"source_ip", address_type{}},
    {"source_as", count_type{}},
    {"prefix", subnet_type{}},
  }}.name("mrt::bgp4mp::update::withdrawn");
  types_.bgp4mp_notification_type = record_type{{
    {"error_code", count_type{}},
    {"error_subcode", count_type{}},
  }}.name("mrt::bgp4mp::notification");
  types_.bgp4mp_keepalive_type = record_type{
  }.name("mrt::bgp4mp::keepalive");
  types_.bgp4mp_state_change_type = record_type{{
    {"source_ip", address_type{}},
    {"source_as", count_type{}},
    {"old_state", count_type{}},
    {"new_state", count_type{}},
  }}.name("mrt::bgp4mp::state_change");
}

reader::reader(caf::atom_value table_slice_type,
               std::unique_ptr<std::istream> input)
  : reader(table_slice_type) {
  VAST_ASSERT(input != nullptr);
  input_ = std::move(input);
}

caf::error reader::schema(vast::schema sch) {
  auto xs = {
    &types_.table_dump_v2_peer_entry_type,
    &types_.table_dump_v2_rib_entry_type,
    &types_.bgp4mp_update_announcement_type,
    &types_.bgp4mp_update_withdraw_type,
    &types_.bgp4mp_state_change_type,
    &types_.bgp4mp_open_type,
    &types_.bgp4mp_notification_type,
    &types_.bgp4mp_keepalive_type,
  };
  return replace_if_congruent(xs, sch);
}

vast::schema reader::schema() const {
  vast::schema sch;
  sch.add(types_.table_dump_v2_peer_entry_type);
  sch.add(types_.table_dump_v2_rib_entry_type);
  sch.add(types_.bgp4mp_update_announcement_type);
  sch.add(types_.bgp4mp_update_withdraw_type);
  sch.add(types_.bgp4mp_state_change_type);
  sch.add(types_.bgp4mp_open_type);
  sch.add(types_.bgp4mp_notification_type);
  sch.add(types_.bgp4mp_keepalive_type);
  return sch;
}

const char* reader::name() const {
  return "mrt-reader";
}

caf::error reader::read_impl(size_t max_events, size_t max_slice_size,
                             consumer& f) {
  current_consumer_ = &f;
  max_slice_size_ = max_slice_size;
  size_t produced = 0;
  while (produced < max_events) {
    // We have to read the input block-wise in a manner that respects the
    // protocol framing.
    static constexpr size_t common_header_length = 12;
    if (buffer_.size() < common_header_length)
      buffer_.resize(common_header_length);
    input_->read(buffer_.data(), common_header_length);
    if (input_->eof())
      return finish(f, make_error(ec::end_of_input, "reached end of input"));
    if (input_->fail())
      return finish(f, make_error(ec::format_error,
                                  "failed to read MRT common header"));
    auto ptr = reinterpret_cast<const uint32_t*>(buffer_.data() + 8);
    auto message_length = vast::detail::to_host_order(*ptr);
    // TODO: Where does the RFC specify the maximum length?
    using namespace binary_byte_literals;
    static constexpr size_t max_message_length = 1_MiB;
    if (message_length > max_message_length)
      return finish(f, make_error(ec::format_error,
                                  "MRT message exceeds maximum length",
                                  message_length, max_message_length));
    buffer_.resize(common_header_length + message_length);
    if (!input_->read(buffer_.data() + common_header_length, message_length))
      return finish(f,
                    make_error(ec::format_error, "failed to read MRT message"));
    mrt::record r;
    if (!parser_(buffer_, r))
      return finish(f, make_error(ec::format_error,
                                  "failed to parse MRT message"));
    // Forward to the factory for parsing.
    factory fac{*this, r.header.timestamp};
    if (auto err = visit(fac, r.message))
      return finish(f, err);
    produced += fac.produced();
  }
  return finish(f);
}

} // namespace mrt
} // namespace format
} // namespace vast
