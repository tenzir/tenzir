#include "vast/format/mrt.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"

namespace vast {
namespace format {
namespace mrt {

namespace {

struct factory {
  factory(std::queue<event>& events, const reader::types& types, uint32_t ts)
    : events_{events}, types_{types} {
    using namespace std::chrono;
    auto since_epoch = duration<uint32_t>{ts};
    timestamp_ = timestamp{duration_cast<timespan>(since_epoch)};
  }

  void operator()(none) const {
    // nop
  }

  void operator()(table_dump_v2::peer_index_table& x) {
    for (auto i = 0u; i < x.peer_count; i++) {
      event e{{
        vector{i,
               x.peer_entries[i].peer_bgp_id,
               x.peer_entries[i].peer_ip_address,
               x.peer_entries[i].peer_as},
        types_.table_dump_v2_peer_entry_type
      }};
      e.timestamp(timestamp_);
      events_.push(e);
    }
  }

  void operator()(table_dump_v2::rib_afi_safi& x) {
    for (auto i = 0u; i < x.entries.size(); i++) {
      std::vector<vast::data> as_path;
      count origin_as;
      for (auto j = 0u; j < x.entries[i].bgp_attributes.as_path.size(); j++) {
        origin_as = x.entries[i].bgp_attributes.as_path[j];
        as_path.push_back(origin_as);
      }
      std::vector<vast::data> communities;
      count community;
      for (auto j = 0u; j < x.entries[i].bgp_attributes.communities.size();
           j++) {
        community = x.entries[i].bgp_attributes.communities[j];
        communities.push_back(community);
      }
      event e{{
        vector{x.entries[i].peer_index,
               x.header.prefix[0],
               std::move(as_path),
               origin_as,
               x.entries[i].bgp_attributes.origin,
               x.entries[i].bgp_attributes.next_hop,
               x.entries[i].bgp_attributes.local_pref,
               x.entries[i].bgp_attributes.multi_exit_disc,
               communities,
               x.entries[i].bgp_attributes.atomic_aggregate,
               x.entries[i].bgp_attributes.aggregator_as,
               x.entries[i].bgp_attributes.aggregator_ip},
        types_.table_dump_v2_rib_entry_type
      }};
      e.timestamp(timestamp_);
      events_.push(e);
    }
  }

  void operator()(bgp4mp::state_change& x) {
    event e{{
      vector{x.peer_ip_address,
             x.peer_as_number,
             x.old_state,
             x.new_state},
      types_.bgp4mp_state_change_type
    }};
    e.timestamp(timestamp_);
    events_.push(e);
  }

  void operator()(bgp4mp::message& x) {
    if (auto open = get_if<bgp::open>(x.message.message)) {
      event e{{
        vector{open->version,
               open->my_autonomous_system,
               open->hold_time,
               open->bgp_identifier},
        types_.bgp4mp_open_type
      }};
      e.timestamp(timestamp_);
      events_.push(e);
    } else if (auto update = get_if<bgp::update>(x.message.message)) {
      std::vector<vast::data> as_path;
      count origin_as;
      for (auto i = 0u; i < update->path_attributes.as_path.size(); i++) {
        origin_as = update->path_attributes.as_path[i];
        as_path.push_back(origin_as);
      }
      std::vector<vast::data> communities;
      count community;
      for (auto i = 0u; i < update->path_attributes.communities.size(); i++) {
        community = update->path_attributes.communities[i];
        communities.push_back(community);
      }
      for (auto i = 0u; i < update->withdrawn_routes.size(); i++) {
        event e{{
          vector{x.peer_ip_address,
                 x.peer_as_number,
                 update->withdrawn_routes[i]},
          types_.bgp4mp_update_withdraw_type
        }};
        e.timestamp(timestamp_);
        events_.push(e);
      }
      for (auto i = 0u; i < update->path_attributes.mp_reach_nlri.size(); i++) {
        event e{{
          vector{x.peer_ip_address,
                 x.peer_as_number,
                 update->path_attributes.mp_reach_nlri[i],
                 as_path,
                 origin_as,
                 update->path_attributes.origin,
                 update->path_attributes.next_hop,
                 update->path_attributes.local_pref,
                 update->path_attributes.multi_exit_disc,
                 communities,
                 update->path_attributes.atomic_aggregate,
                 update->path_attributes.aggregator_as,
                 update->path_attributes.aggregator_ip},
          types_.bgp4mp_update_announcement_type
        }};
        e.timestamp(timestamp_);
        events_.push(e);
      }
      for (auto i = 0u; i < update->path_attributes.mp_unreach_nlri.size();
           i++) {
        event e{{
          vector{x.peer_ip_address,
                 x.peer_as_number,
                 update->path_attributes.mp_unreach_nlri[i]},
          types_.bgp4mp_update_withdraw_type
        }};
        e.timestamp(timestamp_);
        events_.push(e);
      }
      for (auto i = 0u;
           i < update->network_layer_reachability_information.size(); i++) {
        event e{{
          vector{x.peer_ip_address,
                 x.peer_as_number,
                 update->network_layer_reachability_information[i],
                 as_path,
                 origin_as,
                 update->path_attributes.origin,
                 update->path_attributes.next_hop,
                 update->path_attributes.local_pref,
                 update->path_attributes.multi_exit_disc,
                 communities,
                 update->path_attributes.atomic_aggregate,
                 update->path_attributes.aggregator_as,
                 update->path_attributes.aggregator_ip},
          types_.bgp4mp_update_announcement_type
        }};
        e.timestamp(timestamp_);
        events_.push(e);
      }
    } else if (auto notification =
               get_if<bgp::notification>(x.message.message)) {
      event e{{
        vector{notification->error_code,
               notification->error_subcode},
        types_.bgp4mp_notification_type
      }};
      e.timestamp(timestamp_);
      events_.push(e);
    }
  }

  void operator()(bgp4mp::message_as4& x) {
    if (auto open = get_if<bgp::open>(x.message.message)) {
      event e{{
        vector{open->version,
               open->my_autonomous_system,
               open->hold_time,
               open->bgp_identifier},
        types_.bgp4mp_open_type
      }};
      e.timestamp(timestamp_);
      events_.push(e);
    } else if (auto update = get_if<bgp::update>(x.message.message)) {
      std::vector<vast::data> as_path;
      count origin_as;
      for (auto i = 0u; i < update->path_attributes.as_path.size(); i++) {
        origin_as = update->path_attributes.as_path[i];
        as_path.push_back(origin_as);
      }
      std::vector<vast::data> communities;
      count community;
      for (auto i = 0u; i < update->path_attributes.communities.size(); i++) {
        community = update->path_attributes.communities[i];
        communities.push_back(community);
      }
      for (auto i = 0u; i < update->withdrawn_routes.size(); i++) {
        event e{{
          vector{x.peer_ip_address,
                 x.peer_as_number,
                 update->withdrawn_routes[i]},
          types_.bgp4mp_update_withdraw_type
        }};
        e.timestamp(timestamp_);
        events_.push(e);
      }
      for (auto i = 0u; i < update->path_attributes.mp_reach_nlri.size(); i++) {
        event e{{
          vector{x.peer_ip_address,
                 x.peer_as_number,
                 update->path_attributes.mp_reach_nlri[i],
                 as_path,
                 origin_as,
                 update->path_attributes.origin,
                 update->path_attributes.next_hop,
                 update->path_attributes.local_pref,
                 update->path_attributes.multi_exit_disc,
                 communities,
                 update->path_attributes.atomic_aggregate,
                 update->path_attributes.aggregator_as,
                 update->path_attributes.aggregator_ip},
          types_.bgp4mp_update_announcement_type
        }};
        e.timestamp(timestamp_);
        events_.push(e);
      }
      for (auto i = 0u; i < update->path_attributes.mp_unreach_nlri.size();
           i++) {
        event e{{
          vector{x.peer_ip_address,
                 x.peer_as_number,
                 update->path_attributes.mp_unreach_nlri[i]},
          types_.bgp4mp_update_withdraw_type
        }};
        e.timestamp(timestamp_);
        events_.push(e);
      }
      for (auto i = 0u;
           i < update->network_layer_reachability_information.size(); i++) {
        event e{{
          vector{x.peer_ip_address,
                 x.peer_as_number,
                 update->network_layer_reachability_information[i],
                 as_path,
                 origin_as,
                 update->path_attributes.origin,
                 update->path_attributes.next_hop,
                 update->path_attributes.local_pref,
                 update->path_attributes.multi_exit_disc,
                 communities,
                 update->path_attributes.atomic_aggregate,
                 update->path_attributes.aggregator_as,
                 update->path_attributes.aggregator_ip},
          types_.bgp4mp_update_announcement_type
        }};
        e.timestamp(timestamp_);
        events_.push(e);
      }
    } else if (auto notification =
               get_if<bgp::notification>(x.message.message)) {
      event e{{
        vector{notification->error_code,
               notification->error_subcode},
        types_.bgp4mp_notification_type
      }};
      e.timestamp(timestamp_);
      events_.push(e);
    }
  }

  void operator()(bgp4mp::state_change_as4& x) {
    event e{{
      vector{x.peer_ip_address,
             x.peer_as_number,
             x.old_state,
             x.new_state},
      types_.bgp4mp_state_change_type
    }};
    e.timestamp(timestamp_);
    events_.push(e);
  }

  std::queue<event>& events_;
  vast::timestamp timestamp_;
  reader::types types_;
};

} // namespace anonymous

reader::reader(std::unique_ptr<std::istream> input) : input_{std::move(input)} {
  VAST_ASSERT(input_);
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

expected<event> reader::read() {
  if (!events_.empty()) {
    auto x = std::move(events_.front());
    events_.pop();
    return x;
  }
  // We have to read the input block-wise in a manner that respects the
  // protocol framing.
  static constexpr size_t common_header_length = 12;
  if (buffer_.size() < common_header_length)
    buffer_.resize(common_header_length);
  input_->read(buffer_.data(), common_header_length);
  if (input_->eof())
    return make_error(ec::end_of_input, "reached end of input");
  if (input_->fail())
    return make_error(ec::format_error, "failed to read MRT common header");
  auto ptr = reinterpret_cast<const uint32_t*>(buffer_.data() + 8);
  auto message_length = vast::detail::to_host_order(*ptr);
  // TODO: Where does the RFC specify the maximum length?
  static constexpr size_t max_message_length = 1 << 20;
  if (message_length > max_message_length)
    return make_error(ec::format_error, "MRT message exceeds maximum length",
                      message_length, max_message_length);
  buffer_.resize(common_header_length + message_length);
  if (!input_->read(buffer_.data() + common_header_length, message_length))
    return make_error(ec::format_error, "failed to read MRT message");
  mrt::record r;
  if (!parser_(buffer_, r))
    return make_error(ec::format_error, "failed to parse MRT message");
  // Convert
  // Take the timestamp from the Common Header as event time.
  visit(factory{events_, types_, r.header.timestamp}, r.message);
  if (!events_.empty()) {
    auto x = std::move(events_.front());
    events_.pop();
    return x;
  }
  return no_error;
}

expected<void> reader::schema(vast::schema const& sch) {
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

expected<vast::schema> reader::schema() const {
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

} // namespace mrt
} // namespace format
} // namespace vast
