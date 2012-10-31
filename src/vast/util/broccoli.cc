#include "vast/util/broccoli.h"

#include <broccoli.h>
#include <ze/event.h>
#include <ze/io.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {
namespace util {
namespace broccoli {

namespace {

struct bro_val
{
  int type;
  void* value;
};

struct table_data
{
  int key_type;
  int val_type;
  ze::table* table;
};

struct set_data
{
  int key_type;
  ze::set* set;
};

ze::value make_value(int type, void* bro_val);

/// Creates a 0event event from a Bro event.
ze::event make_event(BroEvMeta* meta)
{
  ze::event event;
  event.name(meta->ev_name);
  event.timestamp(ze::time_range::fractional(meta->ev_ts));

  event.reserve(meta->ev_numargs);
  for (int i = 0; i < meta->ev_numargs; ++i)
    event.emplace_back(
        make_value(meta->ev_args[i].arg_type,
                   meta->ev_args[i].arg_data));

  event.shrink_to_fit();
  return event;
}

int table_callback(void *key_data, void *val_data, table_data const* data)
{
  ze::value key = make_value(data->key_type, key_data);
  ze::value value = make_value(data->val_type, val_data);
  auto x = ze::table::value_type(std::move(key), std::move(value));
  data->table->insert(std::move(x));
  return 1;
}

int set_callback(void *key_data, set_data const* data)
{
  ze::value key = make_value(data->key_type, key_data);
  data->set->insert(std::move(key));
  return 1;
}

/// Converts a Broccoli type to the corresponding 0event type.
ze::value_type to_ze_type(int broccoli_type)
{
  switch (broccoli_type)
  {
    default:
      return ze::invalid_type;
    case BRO_TYPE_BOOL:
      return ze::bool_type;
    case BRO_TYPE_INT:
      return ze::int_type;
    case BRO_TYPE_COUNT:
    case BRO_TYPE_COUNTER:
      return ze::uint_type;
    case BRO_TYPE_DOUBLE:
      return ze::double_type;
    case BRO_TYPE_TIME:
      return ze::time_point_type;
    case BRO_TYPE_INTERVAL:
      return ze::time_range_type;
    case BRO_TYPE_STRING:
      return ze::string_type;
    case BRO_TYPE_PATTERN:
      return ze::regex_type;
    case BRO_TYPE_VECTOR:
      return ze::vector_type;
    case BRO_TYPE_SET:
      return ze::set_type;
    case BRO_TYPE_TABLE:
      return ze::table_type;
    case BRO_TYPE_RECORD:
      return ze::record_type;
    case BRO_TYPE_IPADDR:
      return ze::address_type;
    case BRO_TYPE_SUBNET:
      return ze::prefix_type;
    case BRO_TYPE_PORT:
      return ze::port_type;
  }
}

ze::value make_value(int type, void* bro_val)
{
  switch (type)
  {
    default:
      LOG(warn, broccoli) << "type " << type << " does not exist";
      break;
    case BRO_TYPE_UNKNOWN:
      LOG(warn, broccoli) << "unknown broccoli type (" << type << ")";
      break;
    case BRO_TYPE_PATTERN:
    case BRO_TYPE_TIMER:
    case BRO_TYPE_ANY:
    case BRO_TYPE_UNION:
    case BRO_TYPE_LIST:
    case BRO_TYPE_FUNC:
    case BRO_TYPE_FILE:
    case BRO_TYPE_VECTOR:
    case BRO_TYPE_ERROR:
    case BRO_TYPE_PACKET:
      LOG(warn, broccoli) << "unsupported broccoli type (" << type << ")";
      break;
    case BRO_TYPE_BOOL:
      return *static_cast<bool*>(bro_val);
    case BRO_TYPE_INT:
      return *static_cast<int64_t*>(bro_val);
    case BRO_TYPE_COUNT:
    case BRO_TYPE_COUNTER:
      return *static_cast<uint64_t*>(bro_val);
    case BRO_TYPE_DOUBLE:
      return *static_cast<double*>(bro_val);
    case BRO_TYPE_TIME:
        return ze::time_point(ze::time_range::fractional(
                *static_cast<double*>(bro_val)));
    case BRO_TYPE_INTERVAL:
        return ze::time_range::fractional(*static_cast<double*>(bro_val));
    case BRO_TYPE_STRING:
      {
        BroString* s = static_cast<BroString*>(bro_val);
        return {reinterpret_cast<char const*>(s->str_val), s->str_len};
      }
    case BRO_TYPE_PORT:
      {
        BroPort* p = static_cast<BroPort*>(bro_val);
        switch (p->port_proto)
        {
          default:
            LOG(warn, broccoli) << "invalid port type";
            return ze::port(p->port_num, ze::port::unknown);
          case IPPROTO_TCP:
            return ze::port(p->port_num, ze::port::tcp);
          case IPPROTO_UDP:
            return ze::port(p->port_num, ze::port::udp);
          case IPPROTO_ICMP:
            return ze::port(p->port_num, ze::port::icmp);
        }
      }
    case BRO_TYPE_IPADDR:
      {
        BroAddr* addr = static_cast<BroAddr*>(bro_val);
        auto is_v4 = bro_util_is_v4_addr(addr);
        return ze::address(
            addr->addr,
            is_v4 ? ze::address::ipv4 : ze::address::ipv6,
            ze::address::network);
      }
    case BRO_TYPE_SUBNET:
      {
        BroSubnet* sn = static_cast<BroSubnet*>(bro_val);
        auto is_v4 = bro_util_is_v4_addr(&sn->sn_net);
        ze::address addr(
            sn->sn_net.addr,
            is_v4 ? ze::address::ipv4 : ze::address::ipv6,
            ze::address::network);
        return ze::prefix(std::move(addr), sn->sn_width);
      }
    case BRO_TYPE_SET:
      {
        BroSet* bro_set = static_cast<BroSet*>(bro_val);
        if (! bro_set_get_size(bro_set))
          return ze::set();

        // Empty sets have BRO_TYPE_UNKNOWN. At this point, we know
        // that the set has a valid type becuase it is not empty.
        int key_type;
        bro_set_get_type(bro_set, &key_type);

        ze::set set(to_ze_type(key_type));
        set_data data{key_type, &set};
        bro_set_foreach(bro_set, (BroSetCallback)set_callback, &data);

        return set;
      }
    case BRO_TYPE_TABLE:
      {
        BroTable* bro_table = static_cast<BroTable*>(bro_val);
        if (! bro_table_get_size(bro_table))
          return ze::table();

        int key_type, val_type;
        bro_table_get_types(bro_table, &key_type, &val_type);

        ze::table table(to_ze_type(key_type), to_ze_type(val_type));
        table_data data{key_type, val_type, &table};
        bro_table_foreach(bro_table, (BroTableCallback)table_callback, &data);

        return table;
      }
    case BRO_TYPE_RECORD:
      {
        ze::record record;
        BroRecord *rec = static_cast<BroRecord*>(bro_val);
        void* bro_val;
        int bro_val_type = BRO_TYPE_UNKNOWN;
        int cnt = 0;
        while ((bro_val = bro_record_get_nth_val(rec, cnt, &bro_val_type)))
        {
          auto val = make_value(bro_val_type, bro_val);
          record.push_back(std::move(val));
          bro_val_type = BRO_TYPE_UNKNOWN;
          ++cnt;
        }

        return record;
      }
  }

  throw error::broccoli("invalid broccoli type");
}

void free(bro_val const& v)
{
  switch (v.type)
  {
    case BRO_TYPE_STRING:
      delete static_cast<BroString*>(v.value);
      break;
    case BRO_TYPE_IPADDR:
      delete static_cast<BroAddr*>(v.value);
      break;
    case BRO_TYPE_PORT:
      delete static_cast<BroPort*>(v.value);
      break;
    case BRO_TYPE_SUBNET:
      delete static_cast<BroSubnet*>(v.value);
      break;
    case BRO_TYPE_RECORD:
      bro_record_free(static_cast<BroRecord*>(v.value));
      break;
    case BRO_TYPE_TABLE:
      bro_table_free(static_cast<BroTable*>(v.value));
      break;
    case BRO_TYPE_SET:
      bro_set_free(static_cast<BroSet*>(v.value));
      break;
  }
}

struct builder
{
  typedef bro_val result_type;

  bro_val operator()(ze::invalid_value i) const
  {
    return { BRO_TYPE_UNKNOWN, nullptr };
  }

  bro_val operator()(ze::nil_value n) const
  {
    return { BRO_TYPE_UNKNOWN, nullptr };
  }

  bro_val operator()(bool b) const
  {
    return { BRO_TYPE_BOOL, const_cast<bool*>(&b) };
  }

  bro_val operator()(int64_t i) const
  {
    // FIXME: perform narrowing check.
    return { BRO_TYPE_INT, &i };
  }

  bro_val operator()(uint64_t i) const
  {
    // FIXME: perform narrowing check.
    return { BRO_TYPE_INT, &i };
  }

  bro_val operator()(double d) const
  {
    return { BRO_TYPE_DOUBLE, const_cast<double*>(&d) };
  }

  bro_val operator()(ze::string const& s) const
  {
    // Caller must free the memory of the BroString!
    BroString* bs = new BroString;
    auto data = reinterpret_cast<const unsigned char*>(s.data()); 
    bro_string_set_data(bs, data, s.size());
    return { BRO_TYPE_STRING, bs };
  }

  bro_val operator()(ze::regex const& r) const
  {
    throw error::broccoli("Broccoli does not yet support regular expressions");
    return { BRO_TYPE_PATTERN, nullptr };
  }

  bro_val operator()(ze::time_range r) const
  {
    double secs = r.to_double();
    bro_val b;
    b.type = BRO_TYPE_INTERVAL;
    b.value = *reinterpret_cast<double**>(&secs);
    return b;
  }

  bro_val operator()(ze::time_point t) const
  {
    return { BRO_TYPE_TIME, nullptr };
  }

  bro_val operator()(ze::vector const& v) const
  {
    throw error::broccoli("Broccoli does not yet support vectors");
    return { BRO_TYPE_VECTOR, nullptr };
  }

  bro_val operator()(ze::set const& s) const
  {
    // Caller must free the memory of the BroSet!
    BroSet* set = bro_set_new();
    for (auto const& x : s)
    {
      bro_val bv = ze::value::visit(x, *this);
      bro_set_insert(set, bv.type, bv.value);
      free(bv);
    }
    return { BRO_TYPE_SET, set };
  }

  bro_val operator()(ze::table const& t) const
  {
    // Caller must free the memory of the BroTable!
    BroTable* table = bro_table_new();
    for (auto const& x : t)
    {
      bro_val key = ze::value::visit(x.first, *this);
      bro_val val = ze::value::visit(x.second, *this);

      // If the table key is a compound type (i.e., record), we need to
      // use BRO_TYPE_LIST instead of BRO_TYPE_RECORD.
      bro_table_insert(
          table,
          key.type == BRO_TYPE_RECORD ? BRO_TYPE_LIST : key.type,
          key.value, val.type, val.value);

      free(key);
      free(val);
    }

    return { BRO_TYPE_TABLE, table };
  }

  bro_val operator()(ze::record const& r) const
  {
    // Caller must free the memory of the BroRecord!
    BroRecord* rec = bro_record_new();
    for (auto const& val : r)
    {
      bro_val bv = ze::value::visit(val, *this);
      bro_record_add_val(rec, NULL, bv.type, NULL, &bv.value);
      free(bv);
    }
    return { BRO_TYPE_RECORD, rec };
  }

  bro_val operator()(ze::address const& a) const
  {
    // Caller must free the memory of the BroAddr!
    BroAddr* addr = new BroAddr;
    std::copy(a.data().begin(), a.data().end(),
              reinterpret_cast<uint8_t*>(&addr->addr));
    return { BRO_TYPE_IPADDR, addr };
  }

  bro_val operator()(ze::prefix const& p) const
  {
    // Caller must free the memory of the BroSubnet!
    BroSubnet* bs = new BroSubnet;
    bs->sn_width = p.length();
    auto net = operator()(p.network());
    auto addr = reinterpret_cast<BroAddr*>(net.value);
    std::copy(addr, addr + sizeof(BroAddr), &bs->sn_net);
    free(net);
    return { BRO_TYPE_PORT, bs };
  }

  bro_val operator()(ze::port const& p) const
  {
    // Caller must free the memory of the BroPort!
    BroPort* bp = new BroPort;
    bp->port_num = p.number();
    switch (p.type())
    {
      default:
        throw error::broccoli("unsupported port type");
      case ze::port::tcp:
        bp->port_proto = IPPROTO_TCP;
        break;
      case ze::port::udp:
        bp->port_proto = IPPROTO_UDP;
        break;
      case ze::port::icmp:
        bp->port_proto = IPPROTO_ICMP;
        break;
    }
    return { BRO_TYPE_PORT, bp };
  }
};

/// Creates a Bro event from a 0event event.
BroEvent* make_event(ze::event const& event)
{
  DBG(broccoli) << "building broccoli event " << event.name();
  BroEvent* bro_event = bro_event_new(event.name().data());
  if (! bro_event)
  {
    LOG(error, broccoli) << "could not create bro_event " << event.name();
    throw error::broccoli("bro_event_new");
  }

  for (auto& arg : event)
  {
    DBG(broccoli) << "adding argument: " << arg;
    bro_val val = ze::value::visit(arg, builder());
    bro_event_add_val(bro_event, val.type, NULL, val.value);
    free(val);
  }

  return bro_event;
}

/// The Broccoli event callback that creates a 0event from a Broccoli event.
void callback(BroConn* bc, void* user_data, BroEvMeta* meta)
{
  try
  {
    ze::event event = make_event(meta);
    auto f = static_cast<event_handler*>(user_data);
    (*f)(std::move(event));
  }
  catch (ze::exception const& e)
  {
    LOG(error, broccoli)
      << "could not create ze::event from broccoli event '"
      << meta->ev_name << "' (" << e.what() << ')';
  }
  catch (error::broccoli const& e)
  {
    LOG(error, broccoli)
      << "error with broccoli event '"
      << meta->ev_name << "' (" << e.what() << ')';
  }
}

} // namespace

using namespace cppa;

void init(bool messages, bool calltrace)
{
  if (calltrace)
  {
    bro_debug_calltrace = 1;
    LOG(verbose, broccoli) << "enabling call trace debugging";
  }

  if (messages)
  {
    bro_debug_messages = 1;
    LOG(verbose, broccoli) << "enabling extra debug messages";
  }

  LOG(verbose, broccoli) << "initializing SSL context";
  BroCtx ctx;
  bro_ctx_init(&ctx);
  bro_init(&ctx);
}

connection::connection(
    cppa::network::input_stream_ptr in,
    cppa::network::output_stream_ptr out)
  : in_(std::move(in))
  , out_(std::move(out))
{
  auto fd = in_->read_handle();
  bc_ = bro_conn_new_socket(fd, BRO_CFLAG_DONTCACHE);
  if (bc_ < 0)
    throw error::broccoli("bro_conn_new_socket");

  init_state = (
    on(atom("subscribe"), arg_match) >> [=](std::string const& event)
    {
      bro_event_registry_add_compact(bc_,
                                     event.data(),
                                     &callback,
                                     &event_handler_);
    },
    on(atom("start"), arg_match) >> [=](actor_ptr receiver)
    {
      event_handler_ = [=](ze::event e) { send(receiver, std::move(e)); };
      bro_event_registry_request(bc_);
      if (! bro_conn_connect(bc_))
        throw error::broccoli("bro_conn_connect()");

      send(self, atom("io"));
    },
    on(atom("io")) >> [=]
    {
      if (! bc_)
      {
        send(self, atom("shutdown"));
        return;
      }

      DBG(broccoli) << "polling fd";
      if (poll(fd))
        bro_conn_process_input(bc_);

      self << last_dequeued();
    },
    on_arg_match >> [=](std::vector<uint8_t> const& raw)
    {
      LOG(debug, broccoli) << "sending raw event of size " << raw.size();
      bro_event_send_raw(bc_, raw.data(), raw.size());
    },
    on_arg_match >> [=](ze::event const& event)
    {
      auto bro_event = make_event(event);
      if (! bro_event_send(bc_, bro_event))
        LOG(error, broccoli)
          << "broccoli @ " << id() << " could not send event: " << event;

      bro_event_free(bro_event);
    },
    on(atom("shutdown")) >> [=]
    {
      bro_conn_delete(bc_);
      bc_ = nullptr;
      quit();
    });
}

} // namespace broccoli
} // namespace vast
} // namespace util
