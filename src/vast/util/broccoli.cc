#include "vast/util/broccoli.h"

#include <broccoli.h>
#include <vast/event.h>
#include <vast/io.h>
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
  table* tbl;
};

struct set_data
{
  int key_type;
  set* st;
};

value make_value(int type, void* bro_val);

/// Creates a VAST event from a Bro event.
event make_event(BroEvMeta* meta)
{
  event e;
  e.name(meta->ev_name);
  e.timestamp(time_range::fractional(meta->ev_ts));

  e.reserve(meta->ev_numargs);
  for (int i = 0; i < meta->ev_numargs; ++i)
    e.emplace_back(
        make_value(meta->ev_args[i].arg_type, meta->ev_args[i].arg_data));

  e.shrink_to_fit();
  return e;
}

int table_callback(void *key_data, void *val_data, table_data const* data)
{
  value key = make_value(data->key_type, key_data);
  value val = make_value(data->val_type, val_data);
  auto x = table::type_tag(std::move(key), std::move(val));
  data->tbl->insert(std::move(x));
  return 1;
}

int set_callback(void *key_data, set_data const* data)
{
  value key = make_value(data->key_type, key_data);
  data->st->insert(std::move(key));
  return 1;
}

/// Converts a Broccoli type to the corresponding VAST type.
type_tag to_vast_type(int broccoli_type)
{
  switch (broccoli_type)
  {
    default:
      return invalid_type;
    case BRO_TYPE_BOOL:
      return bool_type;
    case BRO_TYPE_INT:
      return int_type;
    case BRO_TYPE_COUNT:
    case BRO_TYPE_COUNTER:
      return uint_type;
    case BRO_TYPE_DOUBLE:
      return double_type;
    case BRO_TYPE_TIME:
      return time_point_type;
    case BRO_TYPE_INTERVAL:
      return time_range_type;
    case BRO_TYPE_STRING:
      return string_type;
    case BRO_TYPE_PATTERN:
      return regex_type;
    case BRO_TYPE_VECTOR:
      return vector_type;
    case BRO_TYPE_SET:
      return set_type;
    case BRO_TYPE_TABLE:
      return table_type;
    case BRO_TYPE_RECORD:
      return record_type;
    case BRO_TYPE_IPADDR:
      return address_type;
    case BRO_TYPE_SUBNET:
      return prefix_type;
    case BRO_TYPE_PORT:
      return port_type;
  }
}

value make_value(int type, void* bro_val)
{
  switch (type)
  {
    default:
      VAST_LOG_WARN("type " << type << " does not exist");
      break;
    case BRO_TYPE_UNKNOWN:
      VAST_LOG_WARN("unknown broccoli type (" << type << ")");
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
      VAST_LOG_WARN("unsupported broccoli type (" << type << ")");
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
        return time_point(time_range::fractional(
                *static_cast<double*>(bro_val)));
    case BRO_TYPE_INTERVAL:
        return time_range::fractional(*static_cast<double*>(bro_val));
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
            VAST_LOG_WARN("invalid port type");
            return port(p->port_num, port::unknown);
          case IPPROTO_TCP:
            return port(p->port_num, port::tcp);
          case IPPROTO_UDP:
            return port(p->port_num, port::udp);
          case IPPROTO_ICMP:
            return port(p->port_num, port::icmp);
        }
      }
    case BRO_TYPE_IPADDR:
      {
        BroAddr* addr = static_cast<BroAddr*>(bro_val);
        auto is_v4 = bro_util_is_v4_addr(addr);
        return address(addr->addr,
                       is_v4 ? address::ipv4 : address::ipv6,
                       address::network);
      }
    case BRO_TYPE_SUBNET:
      {
        BroSubnet* sn = static_cast<BroSubnet*>(bro_val);
        auto is_v4 = bro_util_is_v4_addr(&sn->sn_net);
        address addr(sn->sn_net.addr,
                     is_v4 ? address::ipv4 : address::ipv6,
                     address::network);
        return prefix(std::move(addr), sn->sn_width);
      }
    case BRO_TYPE_SET:
      {
        BroSet* bro_set = static_cast<BroSet*>(bro_val);
        if (! bro_set_get_size(bro_set))
          return set();

        // Empty sets have BRO_TYPE_UNKNOWN. At this point, we know
        // that the set has a valid type becuase it is not empty.
        int key_type;
        bro_set_get_type(bro_set, &key_type);

        set s(to_vast_type(key_type));
        set_data data{key_type, &s};
        bro_set_foreach(bro_set, (BroSetCallback)set_callback, &data);

        return s;
      }
    case BRO_TYPE_TABLE:
      {
        BroTable* bro_table = static_cast<BroTable*>(bro_val);
        if (! bro_table_get_size(bro_table))
          return table();

        int key_type, val_type;
        bro_table_get_types(bro_table, &key_type, &val_type);

        table tbl(to_vast_type(key_type), to_vast_type(val_type));
        table_data data{key_type, val_type, &tbl};
        bro_table_foreach(bro_table, (BroTableCallback)table_callback, &data);

        return tbl;
      }
    case BRO_TYPE_RECORD:
      {
        record rec;
        BroRecord *rec = static_cast<BroRecord*>(bro_val);
        void* bro_val;
        int bro_val_type = BRO_TYPE_UNKNOWN;
        int cnt = 0;
        while ((bro_val = bro_record_get_nth_val(rec, cnt, &bro_val_type)))
        {
          auto val = make_value(bro_val_type, bro_val);
          rec.push_back(std::move(val));
          bro_val_type = BRO_TYPE_UNKNOWN;
          ++cnt;
        }

        return rec;
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

  bro_val operator()(invalid_value i) const
  {
    return { BRO_TYPE_UNKNOWN, nullptr };
  }

  bro_val operator()(nil_value n) const
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

  bro_val operator()(string const& s) const
  {
    // Caller must free the memory of the BroString!
    BroString* bs = new BroString;
    auto data = reinterpret_cast<const unsigned char*>(s.data());
    bro_string_set_data(bs, data, s.size());
    return { BRO_TYPE_STRING, bs };
  }

  bro_val operator()(regex const& r) const
  {
    throw error::broccoli("Broccoli does not yet support regular expressions");
    return { BRO_TYPE_PATTERN, nullptr };
  }

  bro_val operator()(time_range r) const
  {
    double secs = r.to_double();
    bro_val b;
    b.type = BRO_TYPE_INTERVAL;
    b.value = *reinterpret_cast<double**>(&secs);
    return b;
  }

  bro_val operator()(time_point t) const
  {
    return { BRO_TYPE_TIME, nullptr };
  }

  bro_val operator()(vector const& v) const
  {
    throw error::broccoli("Broccoli does not yet support vectors");
    return { BRO_TYPE_VECTOR, nullptr };
  }

  bro_val operator()(set const& s) const
  {
    // Caller must free the memory of the BroSet!
    BroSet* bs = bro_set_new();
    for (auto const& x : s)
    {
      bro_val bv = value::visit(x, *this);
      bro_set_insert(bs, bv.type, bv.value);
      free(bv);
    }
    return { BRO_TYPE_SET, bs };
  }

  bro_val operator()(table const& t) const
  {
    // Caller must free the memory of the BroTable!
    BroTable* tbl = bro_table_new();
    for (auto const& x : t)
    {
      bro_val key = value::visit(x.first, *this);
      bro_val val = value::visit(x.second, *this);

      // If the table key is a compound type (i.e., record), we need to
      // use BRO_TYPE_LIST instead of BRO_TYPE_RECORD.
      bro_table_insert(
          tbl,
          key.type == BRO_TYPE_RECORD ? BRO_TYPE_LIST : key.type,
          key.value, val.type, val.value);

      free(key);
      free(val);
    }

    return { BRO_TYPE_TABLE, tbl };
  }

  bro_val operator()(record const& r) const
  {
    // Caller must free the memory of the BroRecord!
    BroRecord* rec = bro_record_new();
    for (auto const& val : r)
    {
      bro_val bv = value::visit(val, *this);
      bro_record_add_val(rec, NULL, bv.type, NULL, &bv.value);
      free(bv);
    }
    return { BRO_TYPE_RECORD, rec };
  }

  bro_val operator()(address const& a) const
  {
    // Caller must free the memory of the BroAddr!
    BroAddr* addr = new BroAddr;
    std::copy(a.data().begin(), a.data().end(),
              reinterpret_cast<uint8_t*>(&addr->addr));
    return { BRO_TYPE_IPADDR, addr };
  }

  bro_val operator()(prefix const& p) const
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

  bro_val operator()(port const& p) const
  {
    // Caller must free the memory of the BroPort!
    BroPort* bp = new BroPort;
    bp->port_num = p.number();
    switch (p.type())
    {
      default:
        throw error::broccoli("unsupported port type");
      case port::tcp:
        bp->port_proto = IPPROTO_TCP;
        break;
      case port::udp:
        bp->port_proto = IPPROTO_UDP;
        break;
      case port::icmp:
        bp->port_proto = IPPROTO_ICMP;
        break;
    }
    return { BRO_TYPE_PORT, bp };
  }
};

/// Creates a Bro event from a VAST event.
BroEvent* make_event(event const& e)
{
  VAST_LOG_DEBUG("building broccoli event " << e.name());
  BroEvent* bro_event = bro_event_new(e.name().data());
  if (! bro_event)
  {
    VAST_LOG_ERROR("could not create bro_event " << e.name());
    throw error::broccoli("bro_event_new");
  }

  for (auto& a : e)
  {
    VAST_LOG_DEBUG("adding argument: " << a);
    bro_val val = value::visit(a, builder());
    bro_event_add_val(bro_event, val.type, NULL, val.value);
    free(val);
  }

  return bro_event;
}

/// The Broccoli event callback that creates a VAST from a Broccoli event.
void callback(BroConn* bc, void* user_data, BroEvMeta* meta)
{
  try
  {
    event e = make_event(meta);
    auto f = static_cast<event_handler*>(user_data);
    (*f)(std::move(e));
  }
  catch (error::broccoli const& e)
  {
    VAST_LOG_ERROR("error with broccoli event '" <<
                   meta->ev_name << "' (" << e.what() << ')');
  }
  catch (exception const& e)
  {
    VAST_LOG_ERROR("could not create VAST event from broccoli event '" <<
                   meta->ev_name << "' (" << e.what() << ')');
  }
}

} // namespace

using namespace caf;

void init(bool messages, bool calltrace)
{
  if (calltrace)
  {
    bro_debug_calltrace = 1;
    VAST_LOG_VERBOSE("enabling call trace debugging");
  }

  if (messages)
  {
    bro_debug_messages = 1;
    VAST_LOG_VERBOSE("enabling extra debug messages");
  }

  VAST_LOG_VERBOSE("initializing SSL context");
  BroCtx ctx;
  bro_ctx_init(&ctx);
  bro_init(&ctx);
}

connection::connection(caf::io::input_stream_ptr in,
                       caf::io::output_stream_ptr out)
  : in_{std::move(in)},
    out_{std::move(out)}
{
}

void connection::on_exit()
{
  if (bc_)
    bro_conn_delete(bc_);
  actor<connection>::on_exit();
}

void connection::act()
{
  auto fd = in_->read_handle();
  bc_ = bro_conn_new_socket(fd, BRO_CFLAG_DONTCACHE);
  if (bc_ < 0)
  {
    VAST_LOG_ACTOR_ERROR("failed to create socket (bro_conn_new_socket)");
    quit(exit::error);
    return;
  }

  become(
    on(atom("subscribe"), arg_match) >> [=](std::string const& event)
    {
      bro_event_registry_add_compact(bc_,
                                     event.data(),
                                     &callback,
                                     &event_handler_);
    },
    on(atom("start"), arg_match) >> [=](actor_ptr receiver)
    {
      event_handler_ = [=](event e) { send(receiver, std::move(e)); };
      bro_event_registry_request(bc_);
      if (! bro_conn_connect(bc_))
      {
        VAST_LOG_ACTOR_ERROR("failed to connect (bro_conn_connect)");
        quit(exit::error);
        return;
      }
      send(self, atom("io"));
    },
    on(atom("io")) >> [=]
    {
      if (! bc_)
      {
        VAST_LOG_ACTOR_ERROR("noticed invalid connection");
        quit(exit::error);
        return;
      }

      VAST_LOG_ACTOR_DEBUG("polls fd");
      if (poll(fd))
        bro_conn_process_input(bc_);

      self << last_dequeued();
    },
    on_arg_match >> [=](std::vector<uint8_t> const& raw)
    {
      VAST_LOG_ACTOR_DEBUG("sends raw event of size " << raw.size());
      bro_event_send_raw(bc_, raw.data(), raw.size());
    },
    on_arg_match >> [=](event const& e)
    {
      auto bro_event = make_event(e);
      if (! bro_event_send(bc_, bro_event))
        VAST_LOG_ACTOR_ERROR("could not send event: " << e);
      bro_event_free(bro_event);
    });
}

char const* connection::description() const
{
  return "broccoli";
}

} // namespace broccoli
} // namespace vast
} // namespace util
