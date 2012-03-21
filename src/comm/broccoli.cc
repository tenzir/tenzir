#include "comm/broccoli.h"

#include <ze/event.h>
#include <ze/type/container.h>
#include "comm/connection.h"
#include "comm/error.h"
#include "util/logger.h"
#include "util/make_unique.h"

namespace vast {
namespace comm {

broccoli::broccoli(connection_ptr const& conn, event_handler const& handler)
  : connection_(conn)
  , event_handler_(handler)
  , strand_(
      std::make_unique<boost::asio::strand>(
          connection_->socket().get_io_service()))
  , bc_(nullptr)
{
    assert(initialized);

    auto& socket = connection_->socket();
    boost::asio::ip::tcp::socket::non_blocking_io non_blocking_io(true);
    socket.io_control(non_blocking_io);

    LOG(debug, broccoli) << *connection_ << ": creating broccoli handle";
    bc_ = bro_conn_new_socket(socket.native(), BRO_CFLAG_DONTCACHE);
    if (bc_ < 0)
        THROW(error::broccoli() << error::broccoli_func("bro_conn_new_socket"));
}

broccoli::broccoli(broccoli&& other)
  : connection_(std::move(other.connection_))
  , event_handler_(std::move(other.event_handler_))
  , error_handler_(std::move(other.error_handler_))
  , strand_(std::move(other.strand_))
  , bc_(other.bc_)
{
    other.bc_ = nullptr;
}

broccoli& broccoli::operator=(broccoli other)
{
    swap(other);
    return *this;
}


broccoli::~broccoli()
{
    if (bc_)
        bro_conn_delete(bc_);
}

void broccoli::swap(broccoli& other)
{
    using std::swap;
    swap(connection_, other.connection_);
    swap(event_handler_, other.event_handler_);
    swap(error_handler_, other.error_handler_);
    swap(strand_, other.strand_);
    swap(bc_, other.bc_);
}


void broccoli::subscribe(std::string const& event)
{
    bro_event_registry_add_compact(bc_,
                                   event.c_str(),
                                   &callback,
                                   &event_handler_);
}

void broccoli::send(core::bytes const& raw)
{
    LOG(debug, broccoli)
        << "sending raw event of size " << raw.size();

    bro_event_send_raw(bc_, raw.data(), raw.size());
}

void broccoli::send(ze::event const& event)
{
    auto bro_event = reverse_factory::make_event(event);
    if (! bro_event_send(bc_, bro_event))
        LOG(error, broccoli)
            << *connection_ << ": error sending event " << event.name();

    bro_event_free(bro_event);
}

void broccoli::run(conn_handler const& error_handler)
{
    error_handler_ = error_handler;

    bro_event_registry_request(bc_);

    if (! bro_conn_connect(bc_))
    {
        LOG(error, broccoli) << *connection_ << ": unable to attach broccoli";
        THROW(error::broccoli() << error::broccoli_func("bro_conn_connect"));
    }
    LOG(debug, broccoli) << *connection_ << ": successfully attached to socket";

    async_read();
}

connection_ptr broccoli::connection() const
{
    return connection_;
}


int broccoli::factory::table_callback(void *key_data, void *val_data,
                                      table_data const* data)
{
    ze::value key = make_value(data->key_type, key_data);
    ze::value value = make_value(data->val_type, val_data);
    auto x = ze::table::value_type(std::move(key), std::move(value));
    data->table->insert(std::move(x));

    return 1;
}

int broccoli::factory::set_callback(void *key_data, set_data const* data)
{
    ze::value key = make_value(data->key_type, key_data);
    data->set->insert(std::move(data));

    return 1;
}

void broccoli::factory::make_event(ze::event& event, BroEvMeta* meta)
{
    event.name(meta->ev_name);
    event.timestamp(meta->ev_ts);

    event.args().reserve(meta->ev_numargs);
    for (int i = 0; i < meta->ev_numargs; ++i)
    {
        ze::value val = make_value(meta->ev_args[i].arg_type,
                                   meta->ev_args[i].arg_data);
        event.args().push_back(std::move(val));
    }
    event.args().shrink_to_fit();
}

ze::value broccoli::factory::make_value(int type, void* bro_val)
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
            return {*static_cast<bool*>(bro_val)};
        case BRO_TYPE_INT:
        case BRO_TYPE_COUNT:
        case BRO_TYPE_COUNTER:
            return {*static_cast<bool*>(bro_val)};
        case BRO_TYPE_DOUBLE:
        case BRO_TYPE_TIME: // TODO: make time a first-class value in 0event.
        case BRO_TYPE_INTERVAL:
            return {*static_cast<bool*>(bro_val)};
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
// TODO: Install recent Broccoli.
//        case BRO_TYPE_IPADDR:
//            BroAddr* addr = static_cast<BroAddr*>(bro_val);
//            return ze::address(
//                addr->addr,
//                (addr->size == 1) ? ze::address::ipv4 : ze::address::ipv6,
//                ze::address::network);
//        case BRO_TYPE_SUBNET:
//            {
//                BroSubnet* sn = static_cast<BroSubnet*>(bro_val);
//                ze::address addr(
//                    sn->sn_net->addr,
//                    (sn->sn_net->size == 1) ?
//                        ze::address::ipv4 : ze::address::ipv6,
//                    ze::address::network);
//                return ze::prefix(std::move(addr), sn->sn_width);
//            }
        case BRO_TYPE_SET:
            {
                ze::set set;
                BroSet* bro_set = static_cast<BroSet*>(bro_val);
                if (! bro_set_get_size(bro_set))
                    return set;

                // Empty sets have BRO_TYPE_UNKNOWN. At this point, we know
                // that the set has a valid type becuase it is not empty.
                int key_type;
                bro_set_get_type(bro_set, &key_type);

                set_data data{key_type, &set};
                bro_set_foreach(bro_set, (BroSetCallback)set_callback, &data);

                return set;
            }
        case BRO_TYPE_TABLE:
            {
                ze::table table;
                BroTable* bro_table = static_cast<BroTable*>(bro_val);
                if (! bro_table_get_size(bro_table))
                    return table;

                int key_type, val_type;
                bro_table_get_types(bro_table, &key_type, &val_type);

                table_data data{key_type,val_type, &table};
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

    THROW(error::broccoli() << error::broccoli_type(type));
}

struct broccoli::reverse_factory::builder
{
    typedef bro_val result_type;

    result_type operator()(ze::invalid_value i) const;
    result_type operator()(ze::nil_value n) const;
    result_type operator()(bool b) const;
    result_type operator()(int64_t i) const;
    result_type operator()(uint64_t i) const;
    result_type operator()(double d) const;
    result_type operator()(ze::string const& s) const;
    result_type operator()(ze::vector const& v) const;
    result_type operator()(ze::set const& s) const;
    result_type operator()(ze::table const& t) const;
    result_type operator()(ze::record const& r) const;
    result_type operator()(ze::address const& a) const;
    result_type operator()(ze::prefix const& s) const;
    result_type operator()(ze::port const& p) const;
};

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::invalid_value i) const
{
    return { BRO_TYPE_UNKNOWN, nullptr };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::nil_value n) const
{
    return { BRO_TYPE_UNKNOWN, nullptr };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(bool b) const
{
    return { BRO_TYPE_BOOL, const_cast<bool*>(&b) };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(int64_t i) const
{
    // FIXME: perform narrowing check.
    return { BRO_TYPE_INT, &i };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(uint64_t i) const
{
    // FIXME: perform narrowing check.
    return { BRO_TYPE_INT, &i };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(double d) const
{
    return { BRO_TYPE_DOUBLE, const_cast<double*>(&d) };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::string const& s) const
{
    // Caller must free the memory of the BroString!
    BroString* bs = new BroString;
    bro_string_set_data(
        bs,
        reinterpret_cast<const unsigned char*>(s.str()), s.size());

    return { BRO_TYPE_STRING, bs };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::vector const& v) const
{
    assert(! "not yet supported by Broccoli");
    return { BRO_TYPE_VECTOR, nullptr };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::set const& s) const
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

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::table const& t) const
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

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::record const& r) const
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

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::address const& a) const
{
    assert(! "not yet implemented");
    return {};
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::prefix const& s) const
{
    assert(! "not yet implemented");

    // Caller must free the memory of the BroSubnet!
    BroSubnet* bs = new BroSubnet;
    //bs->sn_net = 
    //bs->sn_width = 

    return { BRO_TYPE_PORT, bs };
}

broccoli::reverse_factory::bro_val
broccoli::reverse_factory::builder::operator()(ze::port const& p) const
{
    // Caller must free the memory of the BroPort!
    BroPort* bp = new BroPort;

    bp->port_num = p.number();
    switch (p.type())
    {
        default:
            {
                bp->port_proto = 0;
                LOG(debug, broccoli) << "unsupported port type";
            }
            break;
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

void broccoli::reverse_factory::free(bro_val const& v)
{
    switch (v.type)
    {
        case BRO_TYPE_STRING:
             delete static_cast<BroString*>(v.value);
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

BroEvent* broccoli::reverse_factory::make_event(ze::event const& event)
{
    LOG(debug, event) << "building broccoli event " << event.name();

    BroEvent* bro_event = bro_event_new(event.name().c_str());
    if (! bro_event)
    {
        LOG(error, broccoli) << "could not create bro_event " << event.name();
        THROW(error::broccoli());
    }

    for (auto const& arg : event.args())
    {
        LOG(debug, event) << "adding argument: " << arg;
        bro_val val = ze::value::visit(arg, builder());
        bro_event_add_val(bro_event, val.type, NULL, val.value);
        free(val);
    }

    return bro_event;
}


bool broccoli::initialized = false;

void broccoli::init(bool calltrace, bool messages)
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

    initialized = true;
}

void broccoli::callback(BroConn* bc, void* user_data, BroEvMeta* meta)
{
    LOG(debug, broccoli) << "callback for " << meta->ev_name;

    auto event = std::make_shared<ze::event>();
    factory::make_event(*event, meta);
    event_handler* handler = static_cast<event_handler*>(user_data);
    (*handler)(event);
}

void broccoli::async_read()
{
    LOG(debug, broccoli) << *connection_ << ": starting async read";
    connection_->socket().async_read_some(
        boost::asio::null_buffers(),
        strand_->wrap(
            [&](boost::system::error_code const& ec, size_t bytes_transferred)
            {
                handle_read(ec);
            }));
}

void broccoli::handle_read(boost::system::error_code const& ec)
{
    if (! (ec || bro_conn_process_input(bc_)))
        LOG(debug, broccoli) << *connection_ <<  ": no input to process";

    if (! ec || ec == boost::asio::error::would_block)
        async_read();
    else
    {
        if (ec == boost::asio::error::eof)
            LOG(info, broccoli)
                << *connection_ << ": remote broccoli disconnected";
        else
            LOG(error, broccoli) << *connection_ << ": " << ec.message();

        error_handler_(connection_);
    }
}

} // namespace comm
} // namespace vast
