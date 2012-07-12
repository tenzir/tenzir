#ifndef VAST_COMM_BROCCOLI_H
#define VAST_COMM_BROCCOLI_H

#include <memory>
#include <boost/asio/strand.hpp>
#include <ze/event.h>
#include <broccoli.h>

namespace vast {
namespace comm {

// Forward declarations
class connection;

/// A Broccoli session.
class broccoli : public std::enable_shared_from_this<broccoli>
{
    broccoli(broccoli const&) = delete;
    broccoli& operator=(broccoli const&) = delete;

public:
    typedef std::function<void(ze::event event)> event_handler;
    typedef std::function<void(std::shared_ptr<broccoli>)> error_handler;

    /// Initializes Broccoli. This function must be called once before any call
    /// into the Broccoli library to set up the necessary SSL context.
    /// @param messages If @c true, show message contents and protocol details.
    /// @param calltrace If @c true, enable call tracing.
    static void init(bool messages = false, bool calltrace = false);

    /// Constructs a broccoli session over an existing TCP connection.
    /// @param conn The underlying connection.
    /// @param handler The event handler to invoke for each new arriving event.
    broccoli(std::shared_ptr<connection> conn, event_handler const& handler);

    /// Destroys the internal Broccoli handle if it is still valid.
    ~broccoli();

    /// Subscribes to a single event. This function must be called before run.
    /// @param event The name of the event to register.
    void subscribe(std::string const& event);

    /// Sends a 0event.
    /// @param event The event to send.
    void send(ze::event const& event);

    /// Sends a raw Broccoli event.
    /// @param raw The raw event to send.
    void send(std::vector<uint8_t> const& raw);

    /// Starts sending/receiving events from the underlying connection.
    /// @param handler Invoked when Broccoli experiences an error.
    void run(error_handler handler);

    /// Signals the broccoli session to shutdown. Since a broccoli instance is
    /// handed to Boost Asio via @c shared_from_this(), it has potentially
    /// infinite lifetime because Asio will continue to perform asynchronous
    /// read attempts on the underlying connection until an error with the
    /// connection occurs. This is why this function exists: it breaks this
    /// infinite handler re-insertion by explicitly setting a flag that will
    /// cause the next handler invocation to return immediately, without
    /// performing further read operations.
    void stop();

private:
    /// Creates a 0event from a Broccoli event.
    class factory
    {
        factory(factory const&) = delete;
        factory& operator=(factory const&) = delete;

    public:
        /// Constructs a 0event from a Broccoli event.
        /// @param meta The Broccoli meta data from the callback.
        static void make_event(ze::event& event, BroEvMeta* meta);

    private:
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

        static int table_callback(void *key, void *val, table_data const* data);
        static int set_callback(void *key, set_data const* data);
        static ze::value make_value(int type, void* bro_val);
    };

    /// Creates a Broccoli event from a 0event.
    struct reverse_factory
    {
        /// Broccoli's representation of a typed value.
        struct bro_val
        {
            int type;
            void* value;
        };

        /// Converts a 0event value into to a Broccoli value.
        struct builder;

        /// Constructs a 0event from a Broccoli event.
        /// @param meta The Broccoli meta data from the callback.
        static BroEvent* make_event(ze::event const& event);

        /// Frees broccoli data if it was heap-allocated.
        static void free(bro_val const& val);
    };

    /// The Broccoli callback handler. After subscribing to an Bro event, this
    /// handler is called for each event arriving from a Broccoli connection.
    /// The handler converts the Bro event into a 0event and invokes the event
    /// handler specified upon construction of a %broccoli object.
    ///
    /// @param bc The Broccoli handle
    ///
    /// @param user_data A typeless pointer that is used to carry the event
    /// callback from the source.
    ///
    /// @param meta The event meta data of the Broccoli event.
    static void callback(BroConn* bc, void* user_data, BroEvMeta* meta);

    /// Starts an asynchronous read operation on the socket.
    void async_read();

    /// The read handler which is executed when data is available for reading.
    /// @param ec Boost Asio error code.
    /// @param bytes_transferred The number of bytes transferred.
    void handle_read(boost::system::error_code const& ec,
                     size_t bytes_transferred);

    /// Flag indicating whether Broccoli has already been initialized.
    static bool initialized;

    BroConn* bc_;
    std::shared_ptr<connection> conn_;
    boost::asio::strand strand_;
    event_handler event_handler_;
    error_handler error_handler_;
    bool terminate_;
};

} // namespace comm
} // namespace vast

#endif
