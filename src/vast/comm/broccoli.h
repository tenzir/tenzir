#ifndef VAST_COMM_BROCCOLI_H
#define VAST_COMM_BROCCOLI_H

#include <memory>
#include <boost/asio/strand.hpp>
#include <broccoli.h>
#include "vast/comm/forward.h"
#include "vast/core/bytes.h"
#include "vast/core/forward.h"

namespace vast {
namespace comm {

/// A Broccoli session.
class broccoli
{
public:
    /// Constructs a broccoli session over an existing TCP connection.
    /// @param conn The underlying connection.
    /// @param handler The event handler to invoke for each new arriving event.
    broccoli(connection_ptr const& conn, event_handler const& handler);

    /// Moves another broccoli instance.
    /// @param other The broccoli instance to move.
    /// @note It is okay to let the strand of the other broccoli go out of
    ///     scope, because handlers "posted through the strand that have not
    ///     yet been invoked will still be dispatched in a way that meets the
    ///     guarantee of non-concurrency."
    broccoli(broccoli&& other);

    broccoli(broccoli const&) = delete;

    /// Assigns a broccoli to this instance.
    /// @param other The right-hand side of the assignment.
    broccoli& operator=(broccoli other);

    /// Destroys the internal Broccoli handle if it is still valid.
    ~broccoli();

    /// Swaps two Broccoli objects.
    /// @param other The other broccoli instance.
    void swap(broccoli& other);

    /// Subscribes to a single event. This function must be called before run.
    /// @param event The name of the event to register.
    void subscribe(std::string const& event);

    /// Sends a 0event.
    /// @param event The event to send.
    void send(ze::event const& event);

    /// Sends a raw Broccoli event.
    /// @param raw The raw event to send.
    void send(core::bytes const& raw);

    /// Starts sending/receiving events from the underlying connection.
    /// @param error_handler Invoked when Broccoli experiences an error.
    void run(conn_handler const& error_handler);

    /// Retrieves the underlying connection.
    /// @return The connection object;
    connection_ptr connection() const;

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

    /// Initializes Broccoli. This function must be called once before any call
    /// into the Broccoli library to set up the necessary SSL context.
    /// @param calltrace If @c true, enable call tracing.
    /// @param messages If @c true, show message contents and protocol details.
    static void init(bool calltrace = false, bool messages = false);

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
    /// \param ec Boost.Asio error code.
    void handle_read(boost::system::error_code const& ec);

    /// Flag indicating whether Broccoli has already been initialized.
    static bool initialized;

    connection_ptr connection_;
    event_handler event_handler_;
    conn_handler error_handler_;
    std::unique_ptr<boost::asio::strand> strand_;
    BroConn* bc_;
};

} // namespace comm
} // namespace vast

#endif
