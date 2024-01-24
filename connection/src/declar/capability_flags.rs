pub(crate) const CLIENT_LONG_PASSWORD: u64 = 0x0000_0001;

/// Send found rows instead of affected rows in EOF_Packet.
pub(crate) const CLIENT_FOUND_ROWS: u64 = 0x0000_0002;

/// Get all column flags.
/// Longer flags in Protocol::ColumnDefinition320.
///
/// ### Server
/// Supports longer flags.
///
/// ### Client
/// Expects longer flags.
pub(crate) const CLIENT_LONG_FLAG: u64 = 0x0000_0004;

/// Database (schema) name can be specified on connect in Handshake Response Packet.
/// ### Server
/// Supports schema-name in Handshake Response Packet.
///
/// ### Client
/// Handshake Response Packet contains a schema-name.
pub(crate) const CLIENT_CONNECT_WITH_DB: u64 = 0x0000_0008;

/// Don't allow database.table.column.
pub(crate) const CLIENT_NO_SCHEMA: u64 = 0x0000_0010;

/// Compression protocol supported.
///
/// ### Server
/// Supports compression.
///
/// ### Client
/// Switches to Compression compressed protocol after successful authentication.
pub(crate) const CLIENT_COMPRESS: u64 = 0x0000_0020;

/// Special handling of ODBC behavior.
pub(crate) const CLIENT_ODBC: u64 = 0x0000_0040;

/// Can use LOAD DATA LOCAL.
///
/// ### Server
/// Enables the LOCAL INFILE request of LOAD DATA|XML.
///
/// ### Client
/// Will handle LOCAL INFILE request.
pub(crate) const CLIENT_LOCAL_FILES: u64 = 0x0000_0080;

/// Ignore spaces before '('.
///
/// ### Server
/// Parser can ignore spaces before '('.
///
/// ### Client
/// Let the parser ignore spaces before '('.
pub(crate) const CLIENT_IGNORE_SPACE: u64 = 0x0000_0100;

pub(crate) const CLIENT_PROTOCOL_41: u64 = 0x0000_0200;

/// This is an interactive client.
/// Use System_variables::net_wait_timeout versus System_variables::net_interactive_timeout.
///
/// ### Server
/// Supports interactive and noninteractive clients.
///
/// ### Client
/// Client is interactive.
pub(crate) const CLIENT_INTERACTIVE: u64 = 0x0000_0400;

/// Use SSL encryption for the session.
///
/// ### Server
/// Supports SSL
///
/// ### Client
/// Switch to SSL after sending the capability-flags.
pub(crate) const CLIENT_SSL: u64 = 0x0000_0800;

/// Client only flag. Not used.
///
/// ### Client
/// Do not issue SIGPIPE if network failures occur (libmysqlclient only).
pub(crate) const CLIENT_IGNORE_SIGPIPE: u64 = 0x0000_1000;

/// Client knows about transactions.
///
/// ### Server
/// Can send status flags in OK_Packet / EOF_Packet.
///
/// ### Client
/// Expects status flags in OK_Packet / EOF_Packet.
///
/// ### Note
/// This flag is optional in 3.23, but always set by the server since 4.0.
pub(crate) const CLIENT_TRANSACTIONS: u64 = 0x0000_2000;

pub(crate) const CLIENT_RESERVED: u64 = 0x0000_4000;

pub(crate) const CLIENT_SECURE_CONNECTION: u64 = 0x0000_8000;

/// Enable/disable multi-stmt support.
/// Also sets CLIENT_MULTI_RESULTS. Currently not checked anywhere.
///
/// ### Server
/// Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
///
/// ### Client
/// May send multiple statements per COM_QUERY and COM_STMT_PREPARE.
const CLIENT_MULTI_STATEMENTS: u64 = 0x0001_0000;

/// Enable/disable multi-results.
///
/// ### Server
/// Can send multiple resultsets for COM_QUERY. Error if the server needs to send
/// them and client does not support them.
///
/// ### Client
/// Can handle multiple resultsets for COM_QUERY.
///
/// ### Requires
/// `CLIENT_PROTOCOL_41`
const CLIENT_MULTI_RESULTS: u64 = 0x0002_0000;

/// Multi-results and OUT parameters in PS-protocol.
///
/// ### Server
/// Can send multiple resultsets for COM_STMT_EXECUTE.
///
/// ### Client
/// Can handle multiple resultsets for COM_STMT_EXECUTE.
///
/// ### Requires
/// `CLIENT_PROTOCOL_41`
const CLIENT_PS_MULTI_RESULTS: u64 = 0x0004_0000;

/// Client supports plugin authentication.
///
/// ### Server
/// Sends extra data in Initial Handshake Packet and supports the pluggable
/// authentication protocol.
///
/// ### Client
/// Supports authentication plugins.
///
/// ### Requires
/// `CLIENT_PROTOCOL_41`
pub(crate) const CLIENT_PLUGIN_AUTH: u64 = 0x0008_0000;


/// Client supports connection attributes.
///
/// ### Server
/// Permits connection attributes in Protocol::HandshakeResponse41.
///
/// ### Client
/// Sends connection attributes in Protocol::HandshakeResponse41.
pub(crate) const CLIENT_CONNECT_ATTRS: u64 = 0x0010_0000;

/// Enable authentication response packet to be larger than 255 bytes.
/// When the ability to change default plugin require that the initial password
/// field in the Protocol::HandshakeResponse41 paclet can be of arbitrary size.
/// However, the 4.1 client-server protocol limits the length of the auth-data-field
/// sent from client to server to 255 bytes. The solution is to change the type of
/// the field to a true length encoded string and indicate the protocol change with
/// this client capability flag.
///
/// ### Server
/// Understands length-encoded integer for auth response data in
/// Protocol::HandshakeResponse41.
///
/// ### Client
/// Length of auth response data in Protocol::HandshakeResponse41 is a
/// length-encoded integer.
///
/// ### Note
/// The flag was introduced in 5.6.6, but had the wrong value.
pub(crate) const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u64 = 0x0020_0000;

/// Don't close the connection for a user account with expired password.
///
/// ### Server
/// Announces support for expired password extension.
///
/// ### Client
/// Can handle expired passwords.
pub(crate) const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u64 = 0x0040_0000;

/// Capable of handling server state change information.
/// Its a hint to the server to include the state change information in OK_Packet.
///
/// ### Server
/// Can set SERVER_SESSION_STATE_CHANGED in the SERVER_STATUS_flags_enum and send
/// Session State Information in a OK_Packet.
///
/// ### Client
/// Expects the server to send Session State Information in a OK_Packet.
pub(crate) const CLIENT_SESSION_TRACK: u64 = 0x0080_0000;

/// Client no longer needs EOF_Packet and will use OK_Packet instead.
///
/// ### Server
/// Can send OK after a Text Resultset.
///
/// ### Client
/// Expects an OK_Packet (instead of EOF_Packet) after the resultset
/// rows of a Text Resultset.
///
/// ### Background
/// To support CLIENT_SESSION_TRACK, additional information must be sent after all
/// successful commands. Although the OK_Packet is extensible, the EOF_Packet is
/// not due to the overlap of its bytes with the content of the Text Resultset Row.
///
/// Therefore, the EOF_Packet in the Text Resultset is replaced with an OK_Packet.
/// EOF_Packet is deprecated as of MySQL 5.7.5.
pub(crate) const CLIENT_DEPRECATE_EOF: u64 = 0x0100_0000;

/// The client can handle optional metadata information in the resultset.
pub(crate) const CLIENT_OPTIONAL_RESULTSET_METADATA: u64 = 0x0200_0000;

/// Compression protocol extended to support zstd compression method.
///
/// This capability flag is used to send zstd compression level between client and server
/// provided both client and server are enabled with this flag.
///
/// # Server
///
/// Server sets this flag when global variable protocol-compression-algorithms has zstd
/// in its list of supported values.
///
/// # Client
///
/// Client sets this flag when it is configured to use zstd compression method.
pub(crate) const CLIENT_ZSTD_COMPRESSION_ALGORITHM: u64 = 0x0400_0000;

/// Support optional extension for query parameters into the COM_QUERY
/// and COM_STMT_EXECUTE packets.
///
/// # Server
///
/// Expects an optional part containing the query parameter set(s).
/// Executes the query for each set of parameters or returns an error if more than 1 set
/// of parameters is sent and the server can't execute it.
///
/// # Client
///
/// Can send the optional part containing the query parameter set(s).
pub(crate) const CLIENT_QUERY_ATTRIBUTES: u64 = 0x0800_0000;

/// Support Multi factor authentication.
///
/// # Server
///
/// Server sends AuthNextFactor packet after every nth factor
/// authentication method succeeds, except the last factor authentication.
///
/// # Client
///
/// Client reads AuthNextFactor packet sent by server
/// and initiates next factor authentication method.
pub(crate) const MULTI_FACTOR_AUTHENTICATION: u64 = 0x1000_0000;

/// Client or server supports progress reports within error packet.
pub(crate) const CLIENT_PROGRESS_OBSOLETE: u64 = 0x2000_0000;

/// Verify server certificate. Client only flag.
///
/// Deprecated in favor of –ssl-mode.
pub(crate) const CLIENT_SSL_VERIFY_SERVER_CERT: u64 = 0x4000_0000;

/// Don't reset the options after an unsuccessful connect. Client only flag.
pub(crate) const CLIENT_REMEMBER_OPTIONS: u64 = 0x8000_0000;