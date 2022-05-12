/*  =========================================================================
    cscol - Call Stream Collector submodule
    =========================================================================*/

/*
    This submodule receives the CDR, voice and SDS data from the Damm DAMM
    TetraFlex LogServer through an UDP interface. The supported data streams
    are:

      - Status messages, SDS-Type4 (without TL), SDS-TL TRANSFER
      - Individual calls (Duplex/Simplex)
      - Group calls (Normal/Broadcast)
      - Streams of active voice calls as G.711 A-LAW streams

    Each UDP message received is analyzed and transformed into the corresponding
    LogApi message type.

    The submodule discards junk data between LogApi messages received. Further,
    it supports fragmented LogApi messages received in several UDP packets and
    several LogApi messages received in a UDP packet.

    The submodule publish the LogApi messages through an ZMQ PUB interface to
    interested subscribers. A subscriber can narrow the LogApi message types to
    receive through the introduction of filters at the time of its subscription.

    The allowed filters are:

        S or S_         # All LogApi message types less Voice
        S_<log_api_id>  # LogApi messages of type = <log_api_id>
        V or V_         # Voice
        V_<call_id>     # Voice for the Call with Id = <call_id>

    The submodule will publish the LogApi message types in a 0MQ multipart zmsg
    in the following format:

        <filter>
        <timestamp>
        <LogApi structure with received data>
        <Voice data> (in the case of a Voice message type)
*/


#include "cs.h"
#include "csutil.h"


#define CSCOL_BUFFER_LENGTH 4096
#define CSCOL_WORK_AREA_LENGTH 1024


// Context for a Call Stream Collector thread

struct _cscol_t {
  int generate_wav_files;
  int log_server_endpoint_port;
  int log_server_endpoint_channel;
  char *work_area;
  csstring_t *conf_filename;
  csstring_t *log_server_endpoint_ip;
  zsock_t *publisher;
  TetraFlexLogApiMessageHeader current_header;
  time_t timestamp;
  unsigned char *buffer;
  int buffer_offset;
};
typedef struct _cscol_t cscol_t;


//  --------------------------------------------------------------------------
//  Creates a thread specific Call Stream Collector context
//  Input:
//    conf_file : The configuration file
//  Output:
//    The context created according to the configuration file

static cscol_t*
cscol_new (const char * const conf_file)
{
  cscol_t *self = (cscol_t *) zmalloc (sizeof (cscol_t));
  if (self) {
    self->buffer = (unsigned char *) zmalloc (
        CSCOL_BUFFER_LENGTH * sizeof (unsigned char));
    self->work_area = (char *) zmalloc (
        CSCOL_WORK_AREA_LENGTH * sizeof (char));
    self->buffer_offset = 0;
    self->conf_filename = csstring_new (conf_file);
    self->log_server_endpoint_channel = -1;
    self->log_server_endpoint_ip = NULL;
    self->log_server_endpoint_port = -1;
    self->publisher = NULL;
    self->generate_wav_files = 0;
  }
  return self;
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in the thread specific Call Stream Collector
//  context
//  Input:
//    The target Call Stream Collector context

static void
cscol_destroy (cscol_t **self_p)
{
  assert (self_p);
  if (*self_p) {
    cscol_t *self = *self_p;
    free (self->buffer);
    free (self->work_area);
    csstring_destroy (&self->conf_filename);
    csstring_destroy (&self->log_server_endpoint_ip);
    close (self->log_server_endpoint_channel);
    zsock_destroy (&(self->publisher));
    free (self);
    *self_p = NULL;
  }
}


//  --------------------------------------------------------------------------
//  Opens the publisher endpoint interface
//  Input:
//    The target Call Stream Collector context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cscol_start_publisher (cscol_t *ctx)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in cscol_start_publisher");

  ctx->publisher = zsock_new_pub ("@inproc://collector");
  assert (ctx->publisher);

  if (!ctx->publisher) {
    TRACE (ERROR, "Error: zsock_new_pub(), errno=%d text=%s",
        errno, strerror (errno));
    rc = -1;
  }

  TRACE (FUNCTIONS, "Leaving cscol_start_publisher");

  return rc;
}


//  --------------------------------------------------------------------------
//  Opens the UDP listener to receive the UDP messages from the Damm LogServer
//  Input:
//    A Call Stream Collector context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cscol_start_listener (cscol_t *ctx)
{
  int rc = 0;
  struct sockaddr_in serv_addr;
  unsigned long net_addr;

  TRACE (FUNCTIONS, "Entering in cscol_start_listener");

  if ((ctx->log_server_endpoint_channel = socket (
      AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
    TRACE (ERROR, "Error: socket(), errno=%d text=%s",
        errno, strerror (errno));
    rc = -1;
  }

  if (!rc) {
    memset ((char *) &serv_addr, 0, sizeof (serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons (ctx->log_server_endpoint_port);
    net_addr = inet_network (csstring_data (ctx->log_server_endpoint_ip));
    serv_addr.sin_addr.s_addr = htonl (net_addr);
    if (bind (ctx->log_server_endpoint_channel, (struct sockaddr *) &serv_addr,
        sizeof (serv_addr)) == -1) {
      TRACE (ERROR, "Error: bind(), errno=%d text=%s", errno, strerror (errno));
      rc = -1;
    }
  }

  TRACE (FUNCTIONS, "Leaving cscol_start_listener");

  return rc;
}


//  --------------------------------------------------------------------------
// Traces a Call Stream Collector context
//  Input:
//    A Call Stream Collector context

static void
cscol_print (cscol_t *ctx)
{
  TRACE (FUNCTIONS, "Entering in cscol_print");

  TRACE (DEBUG, "----------------------------------");
  TRACE (DEBUG, "Callstream Collector Configuration");
  TRACE (DEBUG, "----------------------------------");
  TRACE (DEBUG, "  File: %s", csstring_data (ctx->conf_filename));
  TRACE (DEBUG, "  LogServer endpoint: udp://%s:%d",
      csstring_data (ctx->log_server_endpoint_ip), ctx->log_server_endpoint_port);
  TRACE (DEBUG, "  LogServer channel: %d", ctx->log_server_endpoint_channel);
  TRACE (DEBUG, "  Publisher: %s", zsock_type_str (ctx->publisher));
  TRACE (DEBUG, "  Buffer: %p", &ctx->buffer);
  TRACE (DEBUG, "  Offset: %d", ctx->buffer_offset);

  TRACE (FUNCTIONS, "Leaving cscol_print");
}


//  --------------------------------------------------------------------------
//  Reads the properties into a Call Stream Collector context from a
//  configuration file and creates all the needed resources
//  Input:
//    A Call Stream Collector context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cscol_configure (cscol_t *ctx)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in cscol_configure");

  zconfig_t *root = zconfig_load (csstring_data (ctx->conf_filename));
  assert (root);

  char *log_server_endpoint_ip = zconfig_resolve (root,
      "/collector/log_server_endpoint/ip", "127.0.0.1");
  ctx->log_server_endpoint_ip = csstring_new (log_server_endpoint_ip);

  char *log_server_endpoint_port = zconfig_resolve (root,
      "/collector/log_server_endpoint/port", "4321");
  ctx->log_server_endpoint_port = atoi (log_server_endpoint_port);

  char *generate_wav_files = zconfig_resolve (root,
      "/collector/generate_wav_files", "0");
  ctx->generate_wav_files = atoi (generate_wav_files);

  ctx->buffer_offset = 0;

  rc = cscol_start_publisher (ctx);

  if (rc == 0) {
    rc = cscol_start_listener (ctx);
  }

  zconfig_destroy (&root);

  TRACE (FUNCTIONS, "Leaving cscol_configure");

  return rc;
}


//  --------------------------------------------------------------------------
//  Write the data chunk read from the socket to a file for debugging 
//  purposes.
//  Input:
//    buffer: the received data from the socket
//    buffer_len: the size of the data received
//  Output:
//    None
static void cscol_save_chunk(const unsigned char * const buffer, int buffer_len)
{
    char *var = NULL;
    char path_chunk[1024];
    int fd = -1;
    int rc = 0;

    TRACE(FUNCTIONS, "Entering in cscol_save_chunk");

    if ((var = getenv ("CALLSTREAMSERVER_WORK_PATH")) != NULL) {
        sprintf(path_chunk, "%s/csserver_chunk_%lu.trace", var, time(NULL));
    }
    else {
        rc = -1;
    }

    if (rc == 0) {
        if ((fd = open(path_chunk, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1) {
            TRACE(ERROR, "Error: open()");
            rc = -1;
        }
    }

    if (rc == 0) {
        if (write(fd, buffer, buffer_len) != buffer_len) {
            TRACE(ERROR, "Error: write()");
            rc = -1;
        }
    }

    if (fd != -1) {
        close(fd);
        fd = -1;
    }

    TRACE(FUNCTIONS, "Leaving cscol_save_chunk");
}

//  --------------------------------------------------------------------------
//  Callback handler. Analyzes and process commands sent by the parent thread
//  through the shared pipe.
//  Input:
//    loop: the reactor
//    reader: the parent thread endpoint
//    arg: the Call Stream Collector context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cscol_command_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  bool command_handled = false;
  int result = 0;
//  cscol_t *ctx = (cscol_t *) arg;

  TRACE (FUNCTIONS, "Entering in cscol_command_handler");

  zmsg_t *msg = zmsg_recv (reader);
//  zmsg_print (msg);

  if (!msg)
    result = 1;

  char *command = zmsg_popstr (msg);
  TRACE (DEBUG, "Command: %s", command);

  if (streq (command, "$TERM")) {
    command_handled = true;
    result = 1;
  }

  if ((!command_handled) && streq (command, "PING")) {
    command_handled = true;
    char *command = zmsg_popstr (msg);
    zsock_send (reader, "s", command);
    free (command);
  }

  if (!command_handled) {
    TRACE (ERROR, "Invalid message");
    assert (false);
  }

  free (command);
  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving cscol_command_handler");

  return result;
}


//  --------------------------------------------------------------------------
//  Analyzes and process a TetraFlexLogApiMessageHeader from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the received data and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_message_header (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_message_header");

  if (buffer_len >= sizeof (ctx->current_header)) {
    memcpy(&ctx->current_header, buffer, sizeof (ctx->current_header));
    bytes_processed = sizeof (ctx->current_header);
    ctx->timestamp = time (NULL);
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_message_header. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiKeepAlive message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    keep_alive: the message to be published

static void
cscol_dispatch_keep_alive (cscol_t *ctx,
    const LogApiKeepAlive * const keep_alive)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_keep_alive");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_ALIVE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, keep_alive, sizeof (LogApiKeepAlive));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_keep_alive");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiKeepAlive from the LogServer UDP data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_keep_alive (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiKeepAlive keep_alive;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_keep_alive");

  if (buffer_len >= sizeof (keep_alive)) {
    memcpy (&keep_alive, buffer, sizeof (keep_alive));
    bytes_processed = sizeof (keep_alive);
    cscol_dispatch_keep_alive (ctx, &keep_alive);
  }

  TRACE(FUNCTIONS, "Leaving cscol_analyze_keep_alive. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiDuplexCallChange message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    duplex_call_change: the message to be published

static void
cscol_dispatch_duplex_call_change (cscol_t *ctx,
    const LogApiDuplexCallChange * const duplex_call_change)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_duplex_call_change");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_DUPLEX_CALL_CHANGE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, duplex_call_change, sizeof (LogApiDuplexCallChange));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_duplex_call_change");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiDuplexCallChange from the LogServer UDP data
//  stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_duplex_call_change (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiDuplexCallChange duplex_call_change;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in analyze_duplex_call_change");

  if (buffer_len >= sizeof (duplex_call_change)) {
    memcpy (&duplex_call_change, buffer, sizeof (duplex_call_change));
    bytes_processed = sizeof (duplex_call_change);
    cscol_dispatch_duplex_call_change (ctx, &duplex_call_change);
  }

  TRACE (FUNCTIONS, "Leaving analyze_duplex_call_change. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiDuplexCallRelease message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    duplex_call_release: the message to be published

static void
cscol_dispatch_duplex_call_release (cscol_t *ctx,
    const LogApiDuplexCallRelease * const duplex_call_release)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_duplex_call_release");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_DUPLEX_CALL_RELEASE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, duplex_call_release, sizeof (LogApiDuplexCallRelease));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_duplex_call_release");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiDuplexCallRelease from the LogServer UDP data
//  stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_duplex_call_release (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiDuplexCallRelease duplex_call_release;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_duplex_call_release");

  if (buffer_len >= sizeof (duplex_call_release)) {
    memcpy (&duplex_call_release, buffer, sizeof (duplex_call_release));
    bytes_processed = sizeof (duplex_call_release);
    cscol_dispatch_duplex_call_release (ctx, &duplex_call_release);
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_duplex_call_release. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiSimplexCallStartChange message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    simplex_call_start_change: the message to be published

static void
cscol_dispatch_simplex_call_start_change (cscol_t *ctx,
    const LogApiSimplexCallStartChange * const simplex_call_start_change)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_simplex_call_start_change");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_SIMPLEX_CALL_CHANGE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, simplex_call_start_change, sizeof (LogApiSimplexCallStartChange));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_simplex_call_start_change");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiSimplexCallStartChange from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_simplex_call_start_change (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiSimplexCallStartChange simplex_call_start_change;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_simplex_call_start_change");

  if (buffer_len >= sizeof (simplex_call_start_change)) {
    memcpy (&simplex_call_start_change, buffer, sizeof (simplex_call_start_change));
    bytes_processed = sizeof (simplex_call_start_change);
    cscol_dispatch_simplex_call_start_change (ctx, &simplex_call_start_change);
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_simplex_call_start_change. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiSimplexCallPttChange message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    simplex_call_ptt_change: the message to be published

static void
cscol_dispatch_simplex_call_ptt_change (cscol_t *ctx,
    const LogApiSimplexCallPttChange * const simplex_call_ptt_change)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_simplex_call_ptt_change");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_SIMPLEX_CALL_PTT_CHANGE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, simplex_call_ptt_change, sizeof (LogApiSimplexCallPttChange));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_simplex_call_ptt_change");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiSimplexCallPttChange from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_simplex_call_ptt_change (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiSimplexCallPttChange simplex_call_ptt_change;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_simplex_call_ptt_change");

  if (buffer_len >= sizeof (simplex_call_ptt_change)) {
    memcpy (&simplex_call_ptt_change, buffer, sizeof (simplex_call_ptt_change));
    bytes_processed = sizeof (simplex_call_ptt_change);
    cscol_dispatch_simplex_call_ptt_change(ctx, &simplex_call_ptt_change);
  }

  TRACE(FUNCTIONS, "Leaving cscol_analyze_simplex_call_ptt_change. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiSimplexCallRelease message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    simplex_call_release: the message to be published

static void
cscol_dispatch_simplex_call_release (cscol_t *ctx,
    const LogApiSimplexCallRelease * const simplex_call_release)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_simplex_call_release");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_SIMPLEX_CALL_RELEASE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, simplex_call_release, sizeof (LogApiSimplexCallRelease));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_simplex_call_release");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiSimplexCallRelease from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_simplex_call_release (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiSimplexCallRelease simplex_call_release;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_simplex_call_release");

  if (buffer_len >= sizeof (simplex_call_release)) {
    memcpy (&simplex_call_release, buffer, sizeof (simplex_call_release));
    bytes_processed = sizeof (simplex_call_release);
    cscol_dispatch_simplex_call_release (ctx, &simplex_call_release);
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_simplex_call_release. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiGroupCallStartChange message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    group_call_start_change: the message to be published

static void
cscol_dispatch_group_call_start_change (cscol_t *ctx,
    const LogApiGroupCallStartChange * const group_call_start_change)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_group_call_start_change");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_GROUP_CALL_CHANGE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, group_call_start_change, sizeof (LogApiGroupCallStartChange));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_group_call_start_change");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiGroupCallStartChange from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_group_call_start_change (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiGroupCallStartChange group_call_start_change;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_group_call_start_change");

  TRACE (DEBUG, "Buffer len: %d - Size struct: %d", buffer_len, sizeof (group_call_start_change));

  if (buffer_len >= sizeof (group_call_start_change)) {
    memcpy (&group_call_start_change, buffer, sizeof (group_call_start_change));
    bytes_processed = sizeof (group_call_start_change);
    cscol_dispatch_group_call_start_change (ctx, &group_call_start_change);
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_group_call_start_change. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiGroupCallPttActive message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    group_call_ptt_active: the message to be published

static void
cscol_dispatch_group_call_ptt_active (cscol_t *ctx,
    const LogApiGroupCallPttActive * const group_call_ptt_active)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_group_call_ptt_active");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_GROUP_CALL_PTT_ACTIVE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, group_call_ptt_active, sizeof (LogApiGroupCallPttActive));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_group_call_ptt_active");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiGroupCallPttActive from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_group_call_ptt_active (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiGroupCallPttActive group_call_ptt_active;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_group_call_ptt_active");

  if (buffer_len >= sizeof (group_call_ptt_active)) {
    memcpy (&group_call_ptt_active, buffer, sizeof (group_call_ptt_active));
    bytes_processed = sizeof (group_call_ptt_active);
    cscol_dispatch_group_call_ptt_active (ctx, &group_call_ptt_active);
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_group_call_ptt_active. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiGroupCallPttIdle message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    group_call_ptt_idle: the message to be published

static void
cscol_dispatch_group_call_ptt_idle (cscol_t *ctx,
    const LogApiGroupCallPttIdle * const group_call_ptt_idle)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_group_call_ptt_idle");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_GROUP_CALL_PTT_IDLE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, group_call_ptt_idle, sizeof (LogApiGroupCallPttIdle));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_group_call_ptt_idle");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiGroupCallPttIdle from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_group_call_ptt_idle (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiGroupCallPttIdle group_call_ptt_idle;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_group_call_ptt_idle");

  if (buffer_len >= sizeof (group_call_ptt_idle)) {
    memcpy (&group_call_ptt_idle, buffer, sizeof (group_call_ptt_idle));
    bytes_processed = sizeof (group_call_ptt_idle);
    cscol_dispatch_group_call_ptt_idle (ctx, &group_call_ptt_idle);
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_group_call_ptt_idle. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiGroupCallRelease message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    group_call_release: the message to be published

static void
cscol_dispatch_group_call_release (cscol_t *ctx,
    const LogApiGroupCallRelease * const group_call_release)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_group_call_release");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_GROUP_CALL_RELEASE);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, group_call_release, sizeof (LogApiGroupCallRelease));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_group_call_release");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiGroupCallRelease from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_group_call_release(cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiGroupCallRelease group_call_release;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in analyze_group_call_release");

  if (buffer_len >= sizeof (group_call_release)) {
    memcpy (&group_call_release, buffer, sizeof (group_call_release));
    bytes_processed = sizeof (group_call_release);
    cscol_dispatch_group_call_release (ctx, &group_call_release);
  }

  TRACE (FUNCTIONS, "Leaving analyze_group_call_release. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiStatusSDS message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    status_sds: the message to be published

static void
cscol_dispatch_status_sds (cscol_t *ctx,
    const LogApiStatusSDS * const status_sds)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_status_sds");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_SDS_STATUS);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, status_sds, sizeof (LogApiStatusSDS));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_status_sds");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiStatusSDS from the LogServer UDP data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_status_sds (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiStatusSDS status_sds;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in analyze_status_sds");

  if (buffer_len >= sizeof (status_sds)) {
    memcpy (&status_sds, buffer, sizeof (status_sds));
    bytes_processed = sizeof (status_sds);
    cscol_dispatch_status_sds (ctx, &status_sds);
  }

  TRACE (FUNCTIONS, "Leaving analyze_status_SDS. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiTextSDS message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    text_sds: the message to be published

static void
cscol_dispatch_text_sds (cscol_t *ctx,
    const LogApiTextSDS * const text_sds)
{
  TRACE (FUNCTIONS, "Entering in cscol_dispatch_text_sds");

  snprintf (ctx->work_area, sizeof (ctx->work_area),
      "S_%d", LOG_API_SDS_TEXT);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &ctx->timestamp, sizeof (time_t));
  zmsg_addmem (msg, text_sds, sizeof (LogApiTextSDS));
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_text_sds");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiTextSDS from the LogServer UDP data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_text_sds (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiTextSDS text_sds;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in analyze_text_sds");

  if (buffer_len >= sizeof (text_sds)) {
    memcpy (&text_sds, buffer, sizeof (text_sds));
    bytes_processed = sizeof (text_sds);
    cscol_dispatch_text_sds (ctx, &text_sds);
  }

  TRACE (FUNCTIONS, "Leaving analyze_text_sds. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Publish a LogApiVoice message type to the registered subscribers
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    voice: the message to be published
//    voice_data: the voice data in a-law format

static void
cscol_dispatch_voice (cscol_t *ctx,
    const LogApiVoice * const voice, const unsigned char * const voice_data)
{
  time_t now = time (NULL);

  TRACE (FUNCTIONS, "Entering in cscol_dispatch_voice");

  snprintf (ctx->work_area, CSCOL_WORK_AREA_LENGTH * sizeof (char), //sizeof (ctx->work_area),
      "V_%u", voice->m_uiCallId);
  TRACE (DEBUG, "Call id: %u", voice->m_uiCallId);
  zmsg_t * msg = zmsg_new ();
  zmsg_pushstr (msg, ctx->work_area);
  zmsg_addmem (msg, &now, sizeof (time_t));
  zmsg_addmem (msg, voice, sizeof (LogApiVoice));
  zmsg_addmem (msg, voice_data, 480);
  zmsg_send (&msg, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cscol_dispatch_voice");
}


//  --------------------------------------------------------------------------
//  Analyzes and process a LogApiVoice with A-law payload from the LogServer UDP
//  data stream.
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_voice (cscol_t *ctx,
    const unsigned char * const buffer, int buffer_len)
{
  LogApiVoice voice;
  int bytes_processed = 0;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_voice");

  if (buffer_len >= sizeof (voice) + 480) {
    memcpy (&voice, buffer, sizeof (voice));
    bytes_processed = sizeof (voice);
    TRACE (DEBUG, "sizeof(voice): %d", sizeof (voice));
    TRACE (DEBUG, "payload: %d", voice.m_uiPayload1Info); 
    if (voice.m_uiPayload1Info == 7) {
      if (ctx->generate_wav_files) {
        char path[256];
        sprintf (path, "voice_%d.wav", voice.m_uiCallId);
        cs_write_wav_file (path, buffer);
      }
      cscol_dispatch_voice (ctx, &voice, buffer + sizeof (voice));
    }
    bytes_processed += 480;
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_voice. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Analyzes and process the LogServer UDP data stream. Onces a LogApi message
//  is detected, invokes the corresponding LogApi handler for further processing
//  Input:
//    ctx: the Call Stream Collector context of the thread
//    buffer: the data received and not yet processed
//    buffer_len: the size of the data received and not yet processed
//  Output:
//    Bytes processed from the received data

static int
cscol_analyze_streaming (cscol_t *ctx,
    const unsigned char *buffer, int buffer_len)
{
  int bytes_processed = 0;
  int log_api_bytes_processed = 0;
  int bytes_remained = buffer_len;
  unsigned int signature;
  const unsigned char *buffer_orig;

  TRACE (FUNCTIONS, "Entering in cscol_analyze_streaming");
  TRACE (DEBUG, "buffer_len: %d", buffer_len);

  buffer_orig = buffer;

  while (bytes_remained > 4) {

    TRACE (DEBUG, "buffer: %x", buffer);
    memcpy (&signature, buffer, 4);

    if (signature == LOG_API_PROTOCOL_SIGNATURE) {

      TRACE (DEBUG, "Protocol signature found");
      cscol_analyze_message_header (ctx, buffer, bytes_remained);

      switch (ctx->current_header.MsgId) {
      case LOG_API_ALIVE:
        TRACE (DEBUG, "Message type: LOG_API_KEEP_ALIVE");
        log_api_bytes_processed = cscol_analyze_keep_alive (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_DUPLEX_CALL_CHANGE:
        TRACE (DEBUG, "Message type: LOG_API_DUPLEX_CALL_CHANGE");
        log_api_bytes_processed = cscol_analyze_duplex_call_change (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_DUPLEX_CALL_RELEASE:
        TRACE (DEBUG, "Message type: LOG_API_DUPLEX_CALL_RELEASE");
        log_api_bytes_processed = cscol_analyze_duplex_call_release (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_SIMPLEX_CALL_CHANGE:
        TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_START_CHANGE");
        log_api_bytes_processed = cscol_analyze_simplex_call_start_change (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_SIMPLEX_CALL_PTT_CHANGE:
        TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_PTT_CHANGE");
        log_api_bytes_processed = cscol_analyze_simplex_call_ptt_change (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_SIMPLEX_CALL_RELEASE:
        TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_RELEASE");
        log_api_bytes_processed = cscol_analyze_simplex_call_release (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_GROUP_CALL_CHANGE:
        TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_START_CHANGE");
        log_api_bytes_processed = cscol_analyze_group_call_start_change (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_GROUP_CALL_PTT_ACTIVE:
        TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_PPT_ACTIVE");
        log_api_bytes_processed = cscol_analyze_group_call_ptt_active (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_GROUP_CALL_PTT_IDLE:
        TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_PTT_IDLE");
        log_api_bytes_processed = cscol_analyze_group_call_ptt_idle (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_GROUP_CALL_RELEASE:
        TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_RELEASE");
        log_api_bytes_processed = cscol_analyze_group_call_release (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_SDS_STATUS:
        TRACE (DEBUG, "Message type: LOG_API_SDS_STATUS");
        log_api_bytes_processed = cscol_analyze_status_sds (ctx,
            buffer, bytes_remained);
        break;
      case LOG_API_SDS_TEXT:
        TRACE (DEBUG, "Message type: LOG_API_SDS_TEXT");
        log_api_bytes_processed = cscol_analyze_text_sds (ctx,
            buffer, bytes_remained);
        break;
      default:
        TRACE (DEBUG, "Message type: UNKNOWN (%x)", ctx->current_header.MsgId);
        log_api_bytes_processed = 1;
        break;
      }
    } else if (signature == VOICE_PROTOCOL_SIGNATURE) {
      TRACE (DEBUG, "Voice protocol signature found");
      log_api_bytes_processed = cscol_analyze_voice (ctx, buffer, bytes_remained);
    } else {
      log_api_bytes_processed = 1;
    }

    bytes_processed += log_api_bytes_processed;
    buffer = buffer_orig + bytes_processed;
    bytes_remained = buffer_len - bytes_processed;

    TRACE (FUNCTIONS, "bytes_processed: %d", bytes_processed);
    TRACE (FUNCTIONS, "bytes_remained: %d", bytes_remained);

    if (!log_api_bytes_processed) break; // We need to receive more data
  }

  TRACE (FUNCTIONS, "Leaving cscol_analyze_streaming. "
      "bytes_processed: %d", bytes_processed);

  return bytes_processed;
}


//  --------------------------------------------------------------------------
//  Callback responsible for receiving the LogServer UDP data stream.
//  Input:
//    loop: the event-driven reactor
//    item: the descriptor with the data received ready to read
//    arg: the Call Stream Collector context of the thread
//  Output:
//    Bytes processed from the received data

static int
cscol_callstream_handler (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
  int rc = 0;
  struct sockaddr_in cli_addr;
  socklen_t len;
  int nr_bytes;
  int bytes_processed = 0;

  cscol_t *ctx = (cscol_t *) arg;

  TRACE (FUNCTIONS, "Entering in cscol_callstream_handler");

  memset(&cli_addr, 0, sizeof (struct sockaddr_in));
  nr_bytes = recvfrom (item->fd, ctx->buffer + ctx->buffer_offset,
        CSCOL_BUFFER_LENGTH * sizeof (unsigned char), 0,
        (struct sockaddr *) &cli_addr, &len);

  if (nr_bytes == -1) {
    TRACE (ERROR, "Error: read(), errno=%d text=%s", errno, strerror(errno));
    rc = -1;
  }

  if (nr_bytes == 0) {
    TRACE (WARNING, "Warning: nr_bytes: 0");
  }

  if (nr_bytes > 0) {
    TRACE (DEBUG, "Data received. nr_bytes = %d", nr_bytes);
    TRACE (DEBUG, "Data in buffer. buffer_offset = %d", ctx->buffer_offset);

    // Save the data chunks for debugging
    if (tr_level & L_TR_CS) {
        cscol_save_chunk(ctx->buffer + ctx->buffer_offset, nr_bytes);
    }

    nr_bytes += ctx->buffer_offset; // Data length not yet processed

    bytes_processed = cscol_analyze_streaming (ctx, ctx->buffer, nr_bytes);

    if (nr_bytes - bytes_processed) {
      memcpy (ctx->buffer, ctx->buffer + bytes_processed, nr_bytes - bytes_processed);
      ctx->buffer_offset = nr_bytes - bytes_processed;
    } else {
      ctx->buffer_offset = 0;
    }
  }

  TRACE (FUNCTIONS, "Leaving cscol_callstream_handler");

  return 0;
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

//static int
//cscol_timer_handler (zloop_t *loop, int timer_id, void *arg)
//{
//  cscol_t *ctx = (cscol_t *) arg;
//
//  TRACE (FUNCTIONS, "Entering in cscol_timer_handler");
//  TRACE (FUNCTIONS, "Leaving cscol_timer_handler");
//
//  return 0;
//}


//  --------------------------------------------------------------------------
//  Entry function to the Call Stream Collector submodule
//  Input:
//    pipe: the shared communication channel with the parent thread
//    arg: the configuration file with the submodule's customizable properties

void
cscol_task (zsock_t* pipe, void *args)
{
  int rc = -1;
//  int id_timer;
  cscol_t *ctx;
  char *conf_file;

  TRACE (FUNCTIONS, "Entering in cscol_task");

  conf_file = (char *) args;

  ctx = cscol_new (conf_file);
  assert (ctx);

  rc = cscol_configure (ctx);
  cscol_print (ctx);

  zmq_pollitem_t item = { 0, ctx->log_server_endpoint_channel, ZMQ_POLLIN, 0 };

  zloop_t *loop = zloop_new ();
  assert (loop);
//  zloop_set_verbose (loop, true);

  rc = zloop_reader (loop, pipe, cscol_command_handler, ctx);

  if (rc == 0) {
    rc = zloop_poller (loop, &item, cscol_callstream_handler, ctx);
  }
//  id_timer = zloop_timer (loop, 5000, 0, cscol_timer_handler, ctx);

  if (rc == 0) {
    rc = zsock_signal (pipe, 0);
  }

  if (rc == 0) {
    rc = zloop_start (loop);
    if (rc == 0) {
      TRACE (ERROR, "Interrupted!");
    }
    if (rc == -1) {
      TRACE (ERROR, "Cancelled!");
    }
  }

//  zloop_timer_end (loop, id_timer);
  zloop_poller_end (loop, &item);
  zloop_reader_end (loop, pipe);
  zloop_destroy (&loop);
  cscol_destroy (&ctx);

  TRACE (FUNCTIONS, "Leaving cscol_task");
}

