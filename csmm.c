/*  =========================================================================
    csmm - Call Stream Media Manager submodule

    TODO - Handle potentially orphan calls (lost of call release events)
    TODO - Handle calls without a call setup (lost of call setup events)
    TODO - Queue requested call interceptions when all the feeders are busy
    =========================================================================

    This submodule is responsible for receiving the requested active voice calls'
    streams from the cscol submodule and send them to the Media Server through
    preconfigured feeders.

    To carry out this task, the submodule becomes a subscriber of the cscol
    publisher to receive all the call setup and call release LogApi messages.

    In addition, the submodule offers an API to interested clients through a
    0MQ REQ-REP pattern interface with the following capabilities:

      - Find out the current ongoing calls.
      - Start an ongoing call interception.
      - Stop an ongoing call interception.
      - Start to play a recorded call.
      - Stop an ongoing recorded call player.

    General logic for live calls handling
    -------------------------------------
    Once the start of an ongoing call interception has been requested, the
    submodule will subscribe to all the related call's voice LogApi messages with
    the publisher of the cscol submodule and will select a free preconfigured
    feeder. From this stage, the submodule will reroute all the voice data to
    the selected feeder's Media Server until the call is released or the client
    requests to stop the ongoing call interception.

    General logic for recorded calls handling
    -----------------------------------------
    Once the start of play a recorded call has been requested, the submodule will:
      - Extract the stored data voice from the DB.
      - Write the data voice into a wav file format.
      - Returns to the requester the stream's url from which the call can be heard.
      - Starts the call player.
    The client will be able to stop the playing of a call at any time.
*/


#include "cs.h"
#include "csutil.h"
#include "wave.h"
#include "md5.h"
#include <libpq-fe.h>



#define LIVE_FEEDER_TAG 0x0001cafe
#define LIVE_CALL_TAG   0x0000deaf
#define CALL_PLAYER_TAG 0x0000feda

#define CSMM_TMP_BUFFER 64
#define CSMM_BUFFER_WORK_AREA_LENGTH 2048


#ifdef __cplusplus
extern "C" {
#endif
void csply_task (zsock_t *pipe, void *args);
#ifdef __cplusplus
}
#endif


// The properties of a Media Server Call Player

struct _call_player_t {
  UINT32 tag;
  UINT32 call_id;
  UINT32 call_dbId;
  csstring_t *file_name;
  csstring_t *stream_name;
  csstring_t *feeder_name;
  zactor_t *executor;
  bool free;
};
typedef struct _call_player_t call_player_t;


// The properties of a Media Server Feeder

struct _live_feeder_t {
  UINT32 tag;
  csstring_t *stream_name;
  bool free;
  char feeder_type;
  csstring_t *ip;
  int port;
  int channel;
  struct sockaddr_in serv_addr;
};
typedef struct _live_feeder_t live_feeder_t;


// The properties of an active call

struct _live_call_t {
  UINT32 tag;
  UINT32 id;
  char call_type;
  zchunk_t *voice_data_stream_a;
  zchunk_t *voice_data_stream_b;
  zsock_t *subscriber;
  live_feeder_t *live_feeder;
  time_t last_activity;
};
typedef struct _live_call_t live_call_t;


//  Context for a Call Stream Media Manager thread

struct _csmm_t {
  char *work_area;
  PGconn *pg_conn;
  csstring_t *pg_conn_info;
  csstring_t *conf_filename;
  csstring_t *media_server_endpoint;
  csstring_t *player_command_template;
  csstring_t *filename_template;
  csstring_t *voicerec_url;
  csstring_t *voicerec_repo;
  zlist_t *live_feeders;
  zlist_t *live_calls;
  zlist_t *call_players;
  zsock_t *subscriber;
  zsock_t *command_listener;
  zloop_t *loop;
  unsigned int call_inactivity_period;
  unsigned int maintenance_frequency;
};
typedef struct _csmm_t csmm_t;


//  --------------------------------------------------------------------------
//  Establish a connection with the TeNMS database
//  Input:
//    The target Media Manager context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
csmm_connect_db (csmm_t *ctx)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_connect_db");

  ctx->pg_conn = PQconnectdb (csstring_data (ctx->pg_conn_info));

  if (PQstatus (ctx->pg_conn) != CONNECTION_OK) {
    TRACE(ERROR, "Error: connection to database '%s' failed.", PQdb (ctx->pg_conn));
    TRACE(ERROR, "%s\n", PQerrorMessage (ctx->pg_conn));
    rc = -1;
  }

  TRACE (FUNCTIONS, "Leaving csmm_connect_db");

  return rc;
}


//  --------------------------------------------------------------------------
//  Releases a connection with the TeNMS database
//  Input:
//    The target Call Stream Media Manager context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
csmm_disconnect_db (csmm_t *ctx)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_disconnect_db");

  if (ctx->pg_conn != NULL) {
    PQfinish (ctx->pg_conn);
    ctx->pg_conn = NULL;
  }

  TRACE (FUNCTIONS, "Leaving csmm_disconnect_db");

  return rc;
}


//  --------------------------------------------------------------------------
//  Creates a thread specific Call Stream Media Manager context
//  Input:
//    conf_file: The configuration file
//    loop: the event-driven reactor
//  Output:
//    The context created according to the configuration file

static csmm_t*
csmm_new (const char * const conf_file, zloop_t *loop)
{
  csmm_t *self;

  TRACE (FUNCTIONS, "Entering in csmm_new");

  self = (csmm_t *) zmalloc (sizeof (csmm_t));

  if (self) {
    self->work_area = (char *) zmalloc (
        CSMM_BUFFER_WORK_AREA_LENGTH * sizeof (char));
    self->pg_conn = NULL;
    self->pg_conn_info = NULL;
    self->conf_filename = csstring_new (conf_file);
    self->media_server_endpoint = NULL;
    self->player_command_template = NULL;
    self->filename_template = NULL;
    self->voicerec_url = NULL;
    self->voicerec_repo = NULL;
    self->live_feeders = NULL;
    self->live_calls = NULL;
    self->call_players = NULL;
    self->subscriber = NULL;
    self->command_listener = NULL;
    self->loop = loop;
  }

  TRACE (FUNCTIONS, "Leaving csmm_new");

  return self;
}


//  --------------------------------------------------------------------------
//  Cleans all the resources created in the thread specific Call Stream Media
//  Manager context
//  Input:
//    The target Call Stream Media Manager context

static void
csmm_destroy (csmm_t **self_p)
{
  TRACE (FUNCTIONS, "Entering in csmm_destroy");

  assert (self_p);
  if (*self_p) {
    csmm_t *self = *self_p;
    free (self->work_area);
    csmm_disconnect_db (self);
    csstring_destroy (&self->pg_conn_info);
    csstring_destroy (&self->conf_filename);
    csstring_destroy (&self->media_server_endpoint);
    csstring_destroy (&self->player_command_template);
    csstring_destroy (&self->filename_template);
    csstring_destroy (&self->voicerec_url);
    csstring_destroy (&self->voicerec_repo);
    zlist_destroy (&self->live_feeders);
    zlist_destroy (&self->live_calls);
    zlist_destroy (&self->call_players);
    if (self->subscriber) {
      zloop_reader_end (self->loop, self->subscriber);
      zsock_destroy (&self->subscriber);
    }
    if (self->command_listener) {
      zloop_reader_end (self->loop, self->command_listener);
      zsock_destroy (&self->command_listener);
    }
    free (self);
    *self_p = NULL;
  }

  TRACE (FUNCTIONS, "Leaving csmm_destroy");
}


//  --------------------------------------------------------------------------
//  Verifies if the input parameter is a feeder
//

static bool
csmm_live_feeder_is (void *self)
{
  TRACE(FUNCTIONS, "Entering in csmm_live_feeder_is");
  assert (self);
  TRACE (FUNCTIONS, "Leaving csmm_live_feeder_is");

  return ((live_feeder_t *) self)->tag == LIVE_FEEDER_TAG;
}


//  --------------------------------------------------------------------------
//  Creates a feeder representation
//  Input:
//    stream: stream's name
//    ip: feeder's address
//    port: feeder's port
//  Output:
//    The created feeder

static live_feeder_t*
csmm_live_feeder_new (csstring_t *stream, csstring_t *ip, int port, char type)
{
  live_feeder_t *self;
  unsigned long net_addr;

  TRACE(FUNCTIONS, "Entering in csmm_live_feeder_new");

  self = (live_feeder_t *) zmalloc (sizeof (live_feeder_t));
  if (self) {
    self->tag = LIVE_FEEDER_TAG;
    self->ip = ip;
    self->port = port;
    self->stream_name = stream;
    self->feeder_type = type;
    self->free = true;
    self->channel = -1;
  }

  bzero (&self->serv_addr, sizeof (self->serv_addr));
  self->channel = socket (AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  self->serv_addr.sin_family = AF_INET;
  net_addr = inet_network (csstring_data (self->ip));
  self->serv_addr.sin_addr.s_addr = htonl (net_addr);
  self->serv_addr.sin_port = htons (self->port);

  TRACE (FUNCTIONS, "Leaving csmm_live_feeder_new");

  return self;
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in a feeder
//  Input:
//    A feeder

static void
csmm_live_feeder_destroy (live_feeder_t **self_p)
{
  TRACE (FUNCTIONS, "Entering in csmm_live_feeder_destroy");

  assert (self_p);
  if (*self_p) {
    live_feeder_t *self = *self_p;
    assert (csmm_live_feeder_is (self));
    csstring_destroy (&self->ip);
    csstring_destroy (&self->stream_name);
    close (self->channel);
    free (self);
    *self_p = NULL;
  }

  TRACE (FUNCTIONS, "Leaving csmm_live_feeder_destroy");
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in a feeder
//  Input:
//    A feeder

static void
csmm_live_feeder_destructor (void **item)
{
  TRACE (FUNCTIONS, "Entering in csmm_live_feeder_destructor");
  csmm_live_feeder_destroy ((live_feeder_t **) item);
  TRACE (FUNCTIONS, "Leaving csmm_live_feeder_destructor");
}


//  --------------------------------------------------------------------------
//  Verifies if the input parameter is a call player
//

static bool
csmm_call_player_is (void *self)
{
  TRACE(FUNCTIONS, "Entering in csmm_call_player_is");
  assert (self);
  TRACE (FUNCTIONS, "Leaving csmm_call_player_is");

  return ((call_player_t *) self)->tag == CALL_PLAYER_TAG;
}


//  --------------------------------------------------------------------------
//  Creates a call player representation
//  Input:
//    stream: stream's name
//    feeder: feeder's name
//  Output:
//    The created call player

static call_player_t*
csmm_call_player_new (csstring_t *stream, csstring_t *feeder)
{
  call_player_t *self;

  TRACE(FUNCTIONS, "Entering in csmm_call_player_new");

  self = (call_player_t *) zmalloc (sizeof (call_player_t));
  if (self) {
    self->tag = CALL_PLAYER_TAG;
    self->stream_name = stream;
    self->free = true;
    self->feeder_name = feeder;
    self->executor = NULL;
    self->call_id = 0;
    self->call_dbId = 0;
  }

  TRACE (FUNCTIONS, "Leaving csmm_call_player_new");

  return self;
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in a call player
//  Input:
//    A call player

static void
csmm_call_player_destroy (call_player_t **self_p)
{
  TRACE (FUNCTIONS, "Entering in csmm_call_player_destroy");

  assert (self_p);
  if (*self_p) {
    call_player_t *self = *self_p;
    assert (csmm_call_player_is (self));
    csstring_destroy (&self->file_name);
    csstring_destroy (&self->feeder_name);
    csstring_destroy (&self->stream_name);
    if (self->executor) {
      zactor_destroy (&self->executor);
    }
    free (self);
    *self_p = NULL;
  }

  TRACE (FUNCTIONS, "Leaving csmm_call_player_destroy");
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in a call player
//  Input:
//    A call player

static void
csmm_call_player_destructor (void **item)
{
  TRACE (FUNCTIONS, "Entering in csmm_call_player_destructor");
  csmm_call_player_destroy ((call_player_t **) item);
  TRACE (FUNCTIONS, "Leaving csmm_call_player_destructor");
}


//  --------------------------------------------------------------------------
//  Finds a free call player
//  Input:
//    The target Call Stream Media Manager context
//  Output:
//    The free call player

static call_player_t*
csmm_find_call_player (csmm_t *ctx)
{
  call_player_t *call_player;

  TRACE (FUNCTIONS, "Entering in csmm_find_call_player");

  call_player = (call_player_t *) (zlist_first (ctx->call_players));

  while (call_player) {
    if (call_player->free == true)
      break;
    else
      call_player = (call_player_t *) (zlist_next (ctx->call_players));
  }

  TRACE (FUNCTIONS, "Leaving csmm_find_call_player");

  return call_player;
}


//  --------------------------------------------------------------------------
//  Finds a free call player with a specific call id
//  Input:
//    The target Call Stream Media Manager context
//    The call identifier
//  Output:
//    The free call player

static call_player_t*
csmm_find_call_player_by_call_id (csmm_t *ctx, UINT32 call_id, UINT32 call_dbId)
{
  call_player_t *call_player;

  TRACE (FUNCTIONS, "Entering in csmm_find_call_player_by_call_id");

  call_player = (call_player_t *) (zlist_first (ctx->call_players));

  while (call_player) {
    if (!call_player->free && (call_player->call_id == call_id) && (call_player->call_dbId == call_dbId))
      break;
    else
      call_player = (call_player_t *) (zlist_next (ctx->call_players));
  }

  TRACE (FUNCTIONS, "Leaving csmm_find_call_player_by_call_id");

  return call_player;
}


//  --------------------------------------------------------------------------
//  Verifies if the input parameter is a call

static bool
csmm_live_call_is (void *self)
{
  TRACE (FUNCTIONS, "Entering in csmm_live_call_is");
  assert (self);
  TRACE (FUNCTIONS, "Leaving csmm_live_call_is");

  return ((live_call_t *) self)->tag == LIVE_CALL_TAG;
}


//  --------------------------------------------------------------------------
//  Creates an active call's representation
//  Input:
//    The call identifier
//  Output:
//    The active call's representation

static live_call_t*
csmm_live_call_new (UINT32 call_id, char call_type)
{
  live_call_t *self;

  TRACE (FUNCTIONS, "Entering in csmm_live_call_new");

  self = (live_call_t *) zmalloc (sizeof (live_call_t));
  if (self) {
    self->tag = LIVE_CALL_TAG;
    self->id = call_id;
    self->voice_data_stream_a = NULL;
    self->voice_data_stream_b = NULL;
    self->call_type = call_type;
    self->last_activity = time (NULL);
  }

  TRACE (FUNCTIONS, "Leaving csmm_live_call_new");

  return self;
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in an active call
//  Input:
//    An active call's representation

static void
csmm_live_call_destroy (live_call_t **self_p)
{
  TRACE (FUNCTIONS, "Entering in csmm_live_call_destroy");

  assert (self_p);
  if (*self_p) {
    live_call_t *self = *self_p;
    assert (csmm_live_call_is (self));
    if (self->live_feeder) {
      self->live_feeder->free = true;
      self->live_feeder = NULL;
    }
    if (self->subscriber) {
      zsock_destroy (&self->subscriber);
    }
    if (self->voice_data_stream_a) {
      zchunk_destroy (&self->voice_data_stream_a);
      self->voice_data_stream_a = NULL;
    }
    if (self->voice_data_stream_b) {
      zchunk_destroy (&self->voice_data_stream_b);
      self->voice_data_stream_b = NULL;
    }
    free (self);
    *self_p = NULL;
  }

  TRACE (FUNCTIONS, "Leaving csmm_live_call_destroy");
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in an active call
//  Input:
//    An active call's representation

static void
csmm_live_call_destructor (void **item)
{
  TRACE (FUNCTIONS, "Entering in csmm_live_call_destructor");
  csmm_live_call_destroy ((live_call_t **) item);
  TRACE (FUNCTIONS, "Leaving csmm_live_call_destructor");
}


//  --------------------------------------------------------------------------
//  Finds an active call identified by call_id in the Media manager's thread
//  context
//  Input:
//    The target Call Stream Media Manager context
//    The call identifier
//  Output:
//    The live call representation or NULL

static live_call_t*
csmm_find_live_call (csmm_t *ctx, UINT32 call_id)
{
  live_call_t *live_call;

  TRACE (FUNCTIONS, "Entering in csmm_find_live_call");

  live_call = (live_call_t *) zlist_first (ctx->live_calls);

  while (live_call) {
    if (live_call->id == call_id)
      break;
    else
      live_call = (live_call_t *) zlist_next (ctx->live_calls);
  }

  TRACE (FUNCTIONS, "Leaving csmm_find_live_call");

  return live_call;
}


//  --------------------------------------------------------------------------
//  Inserts an active call identified by call_id into the Media manager's thread
//  context
//  Input:
//    The target Call Stream Media Manager context
//    The call identifier
//  Output:
//    0: call inserted
//   -1: call not inserted

static int
csmm_insert_live_call(csmm_t *ctx, UINT32 call_id, char call_type)
{
  int rc = 0;
  live_call_t *live_call;

  TRACE (FUNCTIONS, "Entering in csmm_insert_live_call");

  live_call = csmm_live_call_new (call_id, call_type);
  assert (live_call);

  rc = zlist_append (ctx->live_calls, live_call);

  TRACE (FUNCTIONS, "Leaving csmm_insert_live_call");


  return rc;
}


//  --------------------------------------------------------------------------
//  Removes an active call identified by call_id from the Media manager's thread
//  context
//  Input:
//    The target Call Stream Media Manager context
//    The call identifier
//  Output:
//    0: call removed
//   -1: call not removed or not found

static int
csmm_remove_live_call (csmm_t *ctx, UINT32 call_id)
{
  int rc = 0;
  live_call_t* live_call;

  TRACE (FUNCTIONS, "Entering in csmm_remove_live_call");

  live_call = csmm_find_live_call (ctx, call_id);

  if (live_call) {
    if (live_call->live_feeder) {
      live_call->live_feeder->free = true;
      live_call->live_feeder = NULL;
    }
    if (live_call->subscriber) {
      zloop_reader_end (ctx->loop, live_call->subscriber);
      zsock_destroy (&live_call->subscriber);
    }
    zlist_remove (ctx->live_calls, live_call);
  } else {
    TRACE (ERROR, "Call with id <%u> not found", call_id);
    rc = -1;
  }

  TRACE (FUNCTIONS, "Leaving csmm_remove_live_call");

  return rc;
}


//  --------------------------------------------------------------------------
//  Callback responsible for processing the query of all active calls request.
//  Input:
//    The Call Stream Media Manager context of the thread
//    The reply to the requester
//  Output:
//    0: processed
//   -1: not processed

static int
csmm_get_live_calls (csmm_t *ctx, zmsg_t *response)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_get_live_calls");

  if (zlist_size (ctx->live_calls) > 0) {
    zmsg_addstrf (response, "%zu", zlist_size (ctx->live_calls));
    live_call_t *call = (live_call_t *) zlist_first (ctx->live_calls);
    while (call) {
      TRACE (DEBUG, "Call: %u", call->id);
      zmsg_addstrf (response, "%u", call->id);
      call = (live_call_t *) zlist_next (ctx->live_calls);
    }
  } else {
    zmsg_addstr (response, "0");
  }

  TRACE (FUNCTIONS, "Leaving csmm_get_live_calls");

  return rc;
}


//  --------------------------------------------------------------------------
//  Callback responsible for processing a received voice LogApi messages.
//  Input:
//    loop: the event-driven reactor
//    reader: the descriptor with the data received ready to read
//    arg: the Call Stream Media Manager context of the thread
//  Output:
//    0: processed
//   -1: not processed

static int
csmm_voice_data_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  int rc = 0;
  UINT32 call_id;
  csmm_t *ctx;
  zmsg_t* msg;
  char *tag;
  zframe_t *timestamp;
  zframe_t *log_api_msg;
  zframe_t *data;

  TRACE (FUNCTIONS, "Entering in csmm_voice_data_handler");

  ctx = (csmm_t *) arg;

  msg = zmsg_recv (reader);
  assert (msg);
//  zmsg_print (msg);

  tag = zmsg_popstr (msg);
  timestamp = zmsg_pop (msg);
  log_api_msg = zmsg_pop (msg);
  data = zmsg_pop (msg);

  if (zframe_size (timestamp) != sizeof (time_t)) {
    TRACE (ERROR, "Timestamp: Bad format");
    rc = -1;
  }

  rc = sscanf (tag, "V_%u", &call_id);

  if (rc != EOF && rc > 0) {


    live_call_t *call = csmm_find_live_call (ctx, call_id);
    if (call) {

      time_t now = time (NULL);
      call->last_activity = now;

      if (call->live_feeder) {

        //
        // Duplex calls. Merge stream A and stream B frames
        //

        if (call->call_type == 'D') {

          //
          // Fetch the originator stream
          //

          LogApiVoice *log_api_voice;
          log_api_voice = (LogApiVoice *) zframe_data (log_api_msg);
          StreamOriginatorEnum originator = log_api_voice->m_uiStreamOriginator;
          TRACE (DEBUG, "Duplex call. Originator: <%d>", originator);

          // Protect from unaligned streams
          //
          if (originator == STREAM_ORG_A_SUB || (originator == STREAM_ORG_B_SUB && call->voice_data_stream_a != NULL)) {

            //
            // Cache partial frames
            //

            if (originator == STREAM_ORG_A_SUB) {
              TRACE (DEBUG, "LMIG: Caching Channel 1");
              call->voice_data_stream_a = zchunk_new (zframe_data (data), zframe_size (data));
            }
            if (originator == STREAM_ORG_B_SUB) {
              TRACE (DEBUG, "LMIG: Caching Channel 2");
              call->voice_data_stream_b = zchunk_new (zframe_data (data), zframe_size (data));
            }
        
            if (call->voice_data_stream_a != NULL && call->voice_data_stream_b != NULL) {

              //
              // Merge stream A frame with stream B frame
              //

              TRACE (DEBUG, "LMIG: Merging channel 1 with channel 2");

              zchunk_t *voice_data = zchunk_new (NULL, zchunk_size(call->voice_data_stream_a) +
                                                       zchunk_size(call->voice_data_stream_b));

              int block_size = zchunk_size(call->voice_data_stream_b);
              int i = 0;

              for (i = 0; i < block_size; i++) {
                zchunk_append (voice_data, zchunk_data (call->voice_data_stream_a) + i, 1);
                zchunk_append (voice_data, zchunk_data (call->voice_data_stream_b) + i, 1);
              }

              zchunk_destroy (&call->voice_data_stream_a);
              zchunk_destroy (&call->voice_data_stream_b);
              call->voice_data_stream_a = NULL;
              call->voice_data_stream_b = NULL;

              //
              // Broadcast merged frames
              //

              TRACE (DEBUG, "Sending duplex data voice with call id <%u> to feeder <%s>",
                  call->id, csstring_data (call->live_feeder->stream_name));

              sendto (call->live_feeder->channel,
                  zchunk_data (voice_data),
                  zchunk_size (voice_data),
                  0,
                  (struct sockaddr *) &call->live_feeder->serv_addr,
                  sizeof (call->live_feeder->serv_addr));

              zchunk_destroy (&voice_data);
            }
          } else {
            TRACE (DEBUG, "LMIG: Channel 2 arrived without channel 1");
          }
        } else {

          //
          // Simplex and Group calls
          //

          TRACE (DEBUG, "Sending data voice with call id <%u> to feeder <%s>",
              call->id, csstring_data (call->live_feeder->stream_name));

          //
          // Broadcast frame
          //

          sendto (call->live_feeder->channel,
              zframe_data (data),
              zframe_size (data),
              0,
              (struct sockaddr *) &call->live_feeder->serv_addr,
              sizeof (call->live_feeder->serv_addr));
        } 
      } else {
        TRACE (ERROR, "No feeder found for call <%u>", call->id);
        rc = -1;
      }
    } else {
      TRACE (ERROR, "No call found for id <%u>", call->id);
      rc = -1;
    }
  } else {
    TRACE (ERROR, "Tag: Bad format");
    rc = -1;
  }

  free (tag);
  zframe_destroy (&timestamp);
  zframe_destroy (&log_api_msg);
  zframe_destroy (&data);
  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving csmm_voice_data_handler");

  return 0;
}


//  --------------------------------------------------------------------------
//  Callback responsible for processing a call interception request.
//  Input:
//    ctx: the Call Stream Media Manager context of the thread
//    call_id: identification of the call to be intercepted
//    response: the response that will be sent to the requester
//  Output:
//    0: processed
//   -1: not processed

static int
csmm_start_broadcast_live_call (csmm_t *ctx, UINT32 call_id, char *call_format, zmsg_t *response)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_start_broadcast_live_call");

  live_call_t *call = csmm_find_live_call (ctx, call_id);

  if (call) {
    if (call->live_feeder && call->live_feeder->free == false) {
        zmsg_addstr (response, "OK");
        zmsg_addstrf (response, "%s/%s.%s", csstring_data (ctx->media_server_endpoint),
            csstring_data (call->live_feeder->stream_name), call_format);
    } else {
      live_feeder_t *live_feeder = (live_feeder_t *) zlist_first (ctx->live_feeders);
      while (live_feeder) {

        // A live_feeder is apropiate to handle a call whenever:
        //  The feeder is free
        //  and
        //    The call type is "D"uplex and the feeder type is "S"tereo
        //    or
        //    The call type is "S"implex or "G"roup and the feeder type is "M"ono

        if (
             (live_feeder->free) && 
             (
               (call->call_type == 'D' && live_feeder->feeder_type == 'S') || 
               ((call->call_type == 'S' || call->call_type == 'G') && live_feeder->feeder_type == 'M')
             )
           )
          break;
        else
          live_feeder = (live_feeder_t *) zlist_next (ctx->live_feeders);
      }
      if (live_feeder) {
        live_feeder->free = false;
        call->live_feeder = live_feeder;
        call->subscriber = zsock_new_sub (">inproc://collector", 0);
        assert (call->subscriber);
        char call_id_str[64];
        snprintf (call_id_str, 64, "V_%u", call_id);
        zsock_set_subscribe (call->subscriber, call_id_str);
        rc = zloop_reader (ctx->loop, call->subscriber, csmm_voice_data_handler,
            ctx);
        zmsg_addstr (response, "OK");
        zmsg_addstrf (response, "%s/%s.%s", csstring_data (ctx->media_server_endpoint),
            csstring_data (live_feeder->stream_name), call_format);
      } else {
        TRACE (ERROR, "No available feeder resource found for call with id <%u>",
            call_id);
        zmsg_addstr (response, "NOK");
        zmsg_addstr (response, "Feeder not available");
        rc = -1;
      }
    }
  } else {
    TRACE (ERROR, "Call with id <%u> not found", call_id);
    zmsg_addstr (response, "NOK");
    zmsg_addstrf (response, "Call <%u> not found", call_id);
    rc = -1;
  }

  TRACE (FUNCTIONS, "Leaving csmm_start_broadcast_live_call");

  return rc;
}


//  --------------------------------------------------------------------------
//  Callback responsible for processing the stop call interception request.
//  Input:
//    ctx: the Call Stream Media Manager context of the thread
//    call_id: identification of the call to be intercepted
//    response: the response that will be sent to the requester
//  Output:
//    0: processed
//   -1: not processed

static int
csmm_stop_broadcast_live_call (csmm_t *ctx, UINT32 call_id, zmsg_t *response)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_stop_broadcast_live_call");

  live_call_t *call = csmm_find_live_call (ctx, call_id);

  if (call) {
    if (call->live_feeder && (call->live_feeder->free == false)) {
      call->live_feeder->free = true;
      call->live_feeder = NULL;
      zloop_reader_end(ctx->loop, call->subscriber);
      zsock_destroy(&call->subscriber);
    } else {
      TRACE (ERROR, "Call with id <%u> not intercepted", call_id);
      zmsg_addstr (response, "NOK");
      zmsg_addstrf (response, "Call <%u> not intercepted", call_id);
      rc = -1;
    }
  } else {
    TRACE (ERROR, "Call with id <%u> not found", call_id);
    zmsg_addstr (response, "NOK");
    zmsg_addstrf (response, "Call <%u> not found", call_id);
    rc = -1;
  }

  if (rc == 0) {
    zmsg_addstr (response, "OK");
    zmsg_addstr (response, "OK");
  }

  TRACE (FUNCTIONS, "Leaving csmm_stop_broadcast_live_call");

  return rc;
}


//  --------------------------------------------------------------------------
//  Creates a file with the voice data passed as parameter
//  Input:
//    path: the absolute path to the file
//    data: the raw voice data
//    len: the length's voice data
//  Output:
//    0 - ok
//   -1 - nok

static int
csmm_copy_db_voice_call_to_file_helper (const char * path, const char * data, UINT32 len)
{
  struct stat file_stat;
  int rc = 0;
  FILE *fp = NULL;

  TRACE (FUNCTIONS, "Entering in csmm_copy_db_voice_call_to_file_helper");

  if (stat (path, &file_stat) == 0) {
    unlink (path);
  }

  TRACE (DEBUG, "Create file: <%s>", path);

  if ((fp = fopen (path, "wb")) == NULL) {
    TRACE (ERROR, "Error: fopen (), errno = %d text = %s", errno, strerror (errno));
    rc = -1;
  }
  if ((rc == 0) && (fwrite (data, 1, len, fp) != len)) {
    TRACE(ERROR, "Error: fwrite(), errno = %d text = %s", errno, strerror (errno));
    rc = -1;
  }

  if (fp) {
    fclose (fp);
    fp = NULL;
  }

  if ((rc == -1) && (stat (path, &file_stat) == 0)) {
    unlink (path);
  }

  TRACE (FUNCTIONS, "Leaving csmm_copy_db_voice_call_to_file_helper");

  return rc;
}


//  --------------------------------------------------------------------------
//  Extracts the raw alaw voice data identified by a call id and writes to a file
//  in the wav file format
//  Input:
//    ctx: the Call Stream Media Manager context of the thread
//    call_type: 'S' or 'D' (individual -simplex or duplex-) or 'G' (group) call
//    call_id: identification of the call to be intercepted
//  Output:
//    0 - ok
//   -1 - nok

static int
csmm_copy_db_voice_call_to_file (csmm_t *ctx, char *call_type, char *call_format, UINT32 call_id, UINT32 call_dbId)
{
  int rc = 0;
  int size = 0;
  char *contents = NULL;
  PGresult *res = NULL;
  char *command = NULL;
  char call_table[CSMM_TMP_BUFFER];
  char voice_table[CSMM_TMP_BUFFER];
//  char db_id[CSMM_TMP_BUFFER];

  TRACE (FUNCTIONS, "Entering in csmm_copy_db_voice_call_to_file");

  if (!strcmp (call_type, "G")) {
    strncpy (call_table, "d_callstream_groupcall", CSMM_TMP_BUFFER);
    strncpy (voice_table, "d_callstream_voicegroupcall", CSMM_TMP_BUFFER);
  } else if (!strcmp (call_type, "I")) {
    strncpy (call_table, "d_callstream_indicall", CSMM_TMP_BUFFER);
    strncpy (voice_table, "d_callstream_voiceindicall", CSMM_TMP_BUFFER);
  } else {
    TRACE (ERROR, "Tables not found");
    rc = -1;
  }

//  if (rc == 0) {
//
//    //
//    // Get the DB call id
//    //
//
//    command = "SELECT db_id "
//        "FROM %s "
//        "WHERE call_id = %u "
//        "ORDER BY call_begin DESC LIMIT 1";
//    snprintf (ctx->work_area, CSMM_BUFFER_WORK_AREA_LENGTH, command, call_table,
//        call_id);
//    TRACE (DEBUG, "Executing <%s>", ctx->work_area);
//    res = PQexec(ctx->pg_conn, ctx->work_area);
//    if (res && (PQresultStatus (res) == PGRES_TUPLES_OK)) {
//      int affected = PQntuples (res);
//      TRACE(DEBUG, "Rows affected <%d>", affected);
//      if (affected == 1) {
//        strncpy (db_id, PQgetvalue(res, 0, 0), CSMM_TMP_BUFFER);
//        TRACE (DEBUG, "Result: <%s>", db_id);
//      } else {
//        TRACE (ERROR, "Too much rows affected");
//        rc = -1;
//      }
//    } else {
//      TRACE (ERROR, "SELECT failed: <%s>", PQerrorMessage(ctx->pg_conn));
//      rc = -1;
//    }
//    PQclear(res);
//  }

  if (rc == 0) {

    //
    // Get the voice data
    //

    command = "SELECT voice_data "
        "FROM %s "
        "WHERE db_id = %d";
    TRACE (DEBUG, "command <%s>", command);
    snprintf (ctx->work_area,
        CSMM_BUFFER_WORK_AREA_LENGTH, command, voice_table, call_dbId);
    TRACE (DEBUG, "Executing <%s>", ctx->work_area);
    res = PQexecParams (ctx->pg_conn,
        ctx->work_area, 0, NULL, NULL, NULL, NULL, 1);
    if (res && (PQresultStatus (res) == PGRES_TUPLES_OK)) {
      size = PQgetlength (res, 0, 0);
      contents = PQgetvalue (res, 0, 0);
      TRACE(DEBUG, "Result: voice data of size <%d> bytes", size);
    } else {
      TRACE(ERROR, "SELECT failed: <%s>", PQerrorMessage(ctx->pg_conn));
      rc = -1;
    }

    if (rc == 0) {

      //
      // Save the voice data to a file
      // 

      call_player_t *call_player = csmm_find_call_player_by_call_id (ctx, call_id, call_dbId);
      if (call_player) {
        csstring_destroy (&call_player->file_name);
        char path[PATH_MAX];
        snprintf (path, PATH_MAX, csstring_data (ctx->filename_template), 
            call_dbId,
            call_id,
            csstring_data (call_player->feeder_name), 
            call_format);
        call_player->file_name = csstring_new (path);
        rc = csmm_copy_db_voice_call_to_file_helper (path, contents, size);
      } else {
        TRACE (ERROR, "Call player unavailable");
        rc = -1;
      }
    }
    PQclear (res);
  }

  TRACE (FUNCTIONS, "Leaving csmm_copy_db_voice_call_to_file");

  return rc;
}

static int
csmm_copy_db_voice_call_to_file_v2 (csmm_t *ctx, char *call_type, char *call_format, UINT32 call_id, UINT32 call_dbId, char *session)
{
  int rc = 0;
  int size = 0;
  char *contents = NULL;
  PGresult *res = NULL;
  char *command = NULL;
  char call_table[CSMM_TMP_BUFFER];
  char voice_table[CSMM_TMP_BUFFER];

  TRACE (FUNCTIONS, "Entering in csmm_copy_db_voice_call_to_file_v2");

  if (!strcmp (call_type, "G")) {
    strncpy (call_table, "d_callstream_groupcall", CSMM_TMP_BUFFER);
    strncpy (voice_table, "d_callstream_voicegroupcall", CSMM_TMP_BUFFER);
  } else if (!strcmp (call_type, "I")) {
    strncpy (call_table, "d_callstream_indicall", CSMM_TMP_BUFFER);
    strncpy (voice_table, "d_callstream_voiceindicall", CSMM_TMP_BUFFER);
  } else {
    TRACE (ERROR, "Tables not found");
    rc = -1;
  }

  if (rc == 0) {

    //
    // Get the voice data
    //

    command = "SELECT voice_data "
        "FROM %s "
        "WHERE db_id = %d";
    TRACE (DEBUG, "command <%s>", command);
    snprintf (ctx->work_area,
        CSMM_BUFFER_WORK_AREA_LENGTH, command, voice_table, call_dbId);
    TRACE (DEBUG, "Executing <%s>", ctx->work_area);
    res = PQexecParams (ctx->pg_conn,
        ctx->work_area, 0, NULL, NULL, NULL, NULL, 1);
    if (res && (PQresultStatus (res) == PGRES_TUPLES_OK)) {
      size = PQgetlength (res, 0, 0);
      contents = PQgetvalue (res, 0, 0);
      TRACE(DEBUG, "Result: voice data of size <%d> bytes", size);
    } else {
      TRACE(ERROR, "SELECT failed: <%s>", PQerrorMessage(ctx->pg_conn));
      rc = -1;
    }

    if (rc == 0) {

      //
      // Save the voice data to a file
      // 

      char filename[PATH_MAX];
      snprintf (filename, PATH_MAX, "voice_%d_%d_%s",
          call_dbId,
          call_id,
          session);

      TRACE (DEBUG, "Unhashed file: <%s>", filename);

      char hashed_filename[PATH_MAX];
      memset(hashed_filename, 0x00, PATH_MAX);
      {
        int i = 0;
        char* hashed_filename_ptr = &hashed_filename[0];
        unsigned char hash[32];
        memset(hash, 0x00, 32);
        MD5_CTX mdContext;
        MD5_Init (&mdContext);  
        MD5_Update (&mdContext, filename, strlen(filename));  
        MD5_Final (hash, &mdContext);  
        for (i = 0; i < 16; i++) {
          hashed_filename_ptr += sprintf(hashed_filename_ptr, "%02x", hash[i]);
        }
      }

      char hashed_path[PATH_MAX];
      snprintf (hashed_path, PATH_MAX, "%s/%s.%s",
          csstring_data (ctx->voicerec_repo), 
          hashed_filename,
          call_format);


      rc = csmm_copy_db_voice_call_to_file_helper (hashed_path, contents, size);

    }

    PQclear (res);
  }

  TRACE (FUNCTIONS, "Leaving csmm_copy_db_voice_call_to_file_v2");

  return rc;
}


//  --------------------------------------------------------------------------
//  Callback handler. Process the finalization signal of a child call player
//  Input:
//    loop: the reactor
//    reader: the shared channel established with a child player
//    arg: the call player's context
//  Output:
//    0 - Ok
//    1 - Nok

static int
csmm_call_player_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  bool command_handled = false;
  int result = 0;

  TRACE (FUNCTIONS, "Entering in csmm_call_player_handler");

  zmsg_t *msg = zmsg_recv (reader);
  zmsg_print (msg);

  if (!msg)
    result = 1;

  char *command = zmsg_popstr (msg);
  TRACE (DEBUG, "Command: %s", command);

  if ((!command_handled) && streq (command, "PLAYER_FINISHED")) {
    call_player_t *call_player = (call_player_t *) arg;
    assert (csmm_call_player_is (call_player));
    command_handled = true;
    if (call_player->executor) {
      zactor_destroy (&call_player->executor);
    }
    call_player->free = true;
    unlink (csstring_data (call_player->file_name));
    zloop_reader_end(loop, reader);
    TRACE (DEBUG, "Call player with feeder <%s> released",
            csstring_data (call_player->feeder_name));
  }

  if (!command_handled) {
    TRACE (ERROR, "Invalid message");
    //assert (false);
  }

  free (command);
  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving csmm_call_player_handler");

  return result;
}


//  --------------------------------------------------------------------------
//  Callback responsible for processing the play call request.
//  Input:
//    ctx: the Call Stream Media Manager context of the thread
//    call_type: 'S' or 'D' (individual -simplex or duplex-) or 'G' (group) call
//    call_id: identification of the call to be intercepted
//    response: the response that will be sent to the requester
//  Output:
//    0: processed
//   -1: not processed

static int
csmm_start_play_call (csmm_t *ctx, char *call_type, UINT32 call_id, UINT32 call_dbId, char *call_format, zmsg_t *response)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_start_play_call");

  // Find a free call player
  //
  call_player_t *call_player = csmm_find_call_player (ctx);

  if (call_player) {
    call_player->free = false;
    call_player->call_id = call_id;
    call_player->call_dbId = call_dbId;

    TRACE (DEBUG, "Call player found with feeder <%s>",
        csstring_data (call_player->feeder_name));

    // Build the player command
    //
    char player_command_aux[PATH_MAX];
    char player_command[PATH_MAX];
    snprintf (player_command_aux, sizeof (player_command_aux),
        csstring_data (ctx->player_command_template),
        csstring_data (ctx->filename_template),
        csstring_data (call_player->feeder_name),
        csstring_data (call_player->feeder_name));
    snprintf (player_command, sizeof (player_command),
        player_command_aux,
        call_dbId,
        call_id,
        csstring_data (call_player->feeder_name),
        call_format);

    // Build the file to play
    //
    rc = csmm_copy_db_voice_call_to_file (ctx, call_type, call_format, call_id, call_dbId);

    if (rc == 0) {
      // Execute the player command
      //
      call_player->executor = zactor_new (csply_task, player_command);
      assert (call_player->executor);
      zloop_reader (ctx->loop, (zsock_t *) call_player->executor,
          csmm_call_player_handler, call_player);

      // Returns to the client the stream's url from which the call can be heard
      //
      zmsg_addstr (response, "OK");
      zmsg_addstrf (response, "%s/%s.%s", csstring_data(ctx->media_server_endpoint),
          csstring_data(call_player->stream_name), call_format);
    } else {
      TRACE (DEBUG, "Call not found");
      call_player->free = true;
      zmsg_addstr (response, "NOK");
      zmsg_addstrf (response, "Call <%u> not found", call_id);
    }
  } else {
    TRACE (DEBUG, "Call player not found");
    rc = -1;
    zmsg_addstr (response, "NOK");
    zmsg_addstr (response, "Player unavailable");
  }

  TRACE (FUNCTIONS, "Leaving csmm_start_play_call (%d)", rc);

  return rc;
}

static int
csmm_start_play_call_v2 (csmm_t *ctx, char *call_type, UINT32 call_id, UINT32 call_dbId, char *call_format, char *session, zmsg_t *response)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_start_play_call_v2");

  // Build the file to play
  //
//  rc = csmm_copy_db_voice_call_to_file (ctx, call_type, call_format, call_id, call_dbId);
  rc = csmm_copy_db_voice_call_to_file_v2 (ctx, call_type, call_format, call_id, call_dbId, session);

  if (rc == 0) {

    // Returns to the client the stream's url from which the call can be heard
    //
 
    char filename[PATH_MAX];

    snprintf (filename, PATH_MAX, "voice_%d_%d_%s",
         call_dbId,
         call_id,
         session);

    TRACE (DEBUG, "Unhashed file: <%s>", filename);

    char hashed_filename[PATH_MAX];
    memset (hashed_filename, 0x00, PATH_MAX);
    {
      int i = 0;
      char* hashed_filename_ptr = &hashed_filename[0];
      unsigned char hash[32];
      memset(hash, 0x00, 32);;
      MD5_CTX mdContext;
      MD5_Init (&mdContext);
      MD5_Update (&mdContext, filename, strlen(filename));
      MD5_Final (hash, &mdContext);
      for (i = 0; i < 16; i++) {
        hashed_filename_ptr += sprintf(hashed_filename_ptr, "%02x", hash[i]);
      }
    }

    zmsg_addstr (response, "OK");
    zmsg_addstrf (response, "/%s/%s.%s",
         csstring_data (ctx->voicerec_url),
         hashed_filename,
         call_format);

  } else {
    TRACE (DEBUG, "Call not found");
    zmsg_addstr (response, "NOK");
    zmsg_addstrf (response, "Call <%u> not found", call_id);
  }

  TRACE (FUNCTIONS, "Leaving csmm_start_play_call_v2 (%d)", rc);

  return rc;
}


//  --------------------------------------------------------------------------
//  Callback responsible for processing the stop play call request.
//  Input:
//    ctx: the Call Stream Media Manager context of the thread
//    call_type: 'S' or 'D' (individual -simplex or duplex-) or 'G' (group) call
//    call_id: identification of the call to be intercepted
//    response: the response that will be sent to the requester
//  Output:
//    0: processed
//   -1: not processed

static int
csmm_stop_play_call (csmm_t *ctx, char* call_type, UINT32 call_id, UINT32 call_dbId, zmsg_t *response)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_stop_play_call");

  call_player_t *call_player = csmm_find_call_player_by_call_id (ctx, call_id, call_dbId);

  if (call_player) {
    zmsg_t *order = zmsg_new ();
    zmsg_addstr (order, "STOP");
    zmsg_send (&order, (zsock_t *) call_player->executor);
    zmsg_destroy (&order);
    zmsg_addstr (response, "OK");
    zmsg_addstr (response, "OK");
    unlink (csstring_data (call_player->file_name));
  } else {
    zmsg_addstr (response, "NOK");
    zmsg_addstr (response, "Call player not found");
  }

  TRACE (FUNCTIONS, "Leaving csmm_stop_play_call");

  return rc;
}

static int
csmm_stop_play_call_v2 (csmm_t *ctx, char* call_type, UINT32 call_id, UINT32 call_dbId, char *call_format, char *session, zmsg_t *response)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csmm_stop_play_call_v2");

  zmsg_addstr (response, "OK");
  zmsg_addstr (response, "OK");

  char filename[PATH_MAX];
  snprintf (filename, PATH_MAX, "voice_%d_%d_%s",
      call_dbId,
      call_id,
      session);

  TRACE (DEBUG, "Unhashed file: <%s>", filename);

  char hashed_filename[PATH_MAX];
  memset(hashed_filename, 0x00, PATH_MAX);
  {
    int i = 0;
    char* hashed_filename_ptr = &hashed_filename[0];
    unsigned char hash[32];
    memset(hash, 0x00, 32);
    MD5_CTX mdContext;
    MD5_Init (&mdContext);
    MD5_Update (&mdContext, filename, strlen(filename));
    MD5_Final (hash, &mdContext);
    for (i = 0; i < 16; i++) {
      hashed_filename_ptr += sprintf(hashed_filename_ptr, "%02x", hash[i]);
    }
  }

  char hashed_path[PATH_MAX];
  snprintf (hashed_path, PATH_MAX, "%s/%s.%s",
    csstring_data (ctx->voicerec_repo),
    hashed_filename,
    call_format);

  TRACE (DEBUG, "Delete file: <%s>", hashed_path);

  unlink (hashed_path);

  TRACE (FUNCTIONS, "Leaving csmm_stop_play_call_v2");

  return rc;
}

//  --------------------------------------------------------------------------
//  Callback handler. Analyzes and process commands sent by the parent thread
//  through the shared pipe and api requests sent by external clients.
//  Input:
//    loop: the reactor
//    reader: the parent thread endpoint or the channel established with an external client
//    arg: the Call Stream Media Manager context
//  Output:
//    0 - Ok
//    1 - Nok

static int
csmm_command_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  bool command_handled = false;
  int result = 0;

  TRACE (FUNCTIONS, "Entering in csmm_command_handler");

  csmm_t *ctx = (csmm_t *) arg;

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

  if ((!command_handled) && streq (command, "START_CALL_INTERCEPTION")) {
    command_handled = true;
    zmsg_t *response = zmsg_new ();
    UINT32 call_id = zmsg_popint (msg);
    char *call_format = zmsg_popstr (msg);
    TRACE (DEBUG, "CallId: <%u>", call_id);
    TRACE (DEBUG, "CallFormat: <%s>", call_format);
    csmm_start_broadcast_live_call (ctx, call_id, call_format, response);
    zmsg_send (&response, reader);
    free (call_format);
  }

  if ((!command_handled) && streq (command, "STOP_CALL_INTERCEPTION")) {
    command_handled = true;
    zmsg_t *response = zmsg_new ();
    UINT32 call_id = zmsg_popint (msg);
    TRACE (DEBUG, "CallId: <%u>", call_id);
    csmm_stop_broadcast_live_call (ctx, call_id, response);
    zmsg_send (&response, reader);
  }

  if ((!command_handled) && streq (command, "GET_ACTIVE_CALLS")) {
    command_handled = true;
    zmsg_t *response = zmsg_new ();
    csmm_get_live_calls (ctx, response);
    zmsg_send (&response, reader);
  }

  if ((!command_handled) && streq (command, "START_PLAY_CALL")) {
    command_handled = true;
    zmsg_t *response = zmsg_new ();
    char *call_dbId_str = zmsg_popstr (msg);
    char *call_id_str = zmsg_popstr (msg);
    char *call_type = zmsg_popstr (msg);
    char *call_format = zmsg_popstr (msg);
    char *session = zmsg_popstr (msg);
    UINT32 call_dbId = atoi(call_dbId_str);
    UINT32 call_id = atoi(call_id_str);
    TRACE (DEBUG, "CallDbId: <%u>", call_dbId);
    TRACE (DEBUG, "CallId: <%u>", call_id);
    TRACE (DEBUG, "CallType: <%s>", call_type);
    TRACE (DEBUG, "CallFormat: <%s>", call_format);
    TRACE (DEBUG, "Session: <%s>", session);
//    csmm_start_play_call (ctx, call_type, call_id, call_dbId, call_format, response);
    csmm_start_play_call_v2 (ctx, call_type, call_id, call_dbId, call_format, session, response);
    zmsg_send (&response, reader);
    free (call_dbId_str);
    free (call_id_str);
    free (call_type);
    free (call_format);
    free (session);
  }

  if ((!command_handled) && streq (command, "STOP_PLAY_CALL")) {
    command_handled = true;
    zmsg_t *response = zmsg_new ();
    char *call_dbId_str = zmsg_popstr (msg);
    char *call_id_str = zmsg_popstr (msg);
    char *call_type = zmsg_popstr (msg);
    char *call_format = zmsg_popstr (msg);
    char *session = zmsg_popstr (msg);
    UINT32 call_dbId = atoi(call_dbId_str);
    UINT32 call_id = atoi(call_id_str);
    TRACE (DEBUG, "CallDbId: <%u>", call_dbId);
    TRACE (DEBUG, "CallId: <%u>", call_id);
    TRACE (DEBUG, "CallType: <%s>", call_type);
    TRACE (DEBUG, "CallFormat: <%s>", call_format);
    TRACE (DEBUG, "Session: <%s>", session);
//    csmm_stop_play_call (ctx, call_type, call_id, call_dbId, response);
    csmm_stop_play_call_v2 (ctx, call_type, call_id, call_dbId, call_format, session, response);
    zmsg_send (&response, reader);
    free (call_dbId_str);
    free (call_id_str);
    free (call_type);
    free (call_format);
    free (session);
  }

  if (!command_handled) {
    TRACE (ERROR, "Invalid message");
    assert (false);
  }

  free (command);
  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving csmm_command_handler");

  return result;
}


//  --------------------------------------------------------------------------
//  Callback responsible for processing a received call setup or call release
//  LogApi message.
//  Input:
//    loop: the event-driven reactor
//    reader: the descriptor with the data received ready to read
//    arg: the Call Stream Media Manager context of the thread
//  Output:
//    0: processed
//   -1: not processed

static int
csmm_voice_signaling_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  int rc;
  UINT32 msg_id;
  UINT32 call_id;
  csmm_t *ctx;
  zmsg_t* msg;
  char *tag;
  zframe_t *timestamp;
  zframe_t *log_api_msg;

  TRACE (FUNCTIONS, "Entering in csmm_voice_signaling_handler");

  ctx = (csmm_t *) arg;

  msg = zmsg_recv (reader);
  assert (msg);
//  zmsg_print (msg);

  tag = zmsg_popstr (msg);
  timestamp = zmsg_pop (msg);
  log_api_msg = zmsg_pop (msg);

  if (zframe_size (timestamp) != sizeof (time_t)) {
    TRACE (ERROR, "Timestamp: Bad format");
  }

  rc = sscanf (tag, "S_%u", &msg_id);

  if (rc != EOF && rc > 0) {
    switch (msg_id) {
    case LOG_API_DUPLEX_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_DUPLEX_CALL_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiDuplexCallChange)) {
        LogApiDuplexCallChange *duplex_call_change =
            (LogApiDuplexCallChange *) zframe_data (log_api_msg);
        if (duplex_call_change->m_uiAction == INDI_NEWCALLSETUP) {
          call_id = duplex_call_change->m_uiCallId;
          csmm_insert_live_call (ctx, call_id, 'D');
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_DUPLEX_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_DUPLEX_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiDuplexCallRelease)) {
        LogApiDuplexCallRelease *duplex_call_release =
            (LogApiDuplexCallRelease *) zframe_data (log_api_msg);
        call_id = duplex_call_release->m_uiCallId;
        csmm_remove_live_call (ctx, call_id);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SIMPLEX_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_START_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiSimplexCallStartChange)) {
        LogApiSimplexCallStartChange *simplex_call_start_change =
            (LogApiSimplexCallStartChange *) zframe_data (log_api_msg);
        if (simplex_call_start_change->m_uiAction == INDI_NEWCALLSETUP) {
          call_id = simplex_call_start_change->m_uiCallId;
          csmm_insert_live_call (ctx, call_id, 'S');
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SIMPLEX_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiSimplexCallRelease)) {
        LogApiSimplexCallRelease *simplex_call_release =
            (LogApiSimplexCallRelease *) zframe_data (log_api_msg);
        call_id = simplex_call_release->m_uiCallId;
        csmm_remove_live_call (ctx, call_id);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_START_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallStartChange)) {
        LogApiGroupCallStartChange *group_call_start_change =
            (LogApiGroupCallStartChange *) zframe_data (log_api_msg);
        if (group_call_start_change->m_uiAction == GROUPCALL_NEWCALLSETUP) {
          call_id = group_call_start_change->m_uiCallId;
          csmm_insert_live_call (ctx, call_id, 'G');
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallRelease)) {
        LogApiGroupCallRelease *group_call_release =
            (LogApiGroupCallRelease *) zframe_data (log_api_msg);
        call_id = group_call_release->m_uiCallId;
        csmm_remove_live_call (ctx, call_id);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    default:
      TRACE (DEBUG, "Message type: UNKNOWN (%x)", msg_id);
      break;
    }
  }

  free (tag);
  zframe_destroy (&timestamp);
  zframe_destroy (&log_api_msg);
  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving csmm_voice_signaling_handler");

  return 0;
}


//  --------------------------------------------------------------------------
// Traces a Call Stream Media Manager context
//  Input:
//    A Call Stream Collector context

static void
csmm_print (csmm_t *ctx)
{
  TRACE (FUNCTIONS, "Entering in csmm_print");

  TRACE (DEBUG, "---------------------------");
  TRACE (DEBUG, "Media Manager Configuration");
  TRACE (DEBUG, "---------------------------");

  TRACE (DEBUG, "  File: %s", csstring_data (ctx->conf_filename));
  TRACE (DEBUG, "  Call inactivity period (secs): %d", ctx->call_inactivity_period);
  TRACE (DEBUG, "  Maintenance frequency (secs): %d", ctx->maintenance_frequency);
  TRACE (DEBUG, "  MediaServer endpoint: %s",
      csstring_data (ctx->media_server_endpoint));
  TRACE (DEBUG, "  Player command template: %s",
      csstring_data (ctx->player_command_template));
  TRACE (DEBUG, "  Wav filename template: %s",
      csstring_data (ctx->filename_template));
  TRACE (DEBUG, "  Wav voice repo: %s",
      csstring_data (ctx->voicerec_repo));
  TRACE (DEBUG, "  Wav voice url: %s",
      csstring_data (ctx->voicerec_url));
  TRACE (DEBUG, "  DB endpoint: %s",
      csstring_data (ctx->pg_conn_info));

  // Print available live feeders
  //
  live_feeder_t *live_feeder = (live_feeder_t *) zlist_first (ctx->live_feeders);
  TRACE (DEBUG, "  ------------");
  TRACE (DEBUG, "  Live Feeders");
  TRACE (DEBUG, "  ------------");
  if (zlist_size (ctx->live_feeders)) {
    while (live_feeder) {
      TRACE (DEBUG, "    Stream: %s", csstring_data (live_feeder->stream_name));
      TRACE (DEBUG, "      ip: %s", csstring_data (live_feeder->ip));
      TRACE (DEBUG, "      port: %d", live_feeder->port);
      TRACE (DEBUG, "      channel: %d", live_feeder->channel);
      TRACE (DEBUG, "      free: %s", live_feeder->free ? "yes" : "no");
      live_feeder = (live_feeder_t *) zlist_next (ctx->live_feeders);
    }
  } else {
    TRACE (DEBUG, "    Empty");
  }

  // Print calls state
  //
  TRACE (DEBUG, "  ------------");
  TRACE (DEBUG, "  Active calls");
  TRACE (DEBUG, "  ------------");
  if (zlist_size (ctx->live_calls)) {
    live_call_t *call = (live_call_t *) zlist_first (ctx->live_calls);
    while (call) {
      TRACE (DEBUG, "    Id: %d", call->id);
      if (call->live_feeder) {
        TRACE (DEBUG, "      Feeder: %s", csstring_data (call->live_feeder->stream_name));
      } else {
        TRACE (DEBUG, "      Feeder: empty");
      }
      if (call->subscriber) {
        TRACE (DEBUG, "      Subscriber: active");
      } else {
        TRACE (DEBUG, "      Subscriber: inactive");
      }
      call = (live_call_t *) zlist_next (ctx->live_calls);
    }
  } else {
    TRACE (DEBUG, "    Empty");
  }

  // Print available call players
  //
  call_player_t *call_player = (call_player_t *) zlist_first (ctx->call_players);
  TRACE (DEBUG, "  -------");
  TRACE (DEBUG, "  Players");
  TRACE (DEBUG, "  -------");
  if (zlist_size (ctx->call_players)) {
    while (call_player) {
      TRACE (DEBUG, "    Stream: %s", csstring_data (call_player->stream_name));
      TRACE (DEBUG, "    Feeder: %s", csstring_data (call_player->feeder_name));
      TRACE (DEBUG, "      free: %s", call_player->free ? "yes" : "no");
      call_player = (call_player_t *) zlist_next (ctx->call_players);
    }
  } else {
    TRACE (DEBUG, "    Empty");
  }


  TRACE (FUNCTIONS, "Leaving csmm_print");
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

static int
csmm_maintenance_handler (zloop_t *loop, int timer_id, void *arg)
{
  TRACE (FUNCTIONS, "Entering in csmm_maintenance_handler");

  csmm_t *ctx = (csmm_t *) arg;
  live_call_t *live_call;

  time_t now = time (NULL);

  live_call = (live_call_t *) zlist_first (ctx->live_calls);

  while (live_call) {
    double inactivity = difftime (now, live_call->last_activity);
    TRACE (DEBUG, "Call <%u> had been without activity since <%.f> seconds",
        live_call->id, inactivity);
    if (inactivity > ctx->call_inactivity_period) {
      csmm_remove_live_call (ctx, live_call->id);
    }
    live_call = (live_call_t *) zlist_next (ctx->live_calls);
  }

  TRACE (FUNCTIONS, "Leaving csmm_maintenance_handler");

  return 0;
}


//  --------------------------------------------------------------------------
//  Reads the properties into a Call Stream Media Manager context from a
//  configuration file and creates all the needed resources
//  Input:
//    A Call Stream Collector context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
csmm_configure (csmm_t *ctx)
{
  int rc;
  char *string = NULL;
  char path [256];
  int x = 0;

  TRACE (FUNCTIONS, "Entering in csmm_configure");

  zconfig_t *root = zconfig_load (csstring_data (ctx->conf_filename));

  // Check potential malformed parameters
  //
  int val;
  int error, error1, error2, error3, error4, error5;
  string = zconfig_resolve (root, "/media_manager/call_inactivity_period", "0");
  val = str_to_int (string, &error1);
  string = zconfig_resolve (root, "/media_manager/maintenance_frequency", "0");
  val = str_to_int (string, &error2);
  string = zconfig_resolve (root, "/media_manager/feeders", "0");
  val = str_to_int (string, &error3);
  string = zconfig_resolve (root, "/media_manager/player/instances", "0");
  val = str_to_int (string, &error4);
  string = zconfig_resolve (root, "/media_manager/subscriptions", "0");
  val = str_to_int (string, &error5); 
  if (error1 || error2 || error3 || error4 || error5) {
    TRACE (ERROR, "Bad configuration");
    zconfig_destroy (&root);
    TRACE (FUNCTIONS, "Leaving csmm_configure");
    return -1;
  }
  string = zconfig_resolve (root, "/media_manager/feeders", "0");
  val = str_to_int (string, &error);
  for (x = 1; x <= val; x++) {
    snprintf (path, sizeof (path), "/media_manager/feeders/feeder_%d/port", x);
    string = zconfig_resolve (root, path, "0");
    val = str_to_int (string, &error);
    if (error) {
      TRACE (ERROR, "Bad configuration");
      zconfig_destroy (&root);
      TRACE (FUNCTIONS, "Leaving csmm_configure");
      return -1;
    }
  }

  // Read general configuration
  //
  string = zconfig_resolve (root, "/media_manager/media_server_endpoint", "");
  ctx->media_server_endpoint = csstring_new (string);
  string = zconfig_resolve (root, "/media_manager/player/command_template", "");
  ctx->player_command_template = csstring_new (string);
  string = zconfig_resolve (root, "/media_manager/player/filename_template", "");
  ctx->filename_template = csstring_new (string);
  string = zconfig_resolve (root, "/media_manager/player/voicerec_repo", "");
  ctx->voicerec_repo = csstring_new (string);
  string = zconfig_resolve (root, "/media_manager/player/voicerec_url", "");
  ctx->voicerec_url = csstring_new (string);
  string = zconfig_resolve (root, "/persistence_manager/pg_conn_info", "");
  ctx->pg_conn_info = csstring_new (string);

  string = zconfig_resolve (root, "/media_manager/call_inactivity_period", "300");
  ctx->call_inactivity_period = atoi (string);
  string = zconfig_resolve (root, "/media_manager/maintenance_frequency", "60");
  ctx->maintenance_frequency = atoi (string);

  rc = csmm_connect_db (ctx);

  // Read feeders' configuration
  //
  string = zconfig_resolve (root, "/media_manager/feeders", "0");
  int num_feeders = atoi (string);

  ctx->live_feeders = zlist_new ();
  assert (ctx->live_feeders);
  zlist_set_destructor (ctx->live_feeders, csmm_live_feeder_destructor);

  for (x = 1; x <= num_feeders; x++) {
    snprintf (path, sizeof (path), "/media_manager/feeders/feeder_%d/stream", x);
    string = zconfig_resolve (root, path, "");
    csstring_t *stream = csstring_new (string);
    assert (stream);

    snprintf (path, sizeof (path), "/media_manager/feeders/feeder_%d/ip", x);
    string = zconfig_resolve (root, path, "127.0.0.1");
    csstring_t *ip = csstring_new (string);
    assert (ip);

    snprintf (path, sizeof (path), "/media_manager/feeders/feeder_%d/port", x);
    string = zconfig_resolve (root, path, "4321");
    int port = atoi (string);

    snprintf (path, sizeof (path), "/media_manager/feeders/feeder_%d/type", x);
    string = zconfig_resolve (root, path, "M");

    live_feeder_t* live_feeder = csmm_live_feeder_new (stream, ip, port, string[0]);
    assert (live_feeder);

    rc = zlist_append (ctx->live_feeders, live_feeder);
  }

  // Read the call players' configuration
  //
  string = zconfig_resolve (root, "/media_manager/player/instances", "0");
  int num_players = atoi (string);

  ctx->call_players = zlist_new ();
  assert (ctx->call_players);
  zlist_set_destructor (ctx->call_players, csmm_call_player_destructor);

  for (x = 1; x <= num_players; x++) {
    snprintf (path, sizeof (path), "/media_manager/player/instances/instance_%d/stream", x);
    string = zconfig_resolve (root, path, "");
    csstring_t *stream = csstring_new (string);
    assert (stream);

    snprintf (path, sizeof (path), "/media_manager/player/instances/instance_%d/feeder", x);
    string = zconfig_resolve (root, path, "");
    csstring_t *feeder = csstring_new (string);
    assert (feeder);

    call_player_t* player = csmm_call_player_new (stream, feeder);
    assert (player);

    rc = zlist_append (ctx->call_players, player);
  }

  // Configure active calls
  //
  ctx->live_calls = zlist_new ();
  assert (ctx->live_calls);
  zlist_set_destructor (ctx->live_calls, csmm_live_call_destructor);

  // Read subscriptions configuration
  //
  ctx->subscriber = zsock_new_sub (">inproc://collector", 0);
  assert (ctx->subscriber);

  string = zconfig_resolve (root, "/media_manager/subscriptions", "0");
  int num_subscriptions = atoi (string);

  for (x = 1; x <= num_subscriptions; x++) {
    snprintf (path, sizeof (path), "/media_manager/subscriptions/subscription_%d", x);
    string = zconfig_resolve (root, path, "0");
    zsock_set_subscribe (ctx->subscriber, string);
  }

  rc = zloop_reader (ctx->loop, ctx->subscriber, csmm_voice_signaling_handler, ctx);

  string = zconfig_resolve (root, "/media_manager/command_listener_endpoint", "");
  ctx->command_listener = zsock_new_rep (string);
  rc = zloop_reader (ctx->loop, ctx->command_listener, csmm_command_handler, ctx);

  zconfig_destroy (&root);

  TRACE (FUNCTIONS, "Leaving csmm_configure");

  return 0;
}


//  --------------------------------------------------------------------------
//  Entry function to the Call Stream Media Manager submodule
//  Input:
//    pipe: the shared communication channel with the parent thread
//    arg: the configuration file with the submodule's customizable properties

void
csmm_task (zsock_t *pipe, void *args)
{
  int rc = 0;
  int id_timer = -1;
  csmm_t *ctx;
  zloop_t *loop;
  char *conf_file;

  TRACE (FUNCTIONS, "Entering in csmm_task");

  conf_file = (char *) args;

  loop = zloop_new ();
  assert (loop);
  //zloop_set_verbose (loop, true);

  ctx = csmm_new (conf_file, loop);
  assert (ctx);

  if (!csmm_configure (ctx)) {
    csmm_print (ctx);
    rc = zloop_reader (loop, pipe, csmm_command_handler, ctx);
    id_timer = zloop_timer (loop, ctx->maintenance_frequency * 1000, 0, csmm_maintenance_handler, ctx);
    zsock_signal (pipe, 0);
    if (!rc) {
      rc = zloop_start (loop);
      if (rc == 0) {
        TRACE (ERROR, "Interrupted!");
      }
      if (rc == -1) {
        TRACE (ERROR, "Cancelled!");
      }
    }
  }

  csmm_destroy (&ctx);
  zloop_reader_end (loop, pipe);
  zloop_timer_end (loop, id_timer);
  zloop_destroy (&loop);

  TRACE (FUNCTIONS, "Leaving csmm_task");
}

