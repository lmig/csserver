/*  =========================================================================
    cspm - Call Stream Persistence Manager submodule
    =========================================================================

    This submodule is responsible for saving the received LogApi message types
    into the database.

    To carry out this task, the submodule becomes a subscriber of the cscol
    publisher to receive the selected LogApi messages.

    The configuration variable mp3_mode drives the format in which the data is 
    saved:

      mp3_mode = 0 -> Data is saved in wav format
      mp3_mode = 1 -> Data is saved in mp3 format
*/


#include "cs.h"
#include "csutil.h"
#include "wave.h"
#include <libpq-fe.h>


#define MP3_CONVERTER_TAG 0x0000fade


#define CSPM_TMP_BUFFER 64
#define CSPM_BUFFER_WORK_AREA_LENGTH 2048
#define NUMBER_LENGTH 30


#ifdef __cplusplus
extern "C" {
#endif
void csmp3_task (zsock_t *pipe, void *args);
void csap_send_alarm (const char *module, const char *text);
#ifdef __cplusplus
}
#endif


// Context for a Call Stream Persistence Manager thread

struct _cspm_t {
  csstring_t *conf_filename;
  zloop_t *loop;
  zsock_t *subscriber;
  csstring_t *pg_conn_info;
  csstring_t *mp3_converter_command_template;
  PGconn *pg_conn;
  char *work_area;
  zhash_t *voice_calls_stream_a;
  zhash_t *voice_calls_stream_b;
  zhash_t *mp3_converters;
  zhash_t *voice_calls_last_activity;
  zhash_t *voice_calls_types;
  unsigned int call_inactivity_period;
  unsigned int maintenance_frequency;
  unsigned int mp3_mode;
};
typedef struct _cspm_t cspm_t;


// MP3 converter properties

struct _mp3_converter_t {
  UINT32 tag;
  UINT32 call_id;
  char call_type;
  cspm_t *ctx;
  zactor_t *executor;
};
typedef struct _mp3_converter_t mp3_converter_t;


//  --------------------------------------------------------------------------
//  Verifies if the input parameter is a mp3 converter
//

static bool
cspm_mp3_converter_is (void *self)
{
  TRACE(FUNCTIONS, "Entering in cspm_mp3_converter_is");
  assert (self);
  TRACE (FUNCTIONS, "Leaving cspm_mp3_converter_is");

  return ((mp3_converter_t *) self)->tag == MP3_CONVERTER_TAG;
}


//  --------------------------------------------------------------------------
//  Creates a mp3 converter representation
//  Input:
//  Output:
//    The created call player

static mp3_converter_t*
cspm_mp3_converter_new (UINT32 call_id, char call_type, cspm_t* ctx)
{
  mp3_converter_t *self;

  TRACE(FUNCTIONS, "Entering in cspm_mp3_converter_new");

  self = (mp3_converter_t *) zmalloc (sizeof (mp3_converter_t));
  if (self) {
    self->tag = MP3_CONVERTER_TAG;
    self->call_id = call_id;
    self->call_type = call_type;
    self->ctx = ctx;
    self->executor = NULL;
  }

  TRACE (FUNCTIONS, "Leaving cspm_mp3_converter_new");

  return self;
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in a mp3 converter
//  Input:
//    A mp3 converter

static void
cspm_mp3_converter_destroy (mp3_converter_t **self_p)
{
  TRACE (FUNCTIONS, "Entering in cspm_mp3_converter_destroy");

  assert (self_p);
  if (*self_p) {
    mp3_converter_t *self = *self_p;
    assert (cspm_mp3_converter_is (self));
    self->ctx = NULL;
    if (self->executor) {
      zactor_destroy (&self->executor);
    }
    free (self);
    *self_p = NULL;
  }

  TRACE (FUNCTIONS, "Leaving cspm_mp3_converter_destroy");
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in a mp3 converter
//  Input:
//    A mp3 converter

// TODO: Review for duplex calls

/*
static void
cspm_mp3_converter_destructor (void **item)
{
  TRACE (FUNCTIONS, "Entering in cspm_mp3_converter_destructor");
  cspm_mp3_converter_destroy ((mp3_converter_t **) item);
  TRACE (FUNCTIONS, "Leaving cspm_mp3_converter_destructor");
}
*/


//  --------------------------------------------------------------------------
//  Frees all the resources created in a mp3 converter
//  Input:
//    A mp3 converter

static void
cspm_remove_mp3_converter (void *item)
{
  TRACE (FUNCTIONS, "Entering in cspm_remove_mp3_converter");

  mp3_converter_t *obj = (mp3_converter_t *) item;
  assert (obj);
  cspm_mp3_converter_destroy (&obj);

  TRACE (FUNCTIONS, "Leaving cspm_remove_mp3_converter");
}

//  --------------------------------------------------------------------------
//  Convert seconds into hh:mm:ss format
//  Input:
//      seconds - seconds value
//  Returns: hms - formatted string (the client must free the memory)
//
char* seconds_to_time(float raw_seconds) {
  char *hms;
  int hours, hours_residue, minutes, seconds, milliseconds;
  hms = (char*) malloc(100);

  sprintf(hms, "%f", raw_seconds);

  hours = (int) raw_seconds/3600;
  hours_residue = (int) raw_seconds % 3600;
  minutes = hours_residue/60;
  seconds = hours_residue % 60;
  milliseconds = 0;

  // get the decimal part of raw_seconds to get milliseconds
  char *pos;
  pos = strchr(hms, '.');
  int ipos = (int) (pos - hms);
  char decimalpart[15];
  memset(decimalpart, ' ', sizeof(decimalpart));
  strncpy(decimalpart, &hms[ipos+1], 3);
  milliseconds = atoi(decimalpart);

  sprintf(hms, "%d:%d:%d.%d", hours, minutes, seconds, milliseconds);
  return hms;
}

//  --------------------------------------------------------------------------
//  Saves the voice call's data into the corresponding database table
//

static int
cspm_save_voice_data_helper (cspm_t *ctx, zchunk_t *data, UINT32 call_id, float duration_in_seconds)
{
  int rc = 0;
  PGresult *res;

  char call_id_str[CSPM_TMP_BUFFER];
  char call_table[CSPM_TMP_BUFFER];
  char voice_table[CSPM_TMP_BUFFER];
  char db_id[CSPM_TMP_BUFFER];
  char call_begin[CSPM_TMP_BUFFER];
  char call_end[CSPM_TMP_BUFFER];
  char call_len[CSPM_TMP_BUFFER];
  char *command = NULL;
  char *call_type = NULL;

  TRACE (FUNCTIONS, "Entering in cspm_save_voice_data_helper");

  snprintf(call_id_str, CSPM_TMP_BUFFER, "%u", call_id);
  call_type = zhash_lookup (ctx->voice_calls_types, call_id_str);

  if (*call_type == 'G') {
    strncpy (call_table, "d_callstream_groupcall", CSPM_TMP_BUFFER);
    strncpy (voice_table, "d_callstream_voicegroupcall", CSPM_TMP_BUFFER);
  }

  if (*call_type == 'D' || *call_type == 'S') {
    strncpy (call_table, "d_callstream_indicall", CSPM_TMP_BUFFER);
    strncpy (voice_table, "d_callstream_voiceindicall", CSPM_TMP_BUFFER);
  }

  TRACE (DEBUG, "Call type: %c", *call_type);
  TRACE (DEBUG, "Call Id: %u", call_id);
  TRACE (DEBUG, "Call Table: %s", call_table);
  TRACE (DEBUG, "Voice Table: %s", voice_table);

  command = "SELECT db_id,call_begin,call_end "
      "FROM %s "
      "WHERE call_id = %u "
      "ORDER BY call_begin DESC LIMIT 1";
  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      call_table, call_id);

  TRACE (DEBUG, "Executing <%s>", ctx->work_area);

  res = PQexec (ctx->pg_conn, ctx->work_area);

  if (res && PQresultStatus (res) == PGRES_TUPLES_OK) {
    int affected = PQntuples (res);
    TRACE (DEBUG, "Rows affected <%d>", affected);
    if (affected == 1) {
      strncpy (db_id, PQgetvalue (res, 0, 0), CSPM_TMP_BUFFER);
      strncpy (call_begin, PQgetvalue (res, 0, 1), CSPM_TMP_BUFFER);
      strncpy (call_end, PQgetvalue (res, 0, 2), CSPM_TMP_BUFFER);
    } else {
      rc = -1;
    }
  } else {
    TRACE (ERROR, "SELECT failed: %s\n", PQerrorMessage (ctx->pg_conn));
    rc = -1;
  }

  PQclear (res);

  if (!rc) {

    char* duration_in_hhmmss_format = seconds_to_time(duration_in_seconds);

    const char *paramValues[6];
    int paramLengths[6];
    int paramFormats[6];

    paramValues[0] = db_id;
    paramLengths[0] = strlen (db_id);
    paramFormats[0] = 0;

    paramValues[1] = call_begin;
    paramLengths[1] = strlen (call_begin);
    paramFormats[1] = 0;

    paramValues[2] = call_end;
    paramLengths[2] = strlen (call_end);
    paramFormats[2] = 0;

    snprintf (call_len, CSPM_TMP_BUFFER, "%zu", zchunk_size (data));
    paramValues[3] = call_len;
    paramLengths[3] = strlen (call_len);
    paramFormats[3] = 0;

    paramValues[4] = (const char *) zchunk_data (data);
    paramLengths[4] = zchunk_size (data);
    paramFormats[4] = 1;

    paramValues[5] = duration_in_hhmmss_format;
    paramLengths[5] = strlen (duration_in_hhmmss_format);
    paramFormats[5] = 0;


    // Timestamp format 2015-12-09 15:23:42+04
    command = "INSERT INTO %s"
        "(db_id, call_begin, call_end, voice_data_len, voice_data, duration) "
        "VALUES ($1::bigint, to_timestamp($2,'YYYY-MM-DD HH24:MI:SS'), to_timestamp($3,'YYYY-MM-DD HH24:MI:SS'), $4::bigint, $5::bytea, $6::interval)";
    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command, voice_table);

    TRACE (DEBUG, "Executing <%s>", ctx->work_area);

    res = PQexecParams (ctx->pg_conn,
        ctx->work_area,
        6,
        NULL,
        paramValues,
        paramLengths,
        paramFormats,
        1);

    TRACE (DEBUG, "Result: %s\n", PQresStatus (PQresultStatus(res)));

    free (duration_in_hhmmss_format);

    if (res && PQresultStatus(res) == PGRES_COMMAND_OK) {
      int affected = PQntuples (res);
      TRACE (DEBUG, "Rows affected <%d>", affected);
    } else {
      TRACE (ERROR, "INSERT failed: %s\n", PQerrorMessage (ctx->pg_conn));
      csap_send_alarm ("CSPM", "Unable to record voice call");
      rc = -1;
    }

    PQclear(res);
  }

  TRACE (FUNCTIONS, "Leaving cspm_save_voice_data_helper");

  return rc;
}

//  --------------------------------------------------------------------------
//  Callback handler. Process the finalization signal of a child mp3 converter
//  Input:
//    loop: the reactor
//    reader: the shared channel established with a child mp3 converter
//    arg: the mp3 converter context
//  Output:
//    0 - Ok
//    1 - Nok

static int
cspm_mp3_converter_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  bool command_handled = false;
  int result = 0;
  float duration_in_seconds = 0;

  TRACE (FUNCTIONS, "Entering in cspm_mp3_converter_handler");

  zmsg_t *msg = zmsg_recv (reader);
  zmsg_print (msg);

  if (!msg)
    result = 1;

  char *command = zmsg_popstr (msg);
  TRACE (DEBUG, "Command: %s", command);

  if ((!command_handled) && streq (command, "MP3_CONVERTER_FINISHED")) {
    int rc = 0;
    char wav_file[PATH_MAX];
    char mp3_file[PATH_MAX];
    mp3_converter_t *mp3_converter = (mp3_converter_t *) arg;
    char call_id_str[CSPM_TMP_BUFFER];
    snprintf (call_id_str, CSPM_TMP_BUFFER, "%u", mp3_converter->call_id);
    snprintf (wav_file, sizeof (wav_file), "/tmp/voice_%s.wav", call_id_str);
    snprintf (mp3_file, sizeof (mp3_file), "/tmp/voice_%s.mp3", call_id_str);
    assert (cspm_mp3_converter_is (mp3_converter));
    command_handled = true;
    zchunk_t *voice_data = zchunk_slurp (mp3_file, 0);
    rc = cspm_save_voice_data_helper (mp3_converter->ctx, voice_data, mp3_converter->call_id, duration_in_seconds);
    if (mp3_converter->executor) {
      zactor_destroy (&mp3_converter->executor);
    }
    zloop_reader_end(loop, reader);
    zhash_delete (mp3_converter->ctx->mp3_converters, call_id_str);
    zchunk_destroy (&voice_data);
    unlink (wav_file);
    unlink (mp3_file);
    TRACE (DEBUG, "MP3 converter released");
  }

  if (!command_handled) {
    TRACE (ERROR, "Invalid message");
    //assert (false);
  }

  free (command);
  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving cspm_mp3_converter_handler");

  return result;
}


//  --------------------------------------------------------------------------
//  Frees all the resources created for a voice call
//  Input:
//    A voice call

static void
cspm_remove_call (void *item)
{
  TRACE (FUNCTIONS, "Entering in cspm_remove_call");

  zlist_t *obj = (zlist_t *) item;
  assert (obj);
  zlist_destroy (&obj);

  TRACE (FUNCTIONS, "Leaving cspm_remove_call");
}


//  --------------------------------------------------------------------------
//  Frees all the resources created for a data voice chunk
//  Input:
//    A data voice chunk

static void
cspm_remove_voice_block (void *item)
{
  TRACE (FUNCTIONS, "Entering in cspm_remove_voice_block");

  zchunk_t *obj = (zchunk_t *) item;
  assert (obj);
  zchunk_destroy (&obj);

  TRACE (FUNCTIONS, "Leaving cspm_remove_voice_block");
}


//  --------------------------------------------------------------------------
//  Frees all the resources created for monitoring a call's activity
//  Input:
//    A call's activity entry

static void
cspm_remove_call_activity (void *item)
{
  TRACE (FUNCTIONS, "Entering in cspm_remove_call_activity");

  time_t *obj = (time_t *) item;
  free (obj);

  TRACE (FUNCTIONS, "Leaving cspm_remove_call_activity");
}


//  --------------------------------------------------------------------------
//  Frees all the resources created for store a call's type
//  Input:
//    A call's activity entry

static void
cspm_remove_call_type (void *item)
{
  TRACE (FUNCTIONS, "Entering in cspm_remove_call_type");

  char *obj = (char *) item;
  free (obj);

  TRACE (FUNCTIONS, "Leaving cspm_remove_call_type");
}


//  --------------------------------------------------------------------------
//  Establish a connection with the database
//  Input:
//    The target Call Stream Persistence Manager context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_connect_db (cspm_t *ctx)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in cspm_connect_db");

  ctx->pg_conn = PQconnectdb (csstring_data (ctx->pg_conn_info));

  if (PQstatus (ctx->pg_conn) != CONNECTION_OK) {
    TRACE(ERROR, "error: connection to database '%s' failed.", PQdb (ctx->pg_conn));
    TRACE(ERROR, "%s\n", PQerrorMessage (ctx->pg_conn));
    rc = -1;
  }

  TRACE (FUNCTIONS, "Leaving cspm_connect_db");

  return rc;
}


//  --------------------------------------------------------------------------
//  Releases a connection with the database
//  Input:
//    The target Call Stream Persistence Manager context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_disconnect_db (cspm_t *ctx)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in cspm_disconnect_db");

  if (ctx->pg_conn != NULL) {
    PQfinish (ctx->pg_conn);
    ctx->pg_conn = NULL;
  }

  TRACE (FUNCTIONS, "Leaving cspm_disconnect_db");

  return rc;
}


//  --------------------------------------------------------------------------
//  Creates a thread specific Call Stream Persistence Manager context
//  Input:
//    conf_file: The configuration file
//    loop: the event-reactor used in the thread
//  Output:
//    The context created according to the configuration file

static cspm_t*
cspm_new (const char * const conf_file, zloop_t *loop)
{
  TRACE (FUNCTIONS, "Entering in cspm_new");

  cspm_t *self = (cspm_t *) zmalloc (sizeof (cspm_t));
  if (self) {
    self->conf_filename = csstring_new (conf_file);
    self->loop = loop;
    self->pg_conn_info = NULL;
    self->mp3_converter_command_template = NULL;
    self->pg_conn = NULL;
    self->work_area = (char *) zmalloc (
        CSPM_BUFFER_WORK_AREA_LENGTH * sizeof (char));
    self->voice_calls_stream_a = zhash_new ();
    self->voice_calls_stream_b = zhash_new ();
    self->mp3_converters = zhash_new ();
    self->voice_calls_last_activity = zhash_new ();
    self->voice_calls_types = zhash_new ();
  }

  TRACE (FUNCTIONS, "Leaving cspm_new");

  return self;
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in the thread specific Call Stream Persistence
//  Manager context
//  Input:
//    The target Call Stream Persistence Manager context

static void
cspm_destroy (cspm_t **self_p)
{
  TRACE (FUNCTIONS, "Entering in cspm_destroy");

  assert (self_p);
  if (*self_p) {
    cspm_t *self = *self_p;
    csstring_destroy (&self->conf_filename);
    csstring_destroy (&self->pg_conn_info);
    csstring_destroy (&self->mp3_converter_command_template);
    free (self->work_area);
    cspm_disconnect_db (self);
    zhash_destroy (&self->voice_calls_stream_a);
    zhash_destroy (&self->voice_calls_stream_b);
    zhash_destroy (&self->mp3_converters);
    zhash_destroy (&self->voice_calls_last_activity);
    zhash_destroy (&self->voice_calls_types);
    if (self->subscriber) {
      zloop_reader_end (self->loop, self->subscriber);
      zsock_destroy (&(self->subscriber));
    }
    free (self);
    *self_p = NULL;
  }

  TRACE (FUNCTIONS, "Entering in cspm_destroy");
}


//  --------------------------------------------------------------------------
// Traces a Call Stream Persistence Manager context
//  Input:
//    A Call Stream Persistence Manager context

static void
cspm_print (cspm_t *ctx)
{
  TRACE (FUNCTIONS, "Entering in cspm_print");

  TRACE (DEBUG, "---------------------------------");
  TRACE (DEBUG, "Persistence Manager Configuration");
  TRACE (DEBUG, "---------------------------------");
  TRACE (DEBUG, "  File: %s", 
      csstring_data (ctx->conf_filename));
  TRACE (DEBUG, "  DB endpoint: %s", 
      csstring_data (ctx->pg_conn_info));
  TRACE (DEBUG, "  MP3 mode: %d", 
      ctx->mp3_mode);
  TRACE (DEBUG, "  MP3 converter command template: %s", 
      csstring_data (ctx->mp3_converter_command_template));
  TRACE (DEBUG, "  Call inactivity period (secs): %d", 
      ctx->call_inactivity_period);
  TRACE (DEBUG, "  Maintenance frequency (secs): %d", 
      ctx->maintenance_frequency);
  TRACE (DEBUG, "  MP3 mode: %d", 
      ctx->mp3_mode);

  TRACE (FUNCTIONS, "Leaving cspm_print");
}


//  --------------------------------------------------------------------------
//  Callback handler. Analyzes and process commands sent by the parent thread
//  through the shared pipe.
//  Input:
//    loop: the reactor
//    reader: the parent thread endpoint
//    arg: the Call Stream Persistence Manager context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_command_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  bool command_handled = false;
  int result = 0;

  TRACE (FUNCTIONS, "Entering in cspm_command_handler");

  zmsg_t *msg = zmsg_recv (reader);
  //zmsg_print (msg);

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

  TRACE (FUNCTIONS, "Leaving cspm_command_handler");

  return result;
}


//  --------------------------------------------------------------------------
// Executes a database query
//  Input:
//    ctx: the Call Stream Persistence Manager context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_execute_db_query (cspm_t *ctx)
{
  int rc = 0;
  PGresult *res;

  TRACE (FUNCTIONS, "Entering in cspm_execute_db_query");

  TRACE (DEBUG, "Query: %s", ctx->work_area);

  res = PQexec (ctx->pg_conn, ctx->work_area);

  if (PQresultStatus (res) != PGRES_TUPLES_OK) {
    TRACE (DEBUG, "Execute code <%d>", PQresultStatus (res));
    TRACE (DEBUG, "Execute text <%s>", PQresultErrorMessage (res));
    rc = -1;
  } else {
    rc = PQntuples(res);
  }

  PQclear (res);

  TRACE (FUNCTIONS, "Leaving cspm_execute_db_query. %d rows affected.", rc);

  return rc;
}


//  --------------------------------------------------------------------------
// Executes a database command
//  Input:
//    ctx: the Call Stream Persistence Manager context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_execute_db_command (cspm_t *ctx)
{
  int rc = 0;
  PGresult *res;

  TRACE (FUNCTIONS, "Entering in cspm_execute_db_command");

  TRACE (DEBUG, "Command: %s", ctx->work_area);

  res = PQexec (ctx->pg_conn, ctx->work_area);

  if (PQresultStatus (res) != PGRES_COMMAND_OK) {
    TRACE (DEBUG, "Execute code <%d>", PQresultStatus (res));
    TRACE (DEBUG, "Execute text <%s>", PQresultErrorMessage (res));
    csap_send_alarm ("CSPM", "Unable to record voice call");
    rc = -1;
  } else {
    char *affected = PQcmdTuples (res);
    rc = atoi (affected);
  }

  PQclear (res);

  TRACE (FUNCTIONS, "Leaving cspm_execute_db_command. %d rows affected.", rc);

  return rc;
}


//  --------------------------------------------------------------------------
// Initializes the memory in order to store a call's voice data block
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    call_id: the call's identifier
//    data: the data block
//    len: the data block's length
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_init_cache_voice_data (cspm_t *ctx, UINT32 call_id, char call_type)
{
  int rc = 0;
  char call_id_str[CSPM_TMP_BUFFER];

  TRACE (FUNCTIONS, "Entering in cspm_init_cache_voice_data");

  snprintf(call_id_str, CSPM_TMP_BUFFER, "%u", call_id);

  // Creates the list of voice data chunks for stream A
  //
  zlist_t *voice_blocks_stream_a = zlist_new ();
  assert (voice_blocks_stream_a);
  rc = zhash_insert (ctx->voice_calls_stream_a, call_id_str, voice_blocks_stream_a);

  // If the call is a duplex one, creates the list of voice data chunks for stream B
  //
  if (rc != -1 && call_type == 'D') {
    zlist_t *voice_blocks_stream_b = zlist_new ();
    assert (voice_blocks_stream_b);
    rc = zhash_insert (ctx->voice_calls_stream_b, call_id_str, voice_blocks_stream_b);
  }

  if (rc != -1) {

    // Binds the destructor handler of the list of voice data chunks for stream A
    //
    zhash_freefn (ctx->voice_calls_stream_a, call_id_str, cspm_remove_call);

    // If the call is a duplex one, binds the destructor of the list of voice data chunks for stream B
    //
    if (call_type == 'D') {
      zhash_freefn (ctx->voice_calls_stream_b, call_id_str, cspm_remove_call);
    }

    // Creates the entry to store the call's last activity
    //
    time_t *aux_time = malloc (sizeof (time_t));
    *aux_time = time (NULL);
    rc = zhash_insert (ctx->voice_calls_last_activity, call_id_str, aux_time);

    if (rc != -1) {

      // Binds the desctructor handler of the last activity entry
      //
      zhash_freefn (ctx->voice_calls_last_activity, call_id_str, cspm_remove_call_activity);

      // Creates the entry to store the call type
      //
      char *aux_char = malloc (sizeof (char));
      *aux_char = call_type;
      rc = zhash_insert (ctx->voice_calls_types, call_id_str, aux_char);

      if (rc != 1) {

        // Binds the desctructor handler of the call type register
        //
        zhash_freefn (ctx->voice_calls_types, call_id_str, cspm_remove_call_type);

        if (ctx->mp3_mode) {

          // If the system is handling mp3 format, creates and registers the wav->mp3 converter
          //
          mp3_converter_t *mp3_converter = cspm_mp3_converter_new (call_id, call_type, ctx);
          rc = zhash_insert (ctx->mp3_converters, call_id_str, mp3_converter);

          if (rc != 1) {

            // Binds the desctructor handler of the wav->mp3 call's converter
            //
            zhash_freefn (ctx->mp3_converters, call_id_str, cspm_remove_mp3_converter);

          } else {
            TRACE (ERROR, "Unable to register mp3 converter for call <%u>", call_id);
          }
        }

      } else {
        TRACE (ERROR, "Unable to register call type for call <%u>", call_id);
      }
    } else {
      TRACE (ERROR, "Unable to register call activity for call <%u>", call_id);
    }
  } else {
    TRACE (ERROR, "Unable to create voice data store for call <%u>", call_id);
  }

  TRACE (FUNCTIONS, "Leaving cspm_init_cache_voice_data");

  return rc;
}


//  --------------------------------------------------------------------------
// Stores a call's voice data block
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    call_id: the call's identifier
//    data: the data block
//    len: the data block's length
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_cache_voice_data (cspm_t *ctx, UINT32 call_id, StreamOriginatorEnum originator, const UINT8 * const data, size_t len)
{
  int rc = 0;
  char call_id_str[CSPM_TMP_BUFFER];

  TRACE (FUNCTIONS, "Entering in cspm_cache_voice_data");

  snprintf(call_id_str, CSPM_TMP_BUFFER, "%u", call_id);

  // Fetch the call type
  //
  char *call_type = zhash_lookup (ctx->voice_calls_types, call_id_str);

  // Fetch the corresponding list of voice chunks
  //
  zlist_t *voice_blocks = (zlist_t *) zhash_lookup (ctx->voice_calls_stream_a, call_id_str);
  if (*call_type == 'D' && originator == STREAM_ORG_B_SUB) {
    voice_blocks = (zlist_t *) zhash_lookup (ctx->voice_calls_stream_b, call_id_str);
  }

  // Store the voice block received
  //
  if (voice_blocks) {
    zchunk_t *voice_block = zchunk_new (data, len);
    assert (voice_block);
    rc = zlist_append (voice_blocks, voice_block);
    if (rc == -1) {
      TRACE (ERROR, "Unable to store voice block for call <%u>", call_id);
    }
    zlist_freefn (voice_blocks, voice_block, cspm_remove_voice_block, false);
  } else {
    TRACE (ERROR, "Protocol error. Call <%u> received without previous CALLSETUP", call_id);
  }

  // Update last activity information
  //
  time_t *last_activity = (time_t *) zhash_lookup (ctx->voice_calls_last_activity, call_id_str);
  if (last_activity) {
    *last_activity = time (NULL);
  } else {
    TRACE (ERROR, "Last activity for call <%u> not registered", call_id);
  }

  TRACE (FUNCTIONS, "Leaving cspm_cache_voice_data");

  return rc;
}

//
//
//
static void fill_wav_header (WaveHeader *wave_header, char call_type, UINT32 dataSize, float *duration_in_seconds)
{
  TRACE (FUNCTIONS, "Entering in fill_wav_header");
  wave_header->riffId[0] = 'R';
  wave_header->riffId[1] = 'I';
  wave_header->riffId[2] = 'F';
  wave_header->riffId[3] = 'F';
  wave_header->riffSize = 4 + 26 + 12 + 8 + dataSize;
  wave_header->waveId[0] = 'W';
  wave_header->waveId[1] = 'A';
  wave_header->waveId[2] = 'V';
  wave_header->waveId[3] = 'E';
  wave_header->fmtId[0] = 'f';
  wave_header->fmtId[1] = 'm';
  wave_header->fmtId[2] = 't';
  wave_header->fmtId[3] = ' ';
  wave_header->fmtSize = 18;
  wave_header->wFormatTag = 6; /* A-law */
  wave_header->nSamplesPerSec = 8000;
  if (call_type == 'D') {
    wave_header->nChannels = 2;
    wave_header->nAvgBytesperSec = 16000;
    wave_header->nBlockAlign = 2;
  } else {
    wave_header->nChannels = 1;
    wave_header->nAvgBytesperSec = 8000;
    wave_header->nBlockAlign = 1;
  }
  wave_header->wBitsPerSample = 8;
  wave_header->cbSize = 0;
  wave_header->factId[0] = 'f';
  wave_header->factId[1] = 'a';
  wave_header->factId[2] = 'c';
  wave_header->factId[3] = 't';
  wave_header->factSize = 4;
  wave_header->dwSampleLength = dataSize;
  wave_header->dataId[0] = 'd';
  wave_header->dataId[1] = 'a';
  wave_header->dataId[2] = 't';
  wave_header->dataId[3] = 'a';
  wave_header->dataSize = dataSize;

  float byterate = wave_header->nSamplesPerSec *  wave_header->nChannels *  wave_header->wBitsPerSample/8;
  float overall_size = wave_header->riffSize;
  *duration_in_seconds = overall_size/byterate;

  TRACE (FUNCTIONS, "Leaving fill_wav_header");
}



//
//
//
static int cspm_copy_voice_data_to_wav_file (const char *path, char call_type, 
       const char *data, UINT32 size)
{
  WaveHeader wave_header;
  struct stat file_stat;
  int rc = 0;
  FILE *fp = NULL;
  float duration_in_seconds = 0;

  TRACE (FUNCTIONS, "Entering in cspm_copy_voice_data_to_wav_file");

  if (stat (path, &file_stat) == 0) {
    unlink (path);
  }

  fill_wav_header (&wave_header, call_type, size, &duration_in_seconds);

  TRACE (DEBUG, "WAVE header size: <%d>", sizeof (wave_header));
  TRACE (DEBUG, "WAVE file: <%s>", path);

  if ((fp = fopen (path, "wb")) == NULL) {
    TRACE (ERROR, "Error: fopen (), errno = %d text = %s", errno, strerror (errno));
    rc = -1;
  }
  if ((rc == 0) && (fwrite (&wave_header, 1, sizeof (wave_header), fp) != sizeof (wave_header))) {
    TRACE (ERROR, "Error: fwrite (), errno = %d text = %s", errno, strerror (errno));
    rc = -1;
  }
  if ((rc == 0) && (fwrite (data, 1, size, fp) != size)) {
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

  TRACE (FUNCTIONS, "Leaving cspm_copy_db_voice_call_to_wav_file");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves a call's voice data
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    call_id: the call's identifier
//  Output:
//    0 - Ok
//   -1 - Nok

// TODO: Prepend the wav header

static int
cspm_save_voice_data (cspm_t *ctx, UINT32 call_id)
{
  int rc = 0;
  char call_id_str[CSPM_TMP_BUFFER];
  uint32_t voice_data_len = 0;
  zchunk_t *voice_data = NULL;
  WaveHeader wave_header;
  float duration_in_seconds = 0;
  int chunks_in_stream_a = 0;
  int chunks_in_stream_b = 0;

  TRACE (FUNCTIONS, "Entering in cspm_save_voice_data");

  snprintf(call_id_str, CSPM_TMP_BUFFER, "%u", call_id);

  char *call_type = zhash_lookup (ctx->voice_calls_types, call_id_str);

  // Retrieves the list of voice data chunks
  //
  zlist_t *voice_blocks_stream_a = (zlist_t *) zhash_lookup (ctx->voice_calls_stream_a, call_id_str);
  if (voice_blocks_stream_a) {
    chunks_in_stream_a = zlist_size(voice_blocks_stream_a);
    TRACE (DEBUG, "Voice data chunks in stream A: <%d>", chunks_in_stream_a);
  }
  zlist_t *voice_blocks_stream_b = NULL;
  if (*call_type == 'D') {
    voice_blocks_stream_b = (zlist_t *) zhash_lookup (ctx->voice_calls_stream_b, call_id_str);
    if (voice_blocks_stream_b) {
      chunks_in_stream_b = zlist_size(voice_blocks_stream_b);
      TRACE (DEBUG, "Voice data chunks in stream B: <%d>", chunks_in_stream_b);
    }
  }

  if (*call_type == 'D') {
    if (chunks_in_stream_a != chunks_in_stream_b) {
      TRACE (WARNING, "Chunks without counterpart will be discarded");
    }
  }

  if (voice_blocks_stream_a) {

    // Calculates the voice call's size
    //
    if (*call_type == 'D') {
      zchunk_t *block_stream_a = (zchunk_t *) zlist_first (voice_blocks_stream_a);
      zchunk_t *block_stream_b = (zchunk_t *) zlist_first (voice_blocks_stream_b);
      while (block_stream_b && block_stream_a) {
        voice_data_len += zchunk_size (block_stream_a);
        voice_data_len += zchunk_size (block_stream_b);
        block_stream_a = (zchunk_t *) zlist_next (voice_blocks_stream_a);
        block_stream_b = (zchunk_t *) zlist_next (voice_blocks_stream_b);
      }
    } else {
      zchunk_t *block_stream_a = (zchunk_t *) zlist_first (voice_blocks_stream_a);
      while (block_stream_a) {
        voice_data_len += zchunk_size (block_stream_a);
        block_stream_a = (zchunk_t *) zlist_next (voice_blocks_stream_a);
      }
    }

    TRACE (DEBUG, "Call Id: <%u>. Voice data length: <%d>", call_id, voice_data_len);

    // Reserves memory for the voice call
    //
    voice_data = zchunk_new (NULL, sizeof(wave_header) + voice_data_len);
  
    // Adds the wave header
    //
    fill_wav_header (&wave_header, *call_type, voice_data_len, &duration_in_seconds);
    zchunk_append (voice_data, &wave_header, sizeof(wave_header));

    // Concatenates the voice call chunks
    //
    zchunk_t *block_stream_a = (zchunk_t *) zlist_first (voice_blocks_stream_a);
    if (*call_type == 'D') {
      zchunk_t *block_stream_b = (zchunk_t *) zlist_first (voice_blocks_stream_b);
      while (block_stream_b && block_stream_a) {
        int block_size = zchunk_size (block_stream_b);
        int i = 0;

        // Merge stream A with stream B
        //
        for (i = 0; i < block_size; i++) {
          zchunk_append (voice_data, zchunk_data (block_stream_a) + i, 1);
          zchunk_append (voice_data, zchunk_data (block_stream_b) + i, 1);
        }

        block_stream_a = (zchunk_t *) zlist_next (voice_blocks_stream_a);
        block_stream_b = (zchunk_t *) zlist_next (voice_blocks_stream_b);
      }
    } else {
      while (block_stream_a) {
        zchunk_append (voice_data, zchunk_data (block_stream_a), zchunk_size (block_stream_a));
        block_stream_a = (zchunk_t *) zlist_next (voice_blocks_stream_a);
      }
    }

    // Saves the voice data call in database
    //
    rc = cspm_save_voice_data_helper (ctx, voice_data, call_id, duration_in_seconds);

    // Frees all the working area memory reserved for the call
    //
    zhash_delete (ctx->voice_calls_stream_a, call_id_str);
    if (*call_type == 'D') {
      zhash_delete (ctx->voice_calls_stream_b, call_id_str);
    }
    zhash_delete (ctx->voice_calls_last_activity, call_id_str);
    zhash_delete (ctx->voice_calls_types, call_id_str);
    zchunk_destroy (&voice_data);
  } else {
    TRACE (ERROR, "No voice data found for call %u", call_id);
  }

  TRACE (FUNCTIONS, "Leaving cspm_save_voice_data");

  return rc;
}

// --------------------------------------------------------------------------
// Saves a call's voice data in mp3 format
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    call_id: the call's identifier
//  Output:
//    0 - Ok
//   -1 - Nok

// TODO: Review for duplex calls

static int
cspm_save_voice_data_as_mp3 (cspm_t *ctx, UINT32 call_id)
{
  int rc = 0;
  char call_id_str[CSPM_TMP_BUFFER];
  uint32_t voice_data_len = 0;
  zchunk_t *voice_data = NULL;

  TRACE (FUNCTIONS, "Entering in cspm_save_voice_data_as_mp3");

  snprintf(call_id_str, CSPM_TMP_BUFFER, "%u", call_id);
  char *call_type = zhash_lookup (ctx->voice_calls_types, call_id_str);

  zlist_t *voice_blocks_a = (zlist_t *) zhash_lookup (ctx->voice_calls_stream_a, call_id_str);

  if (voice_blocks_a) {
    zchunk_t *block = (zchunk_t *) zlist_first (voice_blocks_a);
    while (block) {
      voice_data_len += zchunk_size (block);
      block = (zchunk_t *) zlist_next (voice_blocks_a);
    }
    voice_data = zchunk_new (NULL, voice_data_len);
    block = (zchunk_t *) zlist_first (voice_blocks_a);
    while (block) {
      zchunk_append (voice_data, zchunk_data (block), zchunk_size (block));
      block = (zchunk_t *) zlist_next (voice_blocks_a);
    }
    
    char wav_file[PATH_MAX];
    snprintf (wav_file, sizeof (wav_file), "/tmp/voice_%s.wav", call_id_str);
    rc = cspm_copy_voice_data_to_wav_file (wav_file, *call_type,
              (const char *) zchunk_data (voice_data), voice_data_len);
    zhash_delete (ctx->voice_calls_stream_a, call_id_str);
    zhash_delete (ctx->voice_calls_last_activity, call_id_str);
    zhash_delete (ctx->voice_calls_types, call_id_str);
    zchunk_destroy (&voice_data);

    if (rc == 0) {
      // Execute the mp3 converter command
      //
      char mp3_file[PATH_MAX];
      char log_name[PATH_MAX];
      char mp3_converter_command[PATH_MAX];
      snprintf (mp3_file, sizeof (mp3_file), "/tmp/voice_%s.mp3", call_id_str);
      snprintf (log_name, sizeof (log_name), "voice_%s", call_id_str);
      mp3_converter_t *mp3_converter = (mp3_converter_t *) zhash_lookup (ctx->mp3_converters, call_id_str);
      snprintf (mp3_converter_command, sizeof (mp3_converter_command),
        csstring_data (ctx->mp3_converter_command_template),
        wav_file,
        mp3_file,
        log_name);
      mp3_converter->executor = zactor_new (csmp3_task, mp3_converter_command);
      assert (mp3_converter->executor);

      zloop_reader (ctx->loop, (zsock_t *) mp3_converter->executor,
          cspm_mp3_converter_handler, mp3_converter);
    }
  } else {
    TRACE (ERROR, "No voice data found for call %u", call_id);
  }

  TRACE (FUNCTIONS, "Leaving cspm_save_voice_data_as_mp3");

  return rc;
}

//  --------------------------------------------------------------------------
// Saves the data of a LogApiKeepAlive message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiKeepAlive message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_keep_alive (cspm_t *ctx, const time_t * const timestamp,
    const LogApiKeepAlive * const keep_alive)
{
  int rc;
  char *command;

  BYTE swVer[sizeof (keep_alive->m_bySwVer) + 1];
  BYTE swVerString[sizeof (keep_alive->m_bySwVerString) + 1];
  BYTE logServerDescr[sizeof (keep_alive->m_byLogServerDescr) + 1];

  cs_buffer_to_string (keep_alive->m_bySwVer, sizeof (swVer), swVer);
  cs_buffer_to_string (keep_alive->m_bySwVerString, sizeof (swVerString), swVerString);
  cs_buffer_to_string (keep_alive->m_byLogServerDescr, sizeof (logServerDescr), logServerDescr);

  TRACE (FUNCTIONS, "Entering in cspm_save_keep_alive");

  command = "SELECT log_server_no "
      "FROM d_callstream_keepalive "
      "WHERE log_server_no = %d";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      keep_alive->m_uiLogServerNo);

  rc = cspm_execute_db_query (ctx);

  if (rc == 1) {
    command = "UPDATE d_callstream_keepalive "
        "SET (last_heartbeat, timeout, sw_ver, sw_ver_string, log_server_descr) = "
        "(to_timestamp(%u), %d, '%s', '%s', '%s') "
        "WHERE log_server_no = %d";

    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
        *timestamp,
        keep_alive->m_uiTimeout,
        swVer,
        swVerString,
        logServerDescr,
        keep_alive->m_uiLogServerNo);

    rc = cspm_execute_db_command (ctx);
  }

  if (rc == 0) {
    command = "INSERT INTO d_callstream_keepalive "
        "(log_server_no, last_heartbeat, timeout, sw_ver, sw_ver_string, log_server_descr) "
        "VALUES (%d, to_timestamp(%u), %d, '%s', '%s', '%s')";

    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
        keep_alive->m_uiLogServerNo,
        *timestamp,
        keep_alive->m_uiTimeout,
        swVer,
        swVerString,
        logServerDescr);

    rc = cspm_execute_db_command (ctx);
  }

  TRACE (FUNCTIONS, "Leaving cspm_save_keep_alive");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiDuplexCallChange message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiDuplexCallChange message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_duplex_call_change (cspm_t *ctx, const time_t * const timestamp,
    const LogApiDuplexCallChange * const duplex_call_change)
{
  int rc;
  char *command;

  BYTE descrA[sizeof (duplex_call_change->m_A_Descr) + 1];
  BYTE descrB[sizeof (duplex_call_change->m_B_Descr) + 1];
  BYTE digitsA[NUMBER_LENGTH];
  BYTE digitsB[NUMBER_LENGTH];

  TRACE (FUNCTIONS, "Entering in cspm_save_duplex_call_change");

  cs_buffer_to_string (duplex_call_change->m_A_Descr,
      sizeof (descrA), descrA);
  cs_buffer_to_string (duplex_call_change->m_B_Descr,
      sizeof (descrB), descrB);

  cs_number_to_string (&(duplex_call_change->m_A_Number), digitsA);
  cs_number_to_string (&(duplex_call_change->m_B_Number), digitsB);

  if (duplex_call_change->m_uiAction == INDI_NEWCALLSETUP) {

    TRACE (DEBUG, "Begin call. Call Id: <%u>", duplex_call_change->m_uiCallId);

    command = "INSERT INTO d_callstream_indicall "
        "(call_id, timeout, call_begin, seq_no_begin, "
        "calling_ssi, calling_mnc, calling_mcc, calling_esn, calling_descr, "
        "called_ssi, called_mnc, called_mcc, called_esn, called_descr, "
        "simplex_duplex) "
        "VALUES (%u,%d,to_timestamp(%u),%u,%u,%d,%d,'%s','%s',%u,%d,%d,'%s','%s',1)";

    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
        duplex_call_change->m_uiCallId,
        duplex_call_change->m_uiTimeout,
        *timestamp,
        duplex_call_change->Header.SequenceCounter,
        duplex_call_change->m_A_Tsi.Ssi,
        duplex_call_change->m_A_Tsi.Mnc,
        duplex_call_change->m_A_Tsi.Mcc,
        digitsA,
        descrA,
        duplex_call_change->m_B_Tsi.Ssi,
        duplex_call_change->m_B_Tsi.Mnc,
        duplex_call_change->m_B_Tsi.Mcc,
        digitsB,
        descrB);

    rc = cspm_execute_db_command (ctx);

  } else {

    command = "INSERT INTO d_callstream_indicall_status_change "
        "(call_id, seq_no, received_at, action_id, timeout, "
        "calling_ssi, calling_mnc, calling_mcc, calling_esn, calling_descr, "
        "called_ssi, called_mnc, called_mcc, called_esn, called_descr) "
        "VALUES (%u,%u,to_timestamp(%u),%d,%d,%u,%d,%d,'%s','%s',%u,%d,%d,'%s','%s')";

    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
        duplex_call_change->m_uiCallId,
        duplex_call_change->Header.SequenceCounter,
        *timestamp,
        duplex_call_change->m_uiAction,
        duplex_call_change->m_uiTimeout,
        duplex_call_change->m_A_Tsi.Ssi,
        duplex_call_change->m_A_Tsi.Mnc,
        duplex_call_change->m_A_Tsi.Mcc,
        digitsA,
        descrA,
        duplex_call_change->m_B_Tsi.Ssi,
        duplex_call_change->m_B_Tsi.Mnc,
        duplex_call_change->m_B_Tsi.Mcc,
        digitsB,
        descrB);

    rc = cspm_execute_db_command (ctx);

  }

  TRACE (FUNCTIONS, "Leaving cspm_save_duplex_call_change");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiDuplexCallRelease message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiDuplexCallRelease message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_duplex_call_release (cspm_t *ctx, const time_t * const timestamp,
    const LogApiDuplexCallRelease * const duplex_call_release)
{
  int rc;
  char *command;

  TRACE (FUNCTIONS, "Entering in cspm_save_duplex_call_release");

  TRACE (DEBUG, "End call. Call Id: <%u>", duplex_call_release->m_uiCallId);

  command = "UPDATE d_callstream_indicall "
      "SET (call_end, seq_no_end, disconnect_cause) = (to_timestamp(%u), %u, %d) "
      "WHERE call_id = %u";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      *timestamp,
      duplex_call_release->Header.SequenceCounter,
      duplex_call_release->m_uiReleaseCause,
      duplex_call_release->m_uiCallId);

  rc = cspm_execute_db_command (ctx);

  TRACE (FUNCTIONS, "Leaving cspm_save_duplex_call_release");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiSimplexCallStartChange message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiSimplexCallStartChange message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_simplex_call_start_change(cspm_t *ctx, const time_t * const timestamp,
    const LogApiSimplexCallStartChange * const simplex_call_start_change)
{
  int rc;
  char *command;

  BYTE descrA[sizeof (simplex_call_start_change->m_A_Descr) + 1];
  BYTE descrB[sizeof (simplex_call_start_change->m_B_Descr) + 1];
  BYTE digitsA[NUMBER_LENGTH];
  BYTE digitsB[NUMBER_LENGTH];

  TRACE (FUNCTIONS, "Entering in cspm_save_simplex_call_start_change");

  cs_buffer_to_string (simplex_call_start_change->m_A_Descr,
      sizeof (descrA), descrA);
  cs_buffer_to_string (simplex_call_start_change->m_B_Descr,
      sizeof (descrB), descrB);

  cs_number_to_string (&(simplex_call_start_change->m_A_Number), digitsA);
  cs_number_to_string (&(simplex_call_start_change->m_B_Number), digitsB);

  if (simplex_call_start_change->m_uiAction == INDI_NEWCALLSETUP) {

    TRACE (DEBUG, "Begin call. Call Id: <%u>", simplex_call_start_change->m_uiCallId);

    command = "INSERT INTO d_callstream_indicall "
        "(call_id, timeout, call_begin, seq_no_begin, "
        "calling_ssi, calling_mnc, calling_mcc, calling_esn, calling_descr, "
        "called_ssi, called_mnc, called_mcc, called_esn, called_descr, "
        "simplex_duplex) "
        "VALUES (%u,%d,to_timestamp(%u),%u,%u,%d,%d,'%s','%s',%u,%d,%d,'%s','%s',0)";

    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
        simplex_call_start_change->m_uiCallId,
        simplex_call_start_change->m_uiTimeoutValue,
        *timestamp,
        simplex_call_start_change->Header.SequenceCounter,
        simplex_call_start_change->m_A_Tsi.Ssi,
        simplex_call_start_change->m_A_Tsi.Mnc,
        simplex_call_start_change->m_A_Tsi.Mcc,
        digitsA,
        descrA,
        simplex_call_start_change->m_B_Tsi.Ssi,
        simplex_call_start_change->m_B_Tsi.Mnc,
        simplex_call_start_change->m_B_Tsi.Mcc,
        digitsB,
        descrB);

    rc = cspm_execute_db_command (ctx);

  } else {

    command = "INSERT INTO d_callstream_indicall_status_change "
        "(call_id, seq_no, received_at, action_id, timeout, "
        "calling_ssi, calling_mnc, calling_mcc, calling_esn, calling_descr, "
        "called_ssi, called_mnc, called_mcc, called_esn, called_descr) "
        "VALUES (%u,%u,to_timestamp(%u),%d,%d,%u,%d,%d,'%s','%s',%u,%d,%d,'%s','%s')";

    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
        simplex_call_start_change->m_uiCallId,
        simplex_call_start_change->Header.SequenceCounter,
        *timestamp,
        simplex_call_start_change->m_uiAction,
        simplex_call_start_change->m_uiTimeoutValue,
        simplex_call_start_change->m_A_Tsi.Ssi,
        simplex_call_start_change->m_A_Tsi.Mnc,
        simplex_call_start_change->m_A_Tsi.Mcc,
        digitsA,
        descrA,
        simplex_call_start_change->m_B_Tsi.Ssi,
        simplex_call_start_change->m_B_Tsi.Mnc,
        simplex_call_start_change->m_B_Tsi.Mcc,
        digitsB,
        descrB);

    rc = cspm_execute_db_command (ctx);

  }

  TRACE (FUNCTIONS, "Leaving cspm_save_simplex_call_start_change");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiSimplexCallPttChange message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiSimplexCallPttChange message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_simplex_call_ptt_change (cspm_t *ctx, const time_t * const timestamp,
    const LogApiSimplexCallPttChange * const simplex_call_ptt_change)
{
  int rc;
  char *command;

  TRACE (FUNCTIONS, "Entering in cspm_save_simplex_call_ptt_change");

  command = "INSERT INTO d_callstream_indicall_ptt "
      "(call_id, seq_no, received_at, talking_party) "
      "VALUES (%u,%u,to_timestamp(%u),%d)";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      simplex_call_ptt_change->m_uiCallId,
      simplex_call_ptt_change->Header.SequenceCounter,
      *timestamp,
      simplex_call_ptt_change->m_uiTalkingParty);

  rc = cspm_execute_db_command (ctx);

  TRACE (FUNCTIONS, "Leaving cspm_save_simplex_call_ptt_change");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiSimplexCallRelease message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiSimplexCallRelease message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_simplex_call_release (cspm_t *ctx, const time_t * const timestamp,
    const LogApiSimplexCallRelease * const simplex_call_release)
{
  int rc;
  char *command;

  TRACE (FUNCTIONS, "Entering in cspm_save_simplex_call_release");

  TRACE (DEBUG, "End call. Call Id: <%u>", simplex_call_release->m_uiCallId);

  command = "UPDATE d_callstream_indicall "
      "SET (call_end, seq_no_end, disconnect_cause) = (to_timestamp(%u), %u, %d) "
      "WHERE call_id = %u";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      *timestamp,
      simplex_call_release->Header.SequenceCounter,
      simplex_call_release->m_uiReleaseCause,
      simplex_call_release->m_uiCallId);

  rc = cspm_execute_db_command (ctx);

  TRACE (FUNCTIONS, "Leaving cspm_save_simplex_call_release");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiGroupCallStartChange message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiGroupCallStartChange message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_group_call_start_change (cspm_t *ctx, const time_t * const timestamp,
    const LogApiGroupCallStartChange * const group_call_start_change)
{
  int rc;
  char *command;

  BYTE descrGroup[sizeof (group_call_start_change->m_Group_Descr) + 1];
  BYTE digitsGroup[NUMBER_LENGTH];

  TRACE (FUNCTIONS, "Entering in cspm_save_group_call_start_change");

  cs_buffer_to_string (group_call_start_change->m_Group_Descr,
      sizeof (descrGroup), descrGroup);

  cs_number_to_string (&(group_call_start_change->m_Group_Number), digitsGroup);

  if (group_call_start_change->m_uiAction == GROUPCALL_NEWCALLSETUP) {

    TRACE (DEBUG, "Begin call. Call Id: <%u>", group_call_start_change->m_uiCallId);

    command = "INSERT INTO d_callstream_groupcall "
        "(call_id, timeout, call_begin, seq_no_begin,"
        "group_ssi, group_mnc, group_mcc, group_esn, group_descr) "
        "VALUES (%u,%d,to_timestamp(%u),%u,%u,%d,%d,'%s','%s')";

    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
        group_call_start_change->m_uiCallId,
        group_call_start_change->m_uiTimeoutValue,
        *timestamp,
        group_call_start_change->Header.SequenceCounter,
        group_call_start_change->m_Group_Tsi.Ssi,
        group_call_start_change->m_Group_Tsi.Mnc,
        group_call_start_change->m_Group_Tsi.Mcc,
        digitsGroup,
        descrGroup);

    rc = cspm_execute_db_command (ctx);

  } else {

    command = "INSERT INTO d_callstream_groupcall_status_change "
        "(call_id, timeout, seq_no, received_at, message_id, action_id, "
        "group_ssi, group_mnc, group_mcc, group_esn, group_descr)"
        "VALUES (%u,%d,%u,to_timestamp(%u),%d,%d,%u,%d,%d,'%s','%s')";

    snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
        group_call_start_change->m_uiCallId,
        group_call_start_change->m_uiTimeoutValue,
        group_call_start_change->Header.SequenceCounter,
        *timestamp,
        group_call_start_change->Header.MsgId,
        group_call_start_change->m_uiAction,
        group_call_start_change->m_Group_Tsi.Ssi,
        group_call_start_change->m_Group_Tsi.Mnc,
        group_call_start_change->m_Group_Tsi.Mcc,
        digitsGroup,
        descrGroup);

    rc = cspm_execute_db_command (ctx);
  }

  TRACE (FUNCTIONS, "Leaving cspm_save_group_call_start_change");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiGroupCallPttActive message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiGroupCallPttActive message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_group_call_ptt_active (cspm_t *ctx, const time_t * const timestamp,
    const LogApiGroupCallPttActive * const group_call_ptt_active)
{
  int rc;
  char *command;

  BYTE descrTp[sizeof (group_call_ptt_active->m_TP_Descr) + 1];
  BYTE digitsTp[NUMBER_LENGTH];

  TRACE (FUNCTIONS, "Entering in cspm_save_group_call_ptt_active");

  cs_buffer_to_string (group_call_ptt_active->m_TP_Descr,
      sizeof (descrTp), descrTp);

  cs_number_to_string (&(group_call_ptt_active->m_TP_Number), digitsTp);

  command = "INSERT INTO d_callstream_groupcall_ptt "
      "(call_id, seq_no, received_at, message_id, "
      "tp_ssi, tp_mnc, tp_mcc, tp_esn, tp_descr) "
      "VALUES (%u,%u,to_timestamp(%u),%d,%u,%d,%d,'%s','%s')";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      group_call_ptt_active->m_uiCallId,
      group_call_ptt_active->Header.SequenceCounter,
      *timestamp,
      group_call_ptt_active->Header.MsgId,
      group_call_ptt_active->m_TP_Tsi.Ssi,
      group_call_ptt_active->m_TP_Tsi.Mnc,
      group_call_ptt_active->m_TP_Tsi.Mcc,
      digitsTp,
      descrTp);

  rc = cspm_execute_db_command (ctx);

  TRACE (FUNCTIONS, "Leaving cspm_save_group_call_ptt_active");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiGroupCallPttIdle message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiGroupCallPttIdle message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_group_call_ptt_idle (cspm_t *ctx, const time_t * const timestamp,
    const LogApiGroupCallPttIdle * const group_call_ptt_idle)
{
  int rc;
  char *command;

  TRACE (FUNCTIONS, "Entering in cspm_save_group_call_ptt_idle");

  command = "INSERT INTO d_callstream_groupcall_ptt "
      "(call_id, seq_no, received_at, message_id) "
      "VALUES (%u,%u,to_timestamp(%u),%d)";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      group_call_ptt_idle->m_uiCallId,
      group_call_ptt_idle->Header.SequenceCounter,
      *timestamp,
      group_call_ptt_idle->Header.MsgId);

  rc = cspm_execute_db_command (ctx);

  TRACE (FUNCTIONS, "Leaving cspm_save_group_call_ptt_idle");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiGroupCallRelease message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiGroupCallRelease message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_group_call_release (cspm_t *ctx, const time_t * const timestamp,
    const LogApiGroupCallRelease * const group_call_release)
{
  int rc;
  char *command;

  TRACE (FUNCTIONS, "Entering in cspm_save_group_call_release");

  TRACE (DEBUG, "End call. Call Id: <%u>", group_call_release->m_uiCallId);

  command = "UPDATE d_callstream_groupcall "
      "SET (call_end, seq_no_end, disconnect_cause) = (to_timestamp(%u), %u, %d) "
      "WHERE call_id = %u";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      *timestamp,
      group_call_release->Header.SequenceCounter,
      group_call_release->m_uiReleaseCause,
      group_call_release->m_uiCallId);

  rc = cspm_execute_db_command (ctx);

  TRACE (FUNCTIONS, "Leaving cspm_save_group_call_release");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiTextSDS message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiTextSDS message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_text_sds (cspm_t *ctx, const time_t * const timestamp,
    const LogApiTextSDS * const text_sds)
{
  int rc;
  char *command;

  BYTE descrA[sizeof (text_sds->m_A_Descr) + 1];
  BYTE descrB[sizeof (text_sds->m_B_Descr) + 1];
  BYTE text[sizeof (text_sds->m_TextData) + 1];
  BYTE digitsA[NUMBER_LENGTH];
  BYTE digitsB[NUMBER_LENGTH];

  TRACE (FUNCTIONS, "Entering in cspm_save_text_sds");

  cs_number_to_string (&text_sds->m_A_Number, digitsA);
  cs_number_to_string (&text_sds->m_B_Number, digitsB);

  cs_buffer_to_string (text_sds->m_A_Descr, sizeof (descrA), descrA);
  cs_buffer_to_string (text_sds->m_B_Descr, sizeof (descrB), descrB);
  cs_buffer_to_string (text_sds->m_TextData, sizeof (text), text);

  command = "INSERT INTO d_callstream_sdsdata "
      "(received_at, "
      "calling_ssi, calling_mnc, calling_mcc, calling_esn, calling_descr, "
      "called_ssi, called_mnc, called_mcc, called_esn, called_descr, "
      "user_data_length, user_data) "
      "VALUES (to_timestamp (%u),%u,%d,%d,'%s','%s',%u,%d,%d,'%s','%s',%d,'%s')";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      *timestamp,
      text_sds->m_A_Tsi.Ssi,
      text_sds->m_A_Tsi.Mnc,
      text_sds->m_A_Tsi.Mcc,
      digitsA,
      descrA,
      text_sds->m_B_Tsi.Ssi,
      text_sds->m_B_Tsi.Mnc,
      text_sds->m_B_Tsi.Mcc,
      digitsB,
      descrB,
      strlen (text),
      text);

  rc = cspm_execute_db_command (ctx);

  TRACE (FUNCTIONS, "Leaving cspm_save_text_sds");

  return rc;
}


//  --------------------------------------------------------------------------
// Saves the data of a LogApiStatusSDS message into the database
//  Input:
//    ctx: the Call Stream Persistence Manager context
//    timestamp: the reception time
//    keep_alive: the LogApiStatusSDS message
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_save_status_sds (cspm_t *ctx, const time_t * const timestamp,
    const LogApiStatusSDS * const status_sds)
{
  int rc;
  char *command;

  BYTE descrA[sizeof (status_sds->m_A_Descr) + 1];
  BYTE descrB[sizeof (status_sds->m_B_Descr) + 1];
  BYTE digitsA[NUMBER_LENGTH];
  BYTE digitsB[NUMBER_LENGTH];

  TRACE (FUNCTIONS, "Entering in cspm_save_text_sds");

  cs_number_to_string (&status_sds->m_A_Number, digitsA);
  cs_number_to_string (&status_sds->m_B_Number, digitsB);

  cs_buffer_to_string (status_sds->m_A_Descr, sizeof (descrA), descrA);
  cs_buffer_to_string (status_sds->m_B_Descr, sizeof (descrB), descrB);

  command = "INSERT INTO d_callstream_sdsstatus "
      "(received_at, "
      "calling_ssi, calling_mnc, calling_mcc, calling_esn, calling_descr, "
      "called_ssi, called_mnc, called_mcc, called_esn, called_descr, "
      "precoded_status_value) "
      "VALUES (to_timestamp (%u),%u,%d,%d,'%s','%s',%u,%d,%d,'%s','%s',%u)";

  snprintf (ctx->work_area, CSPM_BUFFER_WORK_AREA_LENGTH, command,
      *timestamp,
      status_sds->m_A_Tsi.Ssi,
      status_sds->m_A_Tsi.Mnc,
      status_sds->m_A_Tsi.Mcc,
      digitsA,
      descrA,
      status_sds->m_B_Tsi.Ssi,
      status_sds->m_B_Tsi.Mnc,
      status_sds->m_B_Tsi.Mcc,
      digitsB,
      descrB,
      status_sds->m_uiPrecodedStatusValue);

  rc = cspm_execute_db_command (ctx);

  TRACE (FUNCTIONS, "Leaving cspm_save_text_sds");

  return rc;
}


//  --------------------------------------------------------------------------
//  Callback responsible for processing a received LogApi message.
//  Input:
//    loop: the event-driven reactor
//    reader: the descriptor with the data received ready to read
//    arg: the Call Stream Persistence Manager context of the thread
//  Output:
//    0: processed
//   -1: not processed

static int
cspm_callstream_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  TRACE (FUNCTIONS, "Entering in cspm_callstream_handler");
  UINT32 msg_id;
  UINT32 call_id;
  int rc;

  cspm_t *ctx = (cspm_t *) arg;

  zmsg_t* msg;
  msg = zmsg_recv (reader);
  assert (msg);
  zmsg_print (msg);

  char *tag = zmsg_popstr (msg);
  zframe_t *timestamp = zmsg_pop (msg);
  zframe_t *log_api_msg = zmsg_pop (msg);

  if (zframe_size (timestamp) != sizeof (time_t)) {
    TRACE (ERROR, "Timestamp: Bad format");
  }

  rc = sscanf (tag, "S_%u", &msg_id);

  if (rc != EOF && rc > 0) {
    switch (msg_id) {
    case LOG_API_ALIVE:
      TRACE (DEBUG, "Message type: LOG_API_KEEP_ALIVE");
      if (zframe_size (log_api_msg) == sizeof (LogApiKeepAlive)) {
        LogApiKeepAlive *keep_alive;
        keep_alive = (LogApiKeepAlive *) zframe_data (log_api_msg);
        cspm_save_keep_alive (ctx, (time_t *) zframe_data (timestamp), keep_alive);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_DUPLEX_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_DUPLEX_CALL_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiDuplexCallChange)) {
        LogApiDuplexCallChange *duplex_call_change;
        duplex_call_change = (LogApiDuplexCallChange *) zframe_data (log_api_msg);
        if (duplex_call_change->m_uiAction == INDI_NEWCALLSETUP) {
          cspm_save_duplex_call_change (ctx, (time_t *) zframe_data (timestamp), duplex_call_change);
          cspm_init_cache_voice_data (ctx, duplex_call_change->m_uiCallId, 'D');
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_DUPLEX_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_DUPLEX_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiDuplexCallRelease)) {
        LogApiDuplexCallRelease *duplex_call_release;
        duplex_call_release = (LogApiDuplexCallRelease *) zframe_data (log_api_msg);
        cspm_save_duplex_call_release (ctx, (time_t *) zframe_data (timestamp), duplex_call_release);
        if (ctx->mp3_mode) {
          cspm_save_voice_data_as_mp3 (ctx, duplex_call_release->m_uiCallId);
        } else {
          cspm_save_voice_data (ctx, duplex_call_release->m_uiCallId);
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SIMPLEX_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_START_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiSimplexCallStartChange)) {
        LogApiSimplexCallStartChange *simplex_call_start_change;
        simplex_call_start_change = (LogApiSimplexCallStartChange *) zframe_data (log_api_msg);
        if (simplex_call_start_change->m_uiAction == INDI_NEWCALLSETUP) {
          cspm_save_simplex_call_start_change (ctx, (time_t *) zframe_data (timestamp), simplex_call_start_change);
          cspm_init_cache_voice_data (ctx, simplex_call_start_change->m_uiCallId, 'S');
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SIMPLEX_CALL_PTT_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_PTT_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiSimplexCallPttChange)) {
        LogApiSimplexCallPttChange *simplex_call_ptt_change;
        simplex_call_ptt_change = (LogApiSimplexCallPttChange *) zframe_data (log_api_msg);
        cspm_save_simplex_call_ptt_change (ctx, (time_t *) zframe_data (timestamp), simplex_call_ptt_change);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SIMPLEX_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiSimplexCallRelease)) {
        LogApiSimplexCallRelease *simplex_call_release;
        simplex_call_release = (LogApiSimplexCallRelease *) zframe_data (log_api_msg);
        cspm_save_simplex_call_release (ctx, (time_t *) zframe_data (timestamp), simplex_call_release);
        if (ctx->mp3_mode) {
          cspm_save_voice_data_as_mp3 (ctx, simplex_call_release->m_uiCallId);
        } else {
          cspm_save_voice_data (ctx, simplex_call_release->m_uiCallId);
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_START_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallStartChange)) {
        LogApiGroupCallStartChange *group_call_start_change;
        group_call_start_change = (LogApiGroupCallStartChange *) zframe_data (log_api_msg);
        if (group_call_start_change->m_uiAction == GROUPCALL_NEWCALLSETUP) {
          cspm_save_group_call_start_change (ctx, (time_t *) zframe_data (timestamp), group_call_start_change);
          cspm_init_cache_voice_data (ctx, group_call_start_change->m_uiCallId, 'G');
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_PTT_ACTIVE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_PPT_ACTIVE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallPttActive)) {
        LogApiGroupCallPttActive *group_call_ptt_active;
        group_call_ptt_active = (LogApiGroupCallPttActive *) zframe_data (log_api_msg);
        cspm_save_group_call_ptt_active (ctx, (time_t *) zframe_data (timestamp), group_call_ptt_active);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_PTT_IDLE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_PTT_IDLE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallPttIdle)) {
        LogApiGroupCallPttIdle *group_call_ptt_idle;
        group_call_ptt_idle = (LogApiGroupCallPttIdle *) zframe_data (log_api_msg);
        cspm_save_group_call_ptt_idle (ctx, (time_t *) zframe_data (timestamp), group_call_ptt_idle);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallRelease)) {
        LogApiGroupCallRelease *group_call_release;
        group_call_release = (LogApiGroupCallRelease *) zframe_data (log_api_msg);
        cspm_save_group_call_release (ctx, (time_t *) zframe_data (timestamp), group_call_release);
        if (ctx->mp3_mode) {
          cspm_save_voice_data_as_mp3 (ctx, group_call_release->m_uiCallId);
        } else {
          cspm_save_voice_data (ctx, group_call_release->m_uiCallId);
        }
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SDS_STATUS:
      TRACE (DEBUG, "Message type: LOG_API_SDS_STATUS");
      if (zframe_size (log_api_msg) == sizeof (LogApiStatusSDS)) {
        LogApiStatusSDS *status_sds;
        status_sds = (LogApiStatusSDS *) zframe_data (log_api_msg);
        cspm_save_status_sds (ctx, (time_t *) zframe_data (timestamp), status_sds);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SDS_TEXT:
      TRACE (DEBUG, "Message type: LOG_API_SDS_TEXT");
      if (zframe_size (log_api_msg) == sizeof (LogApiTextSDS)) {
        LogApiTextSDS *text_sds;
        text_sds = (LogApiTextSDS *) zframe_data (log_api_msg);
        cspm_save_text_sds (ctx, (time_t *) zframe_data (timestamp), text_sds);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    default:
      TRACE (DEBUG, "Message type: UNKNOWN (%x)", msg_id);
      break;
    }
  } else {
    rc = sscanf (tag, "V_%u", &call_id);
    if (rc != EOF && rc > 0) {
      LogApiVoice *log_api_voice;
      log_api_voice = (LogApiVoice *) zframe_data (log_api_msg);
      StreamOriginatorEnum originator = log_api_voice->m_uiStreamOriginator;
      TRACE (DEBUG, "Originator: <%d>", originator);
      zframe_t *voice_data = zmsg_pop (msg);
      cspm_cache_voice_data (ctx, call_id, originator, zframe_data (voice_data), zframe_size (voice_data));
      zframe_destroy (&voice_data);
    } else {
      TRACE (DEBUG, "Message tag: UNKNOWN (%s)", tag);
    }
  }

  free (tag);
  zframe_destroy (&timestamp);
  zframe_destroy (&log_api_msg);
  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving cspm_callstream_handler");

  return 0;
}


//  --------------------------------------------------------------------------
//  Reads the properties into a Call Stream Persistence Manager context from a
//  configuration file and creates all the needed resources
//  Input:
//    A Call Stream Persistence Manager context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cspm_configure (cspm_t *ctx)
{
  int rc = 0;
  char *string = NULL;
  char path [256];
  int x = 0;

  TRACE (FUNCTIONS, "Entering in cspm_configure");

  zconfig_t *root = zconfig_load (csstring_data (ctx->conf_filename));
  assert (root);

  string = zconfig_resolve (root, "/persistence_manager/pg_conn_info", "");
  ctx->pg_conn_info = csstring_new (string);

  string = zconfig_resolve (root, "/persistence_manager/mp3_converter_command_template", "");
  ctx->mp3_converter_command_template = csstring_new (string);

  string = zconfig_resolve (root, "/persistence_manager/call_inactivity_period", "300");
  ctx->call_inactivity_period = atoi (string);

  string = zconfig_resolve (root, "/persistence_manager/maintenance_frequency", "60");
  ctx->maintenance_frequency = atoi (string);

  string = zconfig_resolve (root, "/basic/mp3_mode", "0");
  ctx->mp3_mode = atoi (string);

  rc = cspm_connect_db (ctx);

  ctx->subscriber = zsock_new_sub (">inproc://collector", 0);
  assert (ctx->subscriber);

  string = zconfig_resolve (root, "/persistence_manager/subscriptions", "0");
  int num_subscriptions = atoi (string);

  for (x = 1; x <= num_subscriptions; x++) {
    snprintf (path, sizeof (path), "/persistence_manager/subscriptions/subscription_%d", x);
    string = zconfig_resolve (root, path, "0");
    zsock_set_subscribe (ctx->subscriber, string);
  }

  rc = zloop_reader (ctx->loop, ctx->subscriber, cspm_callstream_handler, ctx);

  zconfig_destroy (&root);

  TRACE (FUNCTIONS, "Leaving cspm_configure");

  return rc;
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

static int
cspm_maintenance_handler (zloop_t *loop, int timer_id, void *arg)
{
  TRACE (FUNCTIONS, "Entering in cspm_maintenance_handler");

  cspm_t *ctx = (cspm_t *) arg;
  time_t *last_call_activity;

  time_t now = time (NULL);

  last_call_activity = (time_t *) zhash_first (ctx->voice_calls_last_activity);

  while (last_call_activity) {
    const char * call_id_str = zhash_cursor (ctx->voice_calls_last_activity);
    double inactivity = difftime (now, *last_call_activity);
    TRACE (DEBUG, "Call <%s> had been without activity since <%.f> seconds",
        call_id_str, inactivity);
    if (inactivity > ctx->call_inactivity_period) {
      UINT32 call_id = atoi (zhash_cursor (ctx->voice_calls_last_activity));
      if (ctx->mp3_mode) {
        cspm_save_voice_data_as_mp3 (ctx, call_id);
      } else {
        cspm_save_voice_data (ctx, call_id);
      }
    }
    last_call_activity = (time_t *) zhash_next (ctx->voice_calls_last_activity);
  }

  TRACE (FUNCTIONS, "Leaving cspm_maintenance_handler");

  return 0;
}


//  --------------------------------------------------------------------------
//  Entry function to the Call Stream Persistence Manager submodule
//  Input:
//    pipe: the shared communication channel with the parent thread
//    arg: the configuration file with the submodule's customizable properties

void
cspm_task (zsock_t* pipe, void *args)
{
  int rc;
  int id_timer;
  cspm_t *ctx;
  char *conf_file;

  TRACE (FUNCTIONS, "Entering in cspm_task");

  conf_file = (char *) args;

  zloop_t *loop = zloop_new ();
  assert (loop);
  //zloop_set_verbose (loop, true);

  ctx = cspm_new (conf_file, loop);
  assert (ctx);

  rc = cspm_configure (ctx);
  cspm_print (ctx);

  rc = zloop_reader (loop, pipe, cspm_command_handler, ctx);
  id_timer = zloop_timer (loop, ctx->maintenance_frequency * 1000, 0,
      cspm_maintenance_handler, ctx);

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

  cspm_destroy (&ctx);
  zloop_timer_end (loop, id_timer);
  zloop_reader_end (loop, pipe);
  zloop_destroy (&loop);

  TRACE (FUNCTIONS, "Leaving cspm_task");
}
