/*  =========================================================================
    cstrc - <Brief Description>

    Copyright (c)
    ...
    =========================================================================*/

/*
@header
    <Description>
@discuss
@end
*/


#include "cs.h"
#include "csutil.h"


#define CSTRC_HEADER_WORKAREA_LENGTH 1024
#define CSTRC_MESSAGE_WORKAREA_LENGTH 1024
#define NUMBER_LENGTH 30


// Context for a Call Stream Tracer thread

struct _cstrc_t {
  csstring_t *conf_filename;
  zloop_t *loop;
  zsock_t *subscriber;
  zsock_t *publisher;
  time_t timestamp;
  int publish_one_json_voice_msg_every;
  BYTE *header_work_area;
  BYTE *Jheader_work_area;
  BYTE *message_work_area;
  BYTE *Jmessage_work_area;
};
typedef struct _cstrc_t cstrc_t;



// --------------------------------------------------------------------------
// Creates a thread specific Call Stream Tracer context
// Input:
//   conf_file: The configuration file
//   loop: the event-reactor used in the thread
// Output:
//   The context created according to the configuration file

static cstrc_t*
cstrc_new (const char * const conf_file, zloop_t *loop)
{
  TRACE (FUNCTIONS, "Entering in cstrc_new");

  cstrc_t *self = (cstrc_t *) zmalloc (sizeof (cstrc_t));
  if (self) {
    self->conf_filename = csstring_new (conf_file);
    self->publish_one_json_voice_msg_every = 0;
    self->loop = loop;
    self->header_work_area = (BYTE *) zmalloc (
        CSTRC_HEADER_WORKAREA_LENGTH * sizeof (BYTE));
    self->Jheader_work_area = (BYTE *) zmalloc (
        CSTRC_HEADER_WORKAREA_LENGTH * sizeof (BYTE));
    self->message_work_area = (BYTE *) zmalloc (
        CSTRC_MESSAGE_WORKAREA_LENGTH * sizeof (BYTE));
    self->Jmessage_work_area = (BYTE *) zmalloc (
        CSTRC_MESSAGE_WORKAREA_LENGTH * sizeof (BYTE));
  }

  TRACE (FUNCTIONS, "Leaving cstrc_new");

  return self;
}


//  --------------------------------------------------------------------------
//  Frees all the resources created in the thread specific Call Stream Tracer
//  context
//  Input:
//    The target Call Stream Tracer context

static void
cstrc_destroy (cstrc_t **self_p)
{
  TRACE (FUNCTIONS, "Entering in cstrc_destroy");

  assert (self_p);
  if (*self_p) {
    cstrc_t *self = *self_p;
    csstring_destroy (&self->conf_filename);
    free (self->header_work_area);
    free (self->Jheader_work_area);
    free (self->message_work_area);
    free (self->Jmessage_work_area);
    if (self->subscriber) {
      zloop_reader_end (self->loop, self->subscriber);
      zsock_destroy (&self->subscriber);
    }
    if (self->publisher) {
      zloop_reader_end (self->loop, self->publisher);
      zsock_destroy (&self->publisher);
    }
    free (self);
    *self_p = NULL;
  }

  TRACE (FUNCTIONS, "Leaving cstrc_destroy");
}


//  --------------------------------------------------------------------------
//  Callback handler. Analyzes and process commands sent by the parent thread
//  through the shared pipe.
//  Input:
//    loop: the reactor
//    reader: the parent thread endpoint
//    arg: the Call Stream Tracer context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cstrc_command_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  bool command_handled = false;
  int result = 0;
//  cscol_t *ctx = (cscol_t *) arg;

  TRACE (FUNCTIONS, "Entering in cstrc_command_handler");

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

  TRACE (FUNCTIONS, "Leaving cstrc_command_handler");

  return result;
}


//  --------------------------------------------------------------------------
//  Builds a message header

static void
cstrc_generate_message_header (cstrc_t *ctx,
    const TetraFlexLogApiMessageHeader * const header)
{
  time_t now = time (NULL);

  TRACE (FUNCTIONS, "Entering in cstrc_trace_message_header");

  snprintf (ctx->header_work_area, CSTRC_HEADER_WORKAREA_LENGTH,
      "S|%" PRIu64 "|%x|%d|%d|%x",
      now,
      header->ProtocolSignature,
      header->SequenceCounter,
      header->ApiVersion,
      header->MsgId);

  snprintf (ctx->Jheader_work_area, CSTRC_HEADER_WORKAREA_LENGTH,
      "{\"type\":\"S\","
      "\"timestamp\":\"%" PRIu64 "\","
      "\"ProtocolSignature\":\"%x\","
      "\"SequenceCounter\":\"%d\","
      "\"ApiVersion\":\"%d\","
      "\"MsgId\":\"%x\"",
      now,
      header->ProtocolSignature,
      header->SequenceCounter,
      header->ApiVersion,
      header->MsgId);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_message_header");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_KEEP_ALIVE trace

static void
cstrc_trace_keep_alive (cstrc_t *ctx,
    const LogApiKeepAlive * const keep_alive)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_keep_alive");

  BYTE swVer[sizeof (keep_alive->m_bySwVer) + 1];
  BYTE swVerString[sizeof (keep_alive->m_bySwVerString) + 1];
  BYTE logServerDescr[sizeof (keep_alive->m_byLogServerDescr) + 1];

  cstrc_generate_message_header (ctx, &keep_alive->Header);

  snprintf (ctx->message_work_area,
      CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%s|%s|%s|",
      ctx->header_work_area,
      "LOG_API_KEEP_ALIVE",
      keep_alive->m_uiLogServerNo,
      keep_alive->m_uiTimeout,
      cs_buffer_to_string (keep_alive->m_bySwVer,
          sizeof (swVer), swVer),
      cs_buffer_to_string (keep_alive->m_bySwVerString,
          sizeof (swVerString), swVerString),
      cs_buffer_to_string (keep_alive->m_byLogServerDescr,
          sizeof (logServerDescr), logServerDescr));

  snprintf (ctx->Jmessage_work_area,
      CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiLogServerNo\":\"%d\","
      "\"m_uiTimeout\":\"%d\","
      "\"m_bySwVer\":\"%s\","
      "\"m_bySwVerString\":\"%s\","
      "\"m_byLogServerDescr\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_KEEP_ALIVE",
      keep_alive->m_uiLogServerNo,
      keep_alive->m_uiTimeout,
      cs_buffer_to_string (keep_alive->m_bySwVer,
          sizeof (swVer), swVer),
      cs_buffer_to_string (keep_alive->m_bySwVerString,
          sizeof (swVerString), swVerString),
      cs_buffer_to_string (keep_alive->m_byLogServerDescr,
          sizeof (logServerDescr), logServerDescr));

  TRACE (DEBUG, "%s", ctx->message_work_area);
  TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_keep_alive");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_DUPLEX_CALL_CHANGE trace

static void
cstrc_trace_duplex_call_change (cstrc_t *ctx,
    const LogApiDuplexCallChange * const duplex_call_change)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_duplex_call_change");

  BYTE descrA[sizeof (duplex_call_change->m_A_Descr) + 1];
  BYTE descrB[sizeof (duplex_call_change->m_B_Descr) + 1];
  BYTE digitsA[NUMBER_LENGTH];
  BYTE digitsB[NUMBER_LENGTH];

  cs_number_to_string (&(duplex_call_change->m_A_Number), digitsA);
  cs_number_to_string (&(duplex_call_change->m_B_Number), digitsB);

  cstrc_generate_message_header (ctx, &duplex_call_change->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%s|%d|%d|%d|%d|%s|%s|%d|%d|%d|%s|%s|",
      ctx->header_work_area,
      "LOG_API_DUPLEX_CALL_CHANGE",
      duplex_call_change->m_uiCallId,
      duplex_call_change->m_uiAction,
      cs_string_from_individual_call_change_action (
          duplex_call_change->m_uiAction),
      duplex_call_change->m_uiTimeout,
      duplex_call_change->m_A_Tsi.Mnc,
      duplex_call_change->m_A_Tsi.Mcc,
      duplex_call_change->m_A_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (duplex_call_change->m_A_Descr,
          sizeof (descrA), descrA),
      duplex_call_change->m_B_Tsi.Mnc,
      duplex_call_change->m_B_Tsi.Mcc,
      duplex_call_change->m_B_Tsi.Ssi,
      digitsB,
      cs_buffer_to_string (duplex_call_change->m_B_Descr,
      sizeof (descrB), descrB));

  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\","
      "\"m_uiAction\":\"%d\","
      "\"m_uiActionS\":\"%s\","
      "\"m_uiTimeout\":\"%d\","
      "\"m_A_Tsi_Mnc\":\"%d\","
      "\"m_A_Tsi_Mcc\":\"%d\","
      "\"m_A_Tsi_Ssi\":\"%d\","
      "\"digitsA\":\"%s\","
      "\"m_A_Descr\":\"%s\","
      "\"m_B_Tsi_Mnc\":\"%d\","
      "\"m_B_Tsi_Mcc\":\"%d\","
      "\"m_B_Tsi_Ssi\":\"%d\","
      "\"digitsB\":\"%s\","
      "\"m_B_Descr\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_DUPLEX_CALL_CHANGE",
      duplex_call_change->m_uiCallId,
      duplex_call_change->m_uiAction,
      cs_string_from_individual_call_change_action (
          duplex_call_change->m_uiAction),
      duplex_call_change->m_uiTimeout,
      duplex_call_change->m_A_Tsi.Mnc,
      duplex_call_change->m_A_Tsi.Mcc,
      duplex_call_change->m_A_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (duplex_call_change->m_A_Descr,
          sizeof (descrA), descrA),
      duplex_call_change->m_B_Tsi.Mnc,
      duplex_call_change->m_B_Tsi.Mcc,
      duplex_call_change->m_B_Tsi.Ssi,
      digitsB,
      cs_buffer_to_string (duplex_call_change->m_B_Descr,
          sizeof (descrB), descrB));

  TRACE (DEBUG, "%s", ctx->message_work_area);
  TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_duplex_call_change");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_DUPLEX_CALL_RELEASE trace

static void
cstrc_trace_duplex_call_release (cstrc_t *ctx,
    const LogApiDuplexCallRelease * const duplex_call_release)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_duplex_call_release");

  cstrc_generate_message_header (ctx, &duplex_call_release->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%s|",
      ctx->header_work_area,
      "LOG_API_DUPLEX_CALL_RELEASE",
      duplex_call_release->m_uiCallId,
      duplex_call_release->m_uiReleaseCause,
      cs_string_from_indi_call_release_cause (duplex_call_release->m_uiReleaseCause));


  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\","
      "\"m_uiReleaseCause\":\"%d\","
      "\"m_uiReleaseCause\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_DUPLEX_CALL_RELEASE",
      duplex_call_release->m_uiCallId,
      duplex_call_release->m_uiReleaseCause,
      cs_string_from_indi_call_release_cause (duplex_call_release->m_uiReleaseCause));

  TRACE(DEBUG, "%s", ctx->message_work_area);
  TRACE(DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_duplex_call_release");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_SIMPLEX_CALL_START_CHANGE trace

static void
cstrc_trace_simplex_call_start_change(cstrc_t *ctx,
    const LogApiSimplexCallStartChange * const simplex_call_start_change)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_simplex_call_start_change");

  BYTE descrA[sizeof (simplex_call_start_change->m_A_Descr) + 1];
  BYTE descrB[sizeof (simplex_call_start_change->m_B_Descr) + 1];
  BYTE digitsA[NUMBER_LENGTH];
  BYTE digitsB[NUMBER_LENGTH];

  cs_number_to_string (&simplex_call_start_change->m_A_Number, digitsA);
  cs_number_to_string (&simplex_call_start_change->m_B_Number, digitsB);

  cstrc_generate_message_header (ctx, &simplex_call_start_change->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%s|%d|%d|%d|%d|%s|%s|%d|%d|%d|%s|%s|",
      ctx->header_work_area,
      "LOG_API_SIMPLEX_CALL_START_CHANGE",
      simplex_call_start_change->m_uiCallId,
      simplex_call_start_change->m_uiAction,
      cs_string_from_individual_call_change_action(
          simplex_call_start_change->m_uiAction),
      simplex_call_start_change->m_uiTimeoutValue,
      simplex_call_start_change->m_A_Tsi.Mnc,
      simplex_call_start_change->m_A_Tsi.Mcc,
      simplex_call_start_change->m_A_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string(simplex_call_start_change->m_A_Descr,
          sizeof (descrA), descrA),
      simplex_call_start_change->m_B_Tsi.Mnc,
      simplex_call_start_change->m_B_Tsi.Mcc,
      simplex_call_start_change->m_B_Tsi.Ssi,
      digitsB,
      cs_buffer_to_string(simplex_call_start_change->m_A_Descr,
          sizeof (descrB), descrB));

  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\","
      "\"m_uiAction\":\"%d\","
      "\"m_uiActionS\":\"%s\","
      "\"m_uiTimeoutValue\":\"%d\","
      "\"m_A_Tsi_Mnc\":\"%d\","
      "\"m_A_Tsi_Mcc\":\"%d\","
      "\"m_A_Tsi_Ssi\":\"%d\","
      "\"digitsA\":\"%s\","
      "\"m_A_Descr\":\"%s\","
      "\"m_B_Tsi_Mnc\":\"%d\","
      "\"m_B_Tsi_Mcc\":\"%d\","
      "\"m_B_Tsi_Ssi\":\"%d\","
      "\"digitsB\":\"%s\","
      "\"m_B_Descr\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_SIMPLEX_CALL_START_CHANGE",
      simplex_call_start_change->m_uiCallId,
      simplex_call_start_change->m_uiAction,
      cs_string_from_individual_call_change_action(
          simplex_call_start_change->m_uiAction),
      simplex_call_start_change->m_uiTimeoutValue,
      simplex_call_start_change->m_A_Tsi.Mnc,
      simplex_call_start_change->m_A_Tsi.Mcc,
      simplex_call_start_change->m_A_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string(simplex_call_start_change->m_A_Descr,
          sizeof (descrA), descrA),
      simplex_call_start_change->m_B_Tsi.Mnc,
      simplex_call_start_change->m_B_Tsi.Mcc,
      simplex_call_start_change->m_B_Tsi.Ssi,
      digitsB,
      cs_buffer_to_string(simplex_call_start_change->m_B_Descr,
          sizeof (descrB), descrB));

  TRACE(DEBUG, "%s", ctx->message_work_area);
  TRACE(DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_simplex_call_start_change");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_SIMPLEX_CALL_PTT_CHANGE trace

static void
cstrc_trace_simplex_call_ptt_change (cstrc_t *ctx,
    const LogApiSimplexCallPttChange * const simplex_call_ptt_change)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_simplex_call_ptt_change");

  cstrc_generate_message_header (ctx, &simplex_call_ptt_change->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%s|",
      ctx->header_work_area,
      "LOG_API_SIMPLEX_CALL_PTT_CHANGE",
      simplex_call_ptt_change->m_uiCallId,
      simplex_call_ptt_change->m_uiTalkingParty,
      cs_string_from_simplex_ptt (simplex_call_ptt_change->m_uiTalkingParty));


  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\","
      "\"m_uiTalkingParty\":\"%d\","
      "\"m_uiTalkingParty\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_SIMPLEX_CALL_PTT_CHANGE",
      simplex_call_ptt_change->m_uiCallId,
      simplex_call_ptt_change->m_uiTalkingParty,
      cs_string_from_simplex_ptt (simplex_call_ptt_change->m_uiTalkingParty));

  TRACE (DEBUG, "%s", ctx->message_work_area);
  TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_simplex_call_ptt_change");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_SIMPLEX_CALL_RELEASE trace

static void
cstrc_trace_simplex_call_release (cstrc_t *ctx,
    const LogApiSimplexCallRelease * const simplex_call_release)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_simplex_call_release");

  cstrc_generate_message_header (ctx, &simplex_call_release->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%s|",
      ctx->header_work_area,
      "LOG_API_SIMPLEX_CALL_RELEASE",
      simplex_call_release->m_uiCallId,
      simplex_call_release->m_uiReleaseCause,
      cs_string_from_indi_call_release_cause (
          simplex_call_release->m_uiReleaseCause));


  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\","
      "\"m_uiReleaseCause\":\"%d\","
      "\"m_uiReleaseCause\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_SIMPLEX_CALL_RELEASE",
      simplex_call_release->m_uiCallId,
      simplex_call_release->m_uiReleaseCause,
      cs_string_from_indi_call_release_cause (
          simplex_call_release->m_uiReleaseCause));

  TRACE (DEBUG, "%s", ctx->message_work_area);
  TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_simplex_call_release");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_GROUP_CALL_START_CHANGE trace

static void
cstrc_trace_group_call_start_change (cstrc_t *ctx,
    const LogApiGroupCallStartChange * const group_call_start_change)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_group_call_start_change");

  BYTE grpDescr[sizeof (group_call_start_change->m_Group_Descr) + 1];
  BYTE digitsA[NUMBER_LENGTH];

  cs_number_to_string (&group_call_start_change->m_Group_Number, digitsA);

  cstrc_generate_message_header (ctx, &group_call_start_change->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%s|%d|%d|%d|%d|%s|%s|",
      ctx->header_work_area,
      "LOG_API_GROUP_CALL_START_CHANGE",
      group_call_start_change->m_uiCallId,
      group_call_start_change->m_uiAction,
      cs_string_from_group_call_change_action (
          group_call_start_change->m_uiAction),
      group_call_start_change->m_uiTimeoutValue,
      group_call_start_change->m_Group_Tsi.Mnc,
      group_call_start_change->m_Group_Tsi.Mcc,
      group_call_start_change->m_Group_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (group_call_start_change->m_Group_Descr,
          sizeof (grpDescr), grpDescr));

  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\","
      "\"m_uiAction\":\"%d\","
      "\"m_uiActionS\":\"%s\","
      "\"m_uiTimeoutValue\":\"%d\","
      "\"m_Group_Tsi_Mnc\":\"%d\","
      "\"m_Group_Tsi_Mcc\":\"%d\","
      "\"m_Group_Tsi_Ssi\":\"%d\","
      "\"digitsA\":\"%s\","
      "\"m_Group_Descr\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_GROUP_CALL_START_CHANGE",
      group_call_start_change->m_uiCallId,
      group_call_start_change->m_uiAction,
      cs_string_from_group_call_change_action (
          group_call_start_change->m_uiAction),
      group_call_start_change->m_uiTimeoutValue,
      group_call_start_change->m_Group_Tsi.Mnc,
      group_call_start_change->m_Group_Tsi.Mcc,
      group_call_start_change->m_Group_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (group_call_start_change->m_Group_Descr,
          sizeof (grpDescr), grpDescr));

  TRACE (DEBUG, "%s", ctx->message_work_area);
  TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_group_call_start_change");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_GROUP_CALL_PTT_ACTIVE trace

static void
cstrc_trace_group_call_ptt_active (cstrc_t *ctx,
    const LogApiGroupCallPttActive * const group_call_ptt_active)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_group_call_ptt_active");

  BYTE descr[sizeof (group_call_ptt_active->m_TP_Descr) + 1];
  BYTE digitsA[NUMBER_LENGTH];

  cs_number_to_string (&group_call_ptt_active->m_TP_Number, digitsA);

  cstrc_generate_message_header (ctx, &group_call_ptt_active->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%d|%d|%s|%s|",
      ctx->header_work_area,
      "LOG_API_GROUP_CALL_PTT_ACTIVE",
      group_call_ptt_active->m_uiCallId,
      group_call_ptt_active->m_TP_Tsi.Mnc,
      group_call_ptt_active->m_TP_Tsi.Mcc,
      group_call_ptt_active->m_TP_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (group_call_ptt_active->m_TP_Descr,
          sizeof (descr), descr));

  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\","
      "\"m_TP_Tsi_Mnc\":\"%d\","
      "\"m_TP_Tsi_Mcc\":\"%d\","
      "\"m_TP_Tsi_Ssi\":\"%d\","
      "\"digitsA\":\"%s\","
      "\"m_TP_Descr\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_GROUP_CALL_PTT_ACTIVE",
      group_call_ptt_active->m_uiCallId,
      group_call_ptt_active->m_TP_Tsi.Mnc,
      group_call_ptt_active->m_TP_Tsi.Mcc,
      group_call_ptt_active->m_TP_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (group_call_ptt_active->m_TP_Descr,
          sizeof (descr), descr));

  TRACE(DEBUG, "%s", ctx->message_work_area);
  TRACE(DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_group_call_ptt_active");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_GROUP_CALL_PTT_IDLE trace

static void
cstrc_trace_group_call_ptt_idle (cstrc_t *ctx,
    const LogApiGroupCallPttIdle * const group_call_ptt_idle)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_group_call_ptt_idle");

  cstrc_generate_message_header (ctx, &group_call_ptt_idle->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|",
      ctx->header_work_area,
      "LOG_API_GROUP_CALL_PTT_IDLE",
      group_call_ptt_idle->m_uiCallId);

  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\"}",
      ctx->Jheader_work_area,
      "LOG_API_GROUP_CALL_PTT_IDLE",
      group_call_ptt_idle->m_uiCallId);

  TRACE (DEBUG, "%s", ctx->message_work_area);
  TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_group_call_ptt_idle");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_GROUP_CALL_RELEASE trace

static void
cstrc_trace_group_call_release (cstrc_t *ctx,
    const LogApiGroupCallRelease * const group_call_release)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_group_call_release");

  cstrc_generate_message_header (ctx, &group_call_release->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%s|",
      ctx->header_work_area,
      "LOG_API_GROUP_CALL_RELEASE",
      group_call_release->m_uiCallId,
      group_call_release->m_uiReleaseCause,
      cs_string_from_group_call_release_cause (
          group_call_release->m_uiReleaseCause));

  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_uiCallId\":\"%d\","
      "\"m_uiReleaseCause\":\"%d\","
      "\"m_uiReleaseCause\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_GROUP_CALL_RELEASE",
      group_call_release->m_uiCallId,
      group_call_release->m_uiReleaseCause,
      cs_string_from_group_call_release_cause (
          group_call_release->m_uiReleaseCause));

  TRACE (DEBUG, "%s", ctx->message_work_area);
  TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_group_call_release");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_SDS_STATUS trace

static void
cstrc_trace_status_sds (cstrc_t *ctx,
    const LogApiStatusSDS * const status_sds)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_status_sds");

  BYTE descrA[sizeof (status_sds->m_A_Descr) + 1];
  BYTE descrB[sizeof (status_sds->m_B_Descr) + 1];
  BYTE digitsA[NUMBER_LENGTH];
  BYTE digitsB[NUMBER_LENGTH];

  cs_number_to_string (&status_sds->m_A_Number, digitsA);
  cs_number_to_string (&status_sds->m_B_Number, digitsB);

  cstrc_generate_message_header (ctx, &status_sds->Header);

  snprintf(ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%d|%s|%s|%d|%d|%d|%s|%s|%d|",
      ctx->header_work_area,
      "LOG_API_SDS_STATUS",
      status_sds->m_A_Tsi.Mnc,
      status_sds->m_A_Tsi.Mcc,
      status_sds->m_A_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (status_sds->m_A_Descr, sizeof (descrA), descrA),
      status_sds->m_B_Tsi.Mnc,
      status_sds->m_B_Tsi.Mcc,
      status_sds->m_B_Tsi.Ssi,
      digitsB,
      cs_buffer_to_string (status_sds->m_B_Descr, sizeof (descrB), descrB),
      status_sds->m_uiPrecodedStatusValue);

  snprintf(ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_A_Tsi_Mnc\":\"%d\","
      "\"m_A_Tsi_Mcc\":\"%d\","
      "\"m_A_Tsi_Ssi\":\"%d\","
      "\"digitsA\":\"%s\","
      "\"m_A_Descr\":\"%s\","
      "\"m_B_Tsi_Mnc\":\"%d\","
      "\"m_B_Tsi_Mcc\":\"%d\","
      "\"m_B_Tsi_Ssi\":\"%d\","
      "\"digitsB\":\"%s\","
      "\"m_B_Descr\":\"%s\","
      "\"m_uiPrecodedStatusValue\":\"%d\"}",
      ctx->Jheader_work_area,
      "LOG_API_SDS_STATUS",
      status_sds->m_A_Tsi.Mnc,
      status_sds->m_A_Tsi.Mcc,
      status_sds->m_A_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (status_sds->m_A_Descr, sizeof (descrA), descrA),
      status_sds->m_B_Tsi.Mnc,
      status_sds->m_B_Tsi.Mcc,
      status_sds->m_B_Tsi.Ssi,
      digitsB,
      cs_buffer_to_string (status_sds->m_B_Descr, sizeof (descrB), descrB),
      status_sds->m_uiPrecodedStatusValue);

  TRACE(DEBUG, "%s", ctx->message_work_area);
  TRACE(DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_status_sds");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a LOG_API_SDS_TEXT trace

static void
cstrc_trace_text_sds (cstrc_t *ctx,
    const LogApiTextSDS * const text_sds)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_text_sds");

  BYTE descrA[sizeof (text_sds->m_A_Descr) + 1];
  BYTE descrB[sizeof (text_sds->m_B_Descr) + 1];
  BYTE text[sizeof (text_sds->m_TextData) + 1];
  BYTE digitsA[NUMBER_LENGTH];
  BYTE digitsB[NUMBER_LENGTH];

  cs_number_to_string (&text_sds->m_A_Number, digitsA);
  cs_number_to_string (&text_sds->m_B_Number, digitsB);

  cstrc_generate_message_header (ctx, &text_sds->Header);

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|%s|%s|%d|%d|%d|%s|%s|%d|%d|%d|%s|%s|%s|",
      ctx->header_work_area,
      "LOG_API_SDS_TEXT",
      text_sds->m_A_Tsi.Mnc,
      text_sds->m_A_Tsi.Mcc,
      text_sds->m_A_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (text_sds->m_A_Descr, sizeof(descrA), descrA),
      text_sds->m_B_Tsi.Mnc,
      text_sds->m_B_Tsi.Mcc,
      text_sds->m_B_Tsi.Ssi,
      digitsB,
      cs_buffer_to_string (text_sds->m_B_Descr, sizeof(descrB), descrB),
      cs_buffer_to_string (text_sds->m_TextData, sizeof(text), text));

  snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "%s,"
      "\"message_type\":\"%s\","
      "\"m_A_Tsi_Mnc\":\"%d\","
      "\"m_A_Tsi_Mcc\":\"%d\","
      "\"m_A_Tsi_Ssi\":\"%d\","
      "\"digitsA\":\"%s\","
      "\"m_A_Descr\":\"%s\","
      "\"m_B_Tsi_Mnc\":\"%d\","
      "\"m_B_Tsi_Mcc\":\"%d\","
      "\"m_B_Tsi_Ssi\":\"%d\","
      "\"digitsB\":\"%s\","
      "\"m_B_Descr\":\"%s\","
      "\"m_TextData\":\"%s\"}",
      ctx->Jheader_work_area,
      "LOG_API_SDS_TEXT",
      text_sds->m_A_Tsi.Mnc,
      text_sds->m_A_Tsi.Mcc,
      text_sds->m_A_Tsi.Ssi,
      digitsA,
      cs_buffer_to_string (text_sds->m_A_Descr, sizeof(descrA), descrA),
      text_sds->m_B_Tsi.Mnc,
      text_sds->m_B_Tsi.Mcc,
      text_sds->m_B_Tsi.Ssi,
      digitsB,
      cs_buffer_to_string (text_sds->m_B_Descr, sizeof(descrB), descrB),
      cs_buffer_to_string (text_sds->m_TextData, sizeof(text), text));

  TRACE (DEBUG, "%s", ctx->message_work_area);
  TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

  zmsg_t * msg_R = zmsg_new ();
  zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
  zmsg_send (&msg_R, ctx->publisher);

  TRACE (FUNCTIONS, "Leaving cstrc_trace_text_sds");
}


//  --------------------------------------------------------------------------
//  Builds, trace and publish a VOICE trace

static void
cstrc_trace_voice (cstrc_t *ctx,
    const LogApiVoice * const voice)
{
  TRACE (FUNCTIONS, "Entering in cstrc_trace_voice");

  static int counter = 0;

  snprintf (ctx->message_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "|V|%" PRIu64 "|%x|%d|%d|%d|%u|%d|%d|%d|%d|",
      ctx->timestamp,
      voice->m_uiProtocolSignature,
      voice->m_uiApiProtocolVersion,
      voice->m_uiStreamOriginator,
      voice->m_uiOriginatingNode,
      voice->m_uiCallId,
      voice->m_uiSourceAndIndex,
      voice->m_uiStreamRandomId,
      voice->m_uiPacketSeq,
      voice->m_uiPayload1Info);
  TRACE (DEBUG, "%s", ctx->message_work_area);

  counter++;
  if (counter > ctx->publish_one_json_voice_msg_every) {
    snprintf (ctx->Jmessage_work_area, CSTRC_MESSAGE_WORKAREA_LENGTH,
      "{\"type\":\"V\","
      "\"timestamp\":\"%" PRIu64 "\","
      "\"message_type\":\"VOICE\","
      "\"m_uiProtocolSignature\":\"%x\","
      "\"m_uiApiProtocolVersion\":\"%d\","
      "\"m_uiStreamOriginator\":\"%d\","
      "\"m_uiOriginatingNode\":\"%d\","
      "\"m_uiCallId\":\"%u\","
      "\"m_uiSourceAndIndex\":\"%d\","
      "\"m_uiStreamRandomId\":\"%d\","
      "\"m_uiPacketSeq\":\"%d\","
      "\"m_uiPayload1Info\":\"%d\"}",
      ctx->timestamp,
      voice->m_uiProtocolSignature,
      voice->m_uiApiProtocolVersion,
      voice->m_uiStreamOriginator,
      voice->m_uiOriginatingNode,
      voice->m_uiCallId,
      voice->m_uiSourceAndIndex,
      voice->m_uiStreamRandomId,
      voice->m_uiPacketSeq,
      voice->m_uiPayload1Info);

    TRACE (DEBUG, "%s", ctx->Jmessage_work_area);

    zmsg_t * msg_R = zmsg_new ();
    zmsg_pushstr (msg_R, ctx->Jmessage_work_area);
    zmsg_send (&msg_R, ctx->publisher);
  
    counter = 0;
  }

  TRACE (FUNCTIONS, "Leaving cstrc_trace_voice");
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
cstrc_callstream_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  TRACE (FUNCTIONS, "Entering in cstrc_callstream_handler");
  UINT32 msg_id;
  UINT32 call_id;
  int rc;

  cstrc_t *ctx = (cstrc_t *) arg;

  zmsg_t* msg;
  msg = zmsg_recv (reader);
  assert (msg);
//  zmsg_print (msg);

  char *tag = zmsg_popstr (msg);
  zframe_t *timestamp = zmsg_pop (msg);
  zframe_t *log_api_msg = zmsg_pop (msg);

  if (zframe_size (timestamp) != sizeof (time_t)) {
    TRACE (ERROR, "Timestamp: Bad format");
    time_t now = time (NULL);
    memcpy (&ctx->timestamp, &now, sizeof (time_t));
  } else {
    memcpy (&ctx->timestamp, zframe_data (timestamp), sizeof (time_t));
  }

  rc = sscanf (tag, "S_%u", &msg_id);

  if (rc != EOF && rc > 0) {
    switch (msg_id) {
    case LOG_API_ALIVE:
      TRACE (DEBUG, "Message type: LOG_API_KEEP_ALIVE");
      if (zframe_size (log_api_msg) == sizeof (LogApiKeepAlive)) {
        LogApiKeepAlive *keep_alive =
            (LogApiKeepAlive *) zframe_data (log_api_msg);
        cstrc_trace_keep_alive (ctx, keep_alive);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_DUPLEX_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_DUPLEX_CALL_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiDuplexCallChange)) {
        LogApiDuplexCallChange *duplex_call_change =
            (LogApiDuplexCallChange *) zframe_data (log_api_msg);
        cstrc_trace_duplex_call_change (ctx, duplex_call_change);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_DUPLEX_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_DUPLEX_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiDuplexCallRelease)) {
        LogApiDuplexCallRelease *duplex_call_release =
            (LogApiDuplexCallRelease *) zframe_data (log_api_msg);
        cstrc_trace_duplex_call_release (ctx, duplex_call_release);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SIMPLEX_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_START_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiSimplexCallStartChange)) {
        LogApiSimplexCallStartChange *simplex_call_start_change =
            (LogApiSimplexCallStartChange *) zframe_data (log_api_msg);
        cstrc_trace_simplex_call_start_change (ctx, simplex_call_start_change);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SIMPLEX_CALL_PTT_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_PTT_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiSimplexCallPttChange)) {
        LogApiSimplexCallPttChange *simplex_call_ptt_change =
            (LogApiSimplexCallPttChange *) zframe_data (log_api_msg);
        cstrc_trace_simplex_call_ptt_change (ctx, simplex_call_ptt_change);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SIMPLEX_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_SIMPLEX_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiSimplexCallRelease)) {
        LogApiSimplexCallRelease *simplex_call_release =
            (LogApiSimplexCallRelease *) zframe_data (log_api_msg);
        cstrc_trace_simplex_call_release (ctx, simplex_call_release);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_CHANGE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_START_CHANGE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallStartChange)) {
        LogApiGroupCallStartChange *group_call_start_change =
            (LogApiGroupCallStartChange *) zframe_data (log_api_msg);
        cstrc_trace_group_call_start_change (ctx, group_call_start_change);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_PTT_ACTIVE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_PPT_ACTIVE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallPttActive)) {
        LogApiGroupCallPttActive *group_call_ptt_active =
            (LogApiGroupCallPttActive *) zframe_data (log_api_msg);
        cstrc_trace_group_call_ptt_active (ctx, group_call_ptt_active);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_PTT_IDLE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_PTT_IDLE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallPttIdle)) {
        LogApiGroupCallPttIdle *group_call_ptt_idle =
            (LogApiGroupCallPttIdle *) zframe_data (log_api_msg);
        cstrc_trace_group_call_ptt_idle (ctx, group_call_ptt_idle);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_GROUP_CALL_RELEASE:
      TRACE (DEBUG, "Message type: LOG_API_GROUP_CALL_RELEASE");
      if (zframe_size (log_api_msg) == sizeof (LogApiGroupCallRelease)) {
        LogApiGroupCallRelease *group_call_release =
            (LogApiGroupCallRelease *) zframe_data (log_api_msg);
        cstrc_trace_group_call_release (ctx, group_call_release);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SDS_STATUS:
      TRACE (DEBUG, "Message type: LOG_API_SDS_STATUS");
      if (zframe_size (log_api_msg) == sizeof (LogApiStatusSDS)) {
        LogApiStatusSDS *status_sds =
            (LogApiStatusSDS *) zframe_data (log_api_msg);
        cstrc_trace_status_sds (ctx, status_sds);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
      break;
    case LOG_API_SDS_TEXT:
      TRACE (DEBUG, "Message type: LOG_API_SDS_TEXT");
      if (zframe_size (log_api_msg) == sizeof (LogApiTextSDS)) {
        LogApiTextSDS *text_sds =
            (LogApiTextSDS *) zframe_data (log_api_msg);
        cstrc_trace_text_sds (ctx, text_sds);
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
      TRACE (DEBUG, "Message type: LOG_API_VOICE");
      if (zframe_size (log_api_msg) == sizeof (LogApiVoice)) {
        LogApiVoice *voice =
            (LogApiVoice *) zframe_data (log_api_msg);
        cstrc_trace_voice (ctx, voice);
      } else {
        TRACE (ERROR, "LogApi message: Bad format");
      }
    } else {
      TRACE (DEBUG, "Message tag: UNKNOWN (%s)", tag);
    }
  }

  free (tag);
  zframe_destroy (&timestamp);
  zframe_destroy (&log_api_msg);
  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving cstrc_callstream_handler");

  return 0;
}


//  --------------------------------------------------------------------------
//  Traces a Call Stream Tracer context
//    Input:
//      A Call Stream Tracer context

static void
cstrc_print (cstrc_t *ctx)
{
  TRACE (FUNCTIONS, "Entering in cstrc_print");

  TRACE (DEBUG, "--------------------");
  TRACE (DEBUG, "Tracer Configuration");
  TRACE (DEBUG, "--------------------");
  TRACE (DEBUG, "  File: %s", csstring_data (ctx->conf_filename));
  TRACE (DEBUG, "  Subscriber: %s", zsock_type_str (ctx->subscriber));
  TRACE (DEBUG, "  JSON Publisher: %s", zsock_type_str (ctx->publisher));
  TRACE (DEBUG, "  Publish JSON Voice Messages every: %d", ctx->publish_one_json_voice_msg_every);

  TRACE (FUNCTIONS, "Leaving cstrc_print");
}


//  --------------------------------------------------------------------------
//  Reads the properties into a Call Stream Tracer context from a
//  configuration file and creates all the needed resources
//  Input:
//    A Call Stream Tracer context
//  Output:
//    0 - Ok
//   -1 - Nok

static int
cstrc_configure (cstrc_t *ctx)
{
  int rc = 0;
  char *string = NULL;
  char path [256];
  int x = 0;

  TRACE (FUNCTIONS, "Entering in cstrc_configure");

  zconfig_t *root = zconfig_load (csstring_data (ctx->conf_filename));
//  assert (root);

  ctx->subscriber = zsock_new_sub (">inproc://collector", 0);
//  assert (ctx->subscriber);

  string = zconfig_resolve (root, "/tracer_manager/publish_one_json_voice_msg_every", "0");
  int num = atoi (string);
  ctx->publish_one_json_voice_msg_every = num;

  string = zconfig_resolve (root, "/tracer_manager/subscriptions", "0");
  int num_subscriptions = atoi (string);

  for (x = 1; x <= num_subscriptions; x++) {
    snprintf (path, sizeof (path), "/tracer_manager/subscriptions/subscription_%d", x);
    string = zconfig_resolve (root, path, "0");
    zsock_set_subscribe (ctx->subscriber, string);
  }

  string = zconfig_resolve (root, "/tracer_manager/json_publisher", "tcp://*:5501");
  ctx->publisher = zsock_new_pub (string);
//  assert (ctx->publisher);

  rc = zloop_reader (ctx->loop, ctx->subscriber, cstrc_callstream_handler, ctx);

  zconfig_destroy (&root);

  TRACE (FUNCTIONS, "Leaving cstrc_configure");

  return rc;
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

//static int
//cstrc_timer_handler (zloop_t *loop, int timer_id, void *arg)
//{
//  cscol_t *ctx = (cscol_t *) arg;
//  TRACE (FUNCTIONS, "Entering in cscol_timer_handler");
//  TRACE (FUNCTIONS, "Leaving cscol_timer_handler");
//
//  return 0;
//}


//  --------------------------------------------------------------------------
//  Entry function to the Call Stream Tracer submodule
//  Input:
//    pipe: the shared communication channel with the parent thread
//    arg: the configuration file with the submodule's customizable properties

void
cstrc_task (zsock_t* pipe, void *args)
{
  int rc = -1;
//  int id_timer;
  cstrc_t *ctx;
  char *conf_file;

  TRACE (FUNCTIONS, "Entering in cstrc_task");

  conf_file = (char *) args;

  zloop_t *loop = zloop_new ();
  assert (loop);
//  zloop_set_verbose (loop, true);

  ctx = cstrc_new (conf_file, loop);
  assert (ctx);

  rc = cstrc_configure (ctx);
  cstrc_print (ctx);

  rc = zloop_reader (loop, pipe, cstrc_command_handler, ctx);
//  id_timer = zloop_timer (loop, 5000, 0, cstrc_timer_handler, ctx);

  rc = zsock_signal (pipe, 0);

  if (!rc) {
    rc = zloop_start (loop);
    if (rc == 0) {
      TRACE (ERROR, "Interrupted!");
    }
    if (rc == -1) {
      TRACE (ERROR, "Cancelled!");
    }
  }

  cstrc_destroy (&ctx);
//  zloop_timer_end (loop, id_timer);
  zloop_reader_end (loop, pipe);
  zloop_destroy (&loop);

  TRACE (FUNCTIONS, "Leaving cstrc_task");
}

