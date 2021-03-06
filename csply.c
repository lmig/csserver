/*  =========================================================================
    csply - Voice Player submodule
    =========================================================================*/

/*
    This submodule is responsible for playing voice recordings.
*/


#include "cs.h"

// The properties of an active call

struct _csply_t {
  FILE *pipe_stream;
  zloop_t *loop;
  zsock_t *parent_channel;
};
typedef struct _csply_t csply_t;


//  --------------------------------------------------------------------------
//  Callback handler. Process the stop and finalization signal sent by the
//  parent thread.
//  Input:
//    loop: the reactor
//    reader: the communication channel shared with the parent thread endpoint
//    arg: the Call Stream Player context
//  Output:
//    0 - Ok
//    1 - Nok

static int
csply_command_handler (zloop_t *loop, zsock_t *reader, void *arg)
{
  int rc = 0;
  bool command_handled = false;
  csply_t *ctx = (csply_t *) arg;

  TRACE (FUNCTIONS, "Entering in csply_command_handler");

  zmsg_t *msg = zmsg_recv (reader);

  if (!msg) {
    TRACE (ERROR, "Empty message");
    rc = 1;
  }

  if (!rc) {
    char *command = zmsg_popstr (msg);
    TRACE (DEBUG, "Command: %s", command);

    if (streq (command, "$TERM")) {
      command_handled = true;
      rc = -1;
    }

    if ((!command_handled) && streq (command, "STOP")) {
      command_handled = true;
      if (fputs ("q\n", ctx->pipe_stream) < 0) {
        TRACE (ERROR, "Unable to finish player execution. Code: %d",
            ferror (ctx->pipe_stream));
      }
      if (fflush (ctx->pipe_stream) != 0) {
        TRACE (ERROR, "Unable to finish player execution. Code: %d",
            ferror (ctx->pipe_stream));
      }
    }

    if (!command_handled) {
      TRACE (ERROR, "Invalid message");
    }

    free (command);
    zmsg_destroy (&msg);
  }

  TRACE (FUNCTIONS, "Leaving csply_command_handler");

  return rc;
}


//  --------------------------------------------------------------------------
//  Callback handler. Process the finalization signal generated by the player
//  process.
//  Input:
//    loop: the reactor
//    item: the voice player process endpoint
//    arg: the Call Stream Player context
//  Output:
//    0 - Ok
//    1 - Nok

static int
csply_process_handler (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csply_process_handler");

  csply_t *ctx = (csply_t *) arg;

/*
  if (ctx->pipe_stream != NULL) {
    pclose (ctx->pipe_stream);
  }
*/

  zmsg_t *msg = zmsg_new ();
  assert (msg);

  rc = zmsg_addstr (msg, "PLAYER_FINISHED");
  assert (rc == 0);

  rc = zmsg_send (&msg, ctx->parent_channel);
  assert (rc == 0);

//  zmsg_destroy (&msg);

  TRACE (FUNCTIONS, "Leaving csply_process_handler");

  return rc;
}


//  --------------------------------------------------------------------------
//  Entry function to the Voice Player submodule
//  Input:
//    pipe: the shared communication channel with the parent thread
//    arg: the command to be executed as an individual process

void
csply_task (zsock_t* pipe, void *args)
{
  int rc = 0;
  csply_t ctx;

  TRACE (FUNCTIONS, "Entering in csply_task");

  ctx.parent_channel = pipe;

  char *player_command = (char *) args;

  zloop_t *loop = zloop_new ();
  assert (loop);

  ctx.loop = loop;

  TRACE (DEBUG, "Executing <%s>", player_command);

  rc = zloop_reader (loop, pipe, csply_command_handler, &ctx);
  assert (rc == 0);

  FILE *fp = popen (player_command, "w");
  assert (fp);

  ctx.pipe_stream = fp;

  zmq_pollitem_t item = { 0, fileno (fp), ZMQ_POLLIN, 0 };
  rc = zloop_poller (loop, &item, csply_process_handler, &ctx);
  assert (rc == 0);

  zsock_signal (pipe, 0);
  assert (rc == 0);

  if (!rc) {
    rc = zloop_start (loop);
    if (rc == 0) {
      TRACE (ERROR, "Interrupted!");
    }
    if (rc == -1) {
      TRACE (ERROR, "Cancelled!");
    }
  }

  if (fp != NULL) {
    pclose (fp);
  }

  zloop_reader_end (loop, pipe);
  zloop_poller_end (loop, &item);
  zloop_destroy (&loop);

  TRACE (FUNCTIONS, "Leaving csply_task");
}
