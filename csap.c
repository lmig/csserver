/* =========================================================================
   csap - Alarm publisher submodule
   ========================================================================= */

/*
    This submodule is responsible for publish alarms based on events generated
    in the CallStream module.
*/


#include "cs.h"

#define CSAP_BUFFER_COMMAND_LENGTH 1024
#define CSAP_BUFFER_TEXT_LENGTH 1024
#define CSAP_BUFFER_SERVER_LENGTH 256


//  --------------------------------------------------------------------------
//  Publish an alarm
//  Input:
//

void
csap_send_alarm (const char* module, const char* text)
{
  char command[CSAP_BUFFER_COMMAND_LENGTH];
  char server[HOST_NAME_MAX];
  int rc = 0;

  TRACE (FUNCTIONS, "Entering in csap_send_alarm");

  memset (server, 0, sizeof(server));
  gethostname (server, HOST_NAME_MAX);

  snprintf (command, CSAP_BUFFER_COMMAND_LENGTH, 
            "%s/html/%s/aplicaciones/ALARMS/createAlarmEvent.pl " \
                "%s "                                             \
                "%s "                                             \
                "--event %s "                                     \
                "--object %s "                                    \
                "--text \"%s\" "                                  \
                "--source %s "                                    \
                "--type %s "                                      \
                "--subtype %s#%s "                                \
                "--priority %s "                                  \
                "--externalKey %s",
            getenv ("HTTPD_HOME"),
            getenv ("APLI"),
            getenv ("HTTPD_HOME"),
            getenv ("APLI"),
            "ACT",
            "TeNMS",
            text,
            "-",
            "CALLSTREAM_RECORD",
            "CALLSTREAM_RECORD",
            server,
            "1",
            "-"
  );

  TRACE (DEBUG, "command <%s>", command);

  rc = system(command);
  TRACE (DEBUG, "Result: <%d>", rc);

  TRACE (FUNCTIONS, "Leaving csap_send_alarm");
}
