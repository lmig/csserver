#include "cs.h"

#ifdef __cplusplus
extern "C" {
#endif

void cspm_task (zsock_t *pipe, void *args);
void csmm_task (zsock_t *pipe, void *args);
void cscol_task (zsock_t *pipe, void *args);
void cstrc_task (zsock_t *pipe, void *args);

#ifdef __cplusplus
}
#endif


int main ()
{
  char *var = NULL;
  char conf_file[256];
  char work_dir[256];
  char trace_file[256];
  char httpd_home[256];
  char apli[256];
  int rc = 0;
  zloop_t *loop = NULL;
  zactor_t *collector = NULL;
  zactor_t *media_manager = NULL;
  zactor_t *persistence_manager = NULL;
  zactor_t *tracer_manager = NULL;


  if ((var = getenv ("CALLSTREAMSERVER_WORK_PATH")) != NULL) {
    strcpy (work_dir, var);
    zsys_daemonize (work_dir);
  } else {
    printf ("Error: variable CALLSTREAMSERVER_WORK_PATH not defined\n");
    rc = -1;
  }

  if (!rc) {
    snprintf (trace_file, 256, "%s/csserver.trace", work_dir);
    initTRACE (trace_file, "csserver");
    TRACE (DEBUG, "work_dir: %s", work_dir);
  }
    
  TRACE (FUNCTIONS, "Entering in main");

  if (!rc) {
    if ((var = getenv ("CALLSTREAMSERVER_CONF_FILE")) != NULL) {
      strcpy (conf_file, var);
      TRACE (DEBUG, "conf_file: %s", conf_file);
    } else {
      TRACE (ERROR, "Error: variable CALLSTREAMSERVER_CONF_FILE not defined");
      rc = -1;
    }
  }

  if (!rc) {
    if ((var = getenv ("HTTPD_HOME")) != NULL) {
      strcpy (httpd_home, var);
      TRACE (DEBUG, "httpd_home: %s", httpd_home);
    } else {
      TRACE (ERROR, "Error: variable HTTPD_HOME not defined");
      rc = -1;
    }
  }

  if (!rc) {
    if ((var = getenv ("APLI")) != NULL) {
      strcpy (apli, var);
      TRACE (DEBUG, "apli: %s", apli);
    } else {
      TRACE (ERROR, "Error: variable APLI not defined");
      rc = -1;
    }
  }

  if (!rc) {
    collector = zactor_new (cscol_task, conf_file);
    assert (collector);
    if (!collector) {
      TRACE (ERROR, "Collector not created");
      rc = -1;
    }
  }

  if (!rc) {
    media_manager = zactor_new (csmm_task, conf_file);
    assert (media_manager);
    if (!media_manager) {
      TRACE (ERROR, "Media Manager not created");
      rc = -1;
    }
  }

  if (!rc) {
    persistence_manager = zactor_new (cspm_task, conf_file);
    assert (persistence_manager);
    if (!persistence_manager) {
      TRACE (ERROR, "Persistence Manager not created");
      rc = -1;
    }
  }

  if (!rc) {
    tracer_manager = zactor_new (cstrc_task, conf_file);
    assert (tracer_manager);
    if (!tracer_manager) {
      TRACE (ERROR, "Tracer Manager not created");
      rc = -1;
    }
  }

  if (!rc) {
    loop = zloop_new ();
    assert (loop);
    if (!loop) {
      TRACE (ERROR, "Reactor not created");
      rc = -1;
    }
  }

  if (!rc) {
    rc = zloop_start (loop);
    if (rc == 0) {
      TRACE (ERROR, "Interrupted!");
    }
    if (rc == -1) {
      TRACE (ERROR, "Cancelled!");
    }
  }

  if (collector) zactor_destroy (&collector);
  if (media_manager) zactor_destroy (&media_manager);
  if (persistence_manager) zactor_destroy (&persistence_manager);
  if (tracer_manager) zactor_destroy (&tracer_manager);
  if (loop) zloop_destroy (&loop);

  TRACE (FUNCTIONS, "Leaving main");

  return rc;
}

