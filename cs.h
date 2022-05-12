#ifndef __CS_H_INCLUDED__
#define __CS_H_INCLUDED__

#include "LogApiMsgDef.h"
#include "trace.h"
#include "czmq.h"

#define L_TR_CS   16384
#define TR_CS     L_TR_CS, TRACE_MODULE

typedef struct _csstring_t csstring_t;

#include "csstring.h"

#endif
