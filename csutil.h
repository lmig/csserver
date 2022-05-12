#ifndef __CSUTIL_H_INCLUDED__
#define __CSUTIL_H_INCLUDED__

#include "LogApiMsgDef.h"

#ifdef __cplusplus
extern "C" {
#endif


int
str_to_int(char* str, int* error);

void
cs_number_to_string (const Number * const n, BYTE *dest);

BYTE *
cs_buffer_to_string (const BYTE * const buffer, int len, BYTE *cstr);

const char *
cs_string_from_indi_call_release_cause (enum IndiCallReleaseCauseEnum n);

const char *
cs_string_from_group_call_release_cause (enum GroupCallReleaseCauseEnum n);

const char *
cs_string_from_simplex_ptt (enum SimplexPttEnum n);

const char *
cs_string_from_individual_call_change_action (enum IndividualCallChangeAction n);

const char *
cs_string_from_group_call_change_action (enum GroupCallChangeAction n);

const char *
cs_string_from_stream_originator (enum StreamOriginatorEnum n);

void
cs_write_wav_file (const char * const path, const unsigned char * const buffer);


#ifdef __cplusplus
}
#endif

#endif
