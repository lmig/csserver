#include "csutil.h"
#include "trace.h"
#include "wave.h"
#include "stdlib.h"
#include "memory.h"
#include "sys/stat.h"
#include "errno.h"


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

int str_to_int(char* str, int* error)
{
  char* end;
  int res = strtol(str, &end, 10);

  if (!*end) {
    *error = 0;
  } else {
    *error = -1;
    TRACE (ERROR, "Conversion error, non-convertible part: %s", end);
  }

  return res;
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

void
cs_number_to_string (const Number * const n, BYTE *dest) {
  static const char outputs[] = "0123456789*#+DEF";
  BYTE *ptr = dest;
  int cycles = n->m_uiLen / 2;

  if (n->m_uiLen && n->m_uiLen < (sizeof(n->m_byDigits) * 2)) {
    int cont = 0;
    do {
      unsigned char aByte = (unsigned char) (n->m_byDigits[cont]);
      *dest++ = outputs[(aByte & 0xf0) >> 4];
      *dest++ = outputs[aByte & 0x0f];
      cont++;
    } while (cont <= cycles);
    *(ptr + n->m_uiLen) = 0;
  } else {
    *dest = 0;
  }
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

BYTE *
cs_buffer_to_string (const BYTE * const buffer, int len, BYTE *cstr)
{
  memset(cstr, 0, len);
  memcpy(cstr, buffer, len - 1);
  return cstr;
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

const char *
cs_string_from_indi_call_release_cause (enum IndiCallReleaseCauseEnum n)
{
  static const char *strings[] = {
      "INDI_RELEASE_CAUSE_UNKNOWN",
      "INDI_CAUSE_A_SUB_RELEASE",
      "INDI_CAUSE_B_SUB_RELEASE"
  };
  if (n < 0 || n > 2) return "";
  return (const char *) strings[n];
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

const char *
cs_string_from_group_call_release_cause (enum GroupCallReleaseCauseEnum n)
{
  static const char *strings[] = {
      "GROUPCALL_RELEASE_CAUSE_UNKNOWN",
      "GROUPCALL_PTT_INACTIVITY_TIMEOUT"
  };
  if (n < 0 || n > 1) return "";
  return (const char *) strings[n];
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

const char *
cs_string_from_simplex_ptt (enum SimplexPttEnum n)
{
  static const char *strings[] = {
      "TALKING_PARTY_NONE",
      "TALKING_PARTY_A_SUB",
      "TALKING_PARTY_B_SUB"
  };
  if (n < 0 || n > 2) return "";
  return (const char *) strings[n];
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

const char *
cs_string_from_individual_call_change_action (enum IndividualCallChangeAction n)
{
  static const char *strings[] = {
      "INDI_KEEPALIVEONLY",
      "INDI_NEWCALLSETUP",
      "INDI_CALLTHROUGHCONNECT",
      "INDI_CHANGEOFAORBUSER"
  };
  if (n < 0 || n > 3) return "";
  return (const char *) strings[n];
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

const char *
cs_string_from_group_call_change_action (enum GroupCallChangeAction n)
{
  static const char *strings[] = {
      "GROUPCALL_KEEPALIVEONLY",
      "GROUPCALL_NEWCALLSETUP"
  };
  if (n < 0 || n > 1) return "";
  return (const char *) strings[n];
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

const char *
cs_string_from_stream_originator (enum StreamOriginatorEnum n)
{
  static const char *strings[] = {
      "STREAM_ORG_GROUPCALL",
      "STREAM_ORG_A_SUB",
      "STREAM_ORG_B_SUB"
  };
  if (n < 0 || n > 2) return "";
  return (const char *) strings[n];
}


//  --------------------------------------------------------------------------
//  <Description>
//  <Returns>

void
cs_write_wav_file (const char * const path, const unsigned char * const buffer)
{
  WaveHeader wave_header;
  struct stat file_stat;
  int rc = 0;
  FILE *fp;

  TRACE (FUNCTIONS, "Entering in cs_write_wav_file");

  if (stat (path, &file_stat) == -1) {
    /* File doesn't exist. Generate a new WAVE header for A-law */
    wave_header.riffId[0] = 'R';
    wave_header.riffId[1] = 'I';
    wave_header.riffId[2] = 'F';
    wave_header.riffId[3] = 'F';
    wave_header.riffSize = 4 + 26 + 12 + 8;
    wave_header.waveId[0] = 'W';
    wave_header.waveId[1] = 'A';
    wave_header.waveId[2] = 'V';
    wave_header.waveId[3] = 'E';

    wave_header.fmtId[0] = 'f';
    wave_header.fmtId[1] = 'm';
    wave_header.fmtId[2] = 't';
    wave_header.fmtId[3] = ' ';
    wave_header.fmtSize = 18;
    wave_header.wFormatTag = 6; /* A-law */
    wave_header.nChannels = 1;
    wave_header.nSamplesPerSec = 8000;
    wave_header.nAvgBytesperSec = 8000;
    wave_header.nBlockAlign = 1;
    wave_header.wBitsPerSample = 8;
    wave_header.cbSize = 0;

    wave_header.factId[0] = 'f';
    wave_header.factId[1] = 'a';
    wave_header.factId[2] = 'c';
    wave_header.factId[3] = 't';
    wave_header.factSize = 4;
    wave_header.dwSampleLength = 0;

    wave_header.dataId[0] = 'd';
    wave_header.dataId[1] = 'a';
    wave_header.dataId[2] = 't';
    wave_header.dataId[3] = 'a';
    wave_header.dataSize = 0;

    TRACE (DEBUG, "WAVE header size: %d", sizeof (wave_header));

    if ((fp = fopen (path, "w")) == NULL) {
      TRACE (ERROR, "Error: fopen(), errno=%d text=%s", errno, strerror(errno));
      rc = -1;
    }

    if (rc == 0) {
      if (fwrite (&wave_header, 1, sizeof (wave_header), fp) != sizeof (wave_header)) {
        TRACE (ERROR, "Error: fwrite(), errno=%d text=%s", errno, strerror (errno));
        rc = -1;
      }

      fclose (fp);
      fp = NULL;
    }
  }

  if (rc == 0) {
    if ((fp = fopen (path, "a")) == NULL) {
      TRACE(ERROR, "Error: fopen(), errno=%d text=%s", errno, strerror (errno));
      rc = -1;
    }

    if (rc == 0) {
      if (fwrite (buffer + 20, 1, 480, fp) != 480) {
        TRACE (ERROR, "Error: fwrite(), errno=%d text=%s", errno, strerror (errno));
        rc = -1;
      }

      fclose (fp);
      fp = NULL;
    }
  }

  if (rc == 0) {
    /* Update the WAVE header */
    if ((fp = fopen (path, "r+")) == NULL) {
      TRACE(ERROR, "Error: fopen(), errno=%d text=%s", errno, strerror(errno));
      rc = -1;
    }

    if (rc == 0) {
      if (fread (&wave_header, 1, sizeof (wave_header), fp) != sizeof (wave_header)) {
        TRACE (ERROR, "Error: fread(), errno=%d text=%s", errno, strerror (errno));
        rc = -1;
      }

      if (rc == 0) {
        wave_header.riffSize += 480;
        wave_header.dwSampleLength += 480;
        wave_header.dataSize += 480;

        if (fseek (fp, 0, SEEK_SET) != 0) {
          TRACE (ERROR, "Error: fseek(), errno=%d text=%s", errno, strerror(errno));
          rc = -1;
        }

        if (fwrite (&wave_header, 1, sizeof (wave_header), fp) != sizeof (wave_header)) {
          TRACE (ERROR, "Error: fwrite(), errno=%d text=%s", errno, strerror (errno));
          rc = -1;
        }
      }

      fclose (fp);
      fp = NULL;
    }
  }

  TRACE (FUNCTIONS, "Leaving cs_write_wav_file");
}

