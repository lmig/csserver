#ifndef _WAVE_H
#define _WAVE_H

#pragma pack(1)

typedef struct WaveHeader
{                                                                              
    char riffId[4];
    unsigned int riffSize;
    char waveId[4];

    char fmtId[4];
    unsigned int fmtSize;
    unsigned short wFormatTag;
    unsigned short nChannels;
    unsigned int nSamplesPerSec;
    unsigned int nAvgBytesperSec;
    unsigned short nBlockAlign;
    unsigned short wBitsPerSample;
    unsigned short cbSize;

    char factId[4];
    unsigned int factSize;
    unsigned int dwSampleLength;

    char dataId[4];
    unsigned int dataSize;
} WaveHeader;

#endif

