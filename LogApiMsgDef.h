//*****************************************************************************
//***                                                                       *** 
//***                         DAMM TetraFlex LogApi                         ***                 
//***                                                                       *** 
//*****************************************************************************
// Copyright 2009-2014 Damm Cellular Systems A/S, Denmark
// This file version was first released with LogServer 7.60
// See the formal API version date in the definition below
//
//
//******************************************************
//***                  Introduction                  ***
//******************************************************
//
// This file contains the definitions for the DAMM TetraFlex LOG-API 
// for LogApiClient development. The interface is pure IP based,
// any programming language and operation system can be used.
// 
// The LogApi is an UDP-interface, which outputs CDR-, voice and SDS data.
// A maximum of 2 receiving LogApiClient IP addresses can be setup.
// 
// Every UDP message contains a common message header.
// The byte order is Little Endian.
// 
// Supported features:
// + Status messages, SDS-Type4 (without TL), SDS-TL TRANSFER
// + Individual calls (Duplex/Simplex)
// + Group calls (Normal/Broadcast)
// + Streams of active voice calls either as G.711 A-LAW or Tetra encoded TCH/S
//   streams.
//

#pragma once

#define LOG_API_VERSION_DATE        20111109
#define LOG_API_VERSION             1
#define LOG_API_PROTOCOL_SIGNATURE  0x31474F4C
#define VOICE_PROTOCOL_SIGNATURE    0x32474F4C

#define BYTE char
#define UINT8 unsigned char
#define UINT16 unsigned short
#define UINT32 unsigned int

enum LogApiMsgType
{
  LOG_API_ALIVE                   = 0x01,

  // Individual Duplex Call Messages
  LOG_API_DUPLEX_CALL_CHANGE      = 0x10,
  LOG_API_DUPLEX_CALL_RELEASE     = 0x19,

  // Individual Simplex Call Messages
  LOG_API_SIMPLEX_CALL_CHANGE     = 0x20,
  LOG_API_SIMPLEX_CALL_PTT_CHANGE = 0x21,
  LOG_API_SIMPLEX_CALL_RELEASE    = 0x29,

  // Group Call messages
  LOG_API_GROUP_CALL_CHANGE       = 0x30,
  LOG_API_GROUP_CALL_PTT_ACTIVE   = 0x31,  
  LOG_API_GROUP_CALL_PTT_IDLE     = 0x32,
  LOG_API_GROUP_CALL_RELEASE      = 0x39,

  // SDS Messages
  LOG_API_SDS_STATUS              = 0x40,
  LOG_API_SDS_TEXT                = 0x41
};

enum IndiCallReleaseCauseEnum
{
  INDI_RELEASE_CAUSE_UNKNOWN = 0,
  INDI_CAUSE_A_SUB_RELEASE,
  INDI_CAUSE_B_SUB_RELEASE
};

typedef UINT8 IndiCallReleaseCauseEnum;

enum GroupCallReleaseCauseEnum
{
  GROUPCALL_RELEASE_CAUSE_UNKNOWN = 0,
  GROUPCALL_PTT_INACTIVITY_TIMEOUT
};

typedef UINT8 GroupCallReleaseCauseEnum;

enum SimplexPttEnum
{
  TALKING_PARTY_NONE = 0,
  TALKING_PARTY_A_SUB,
  TALKING_PARTY_B_SUB
};

typedef UINT8 SimplexPttEnum;

enum StreamOriginatorEnum
{
  STREAM_ORG_GROUPCALL = 0,
  STREAM_ORG_A_SUB,
  STREAM_ORG_B_SUB
};

typedef UINT8 StreamOriginatorEnum;


enum DetailedDiscReasonEnum
{
  DETAILED_DISC_REASON_NOT_PRESENT = 0,                                        // No detailed reason present
  DETAILED_DISC_REASON_SS_CAD_REJECTED,                                        // CAD Reject, the call request was rejected by the dispatcher
  DETAILED_DISC_REASON_SS_CAD_CANCELLED                                        // CAD Cancel, e.g. the call request was canceled before the dispatcher responded to it (either by MS or by timer)
};

typedef UINT8 DetailedDiscReasonEnum;


enum IndividualCallChangeAction
{
  INDI_KEEPALIVEONLY       = 0,
  INDI_NEWCALLSETUP        = 1,
  INDI_CALLTHROUGHCONNECT  = 2,
  INDI_CHANGEOFAORBUSER    = 3
};

typedef UINT8 IndividualCallChangeAction;

enum GroupCallChangeAction
{
  GROUPCALL_KEEPALIVEONLY    = 0,
  GROUPCALL_NEWCALLSETUP     = 1
};

typedef UINT8 GroupCallChangeAction;


enum PayloadInfo
{
  PAYLOAD_INFO_NONE = 0,                                                       // 0: No payload included  (  0 bytes)
  PAYLOAD_INFO_TETRA_STCH_U = 1,                                               // 1: TETRA STCH/U         ( 16 bytes)
  PAYLOAD_INFO_TETRA_TCH_S  = 2,                                               // 2: TETRA TCH/S          ( 18 bytes)
  PAYLOAD_INFO_TETRA_TCH7_2 = 3,                                               // 3: TETRA TCH/7.2        ( 27 bytes)
  PAYLOAD_INFO_TETRA_TCH4_8 = 4,                                               // 4: TETRA TCH/4.8        ( 18 bytes)
  PAYLOAD_INFO_TETRA_TCH2_4 = 5,                                               // 5: TETRA TCH/2.4        (  9 bytes)
  PAYLOAD_INFO_G711         = 7                                                // 7: G.711 A-Law          (480 bytes, and no Payload 2 information)
};

typedef UINT8 PayloadInfo;
                                                                               
                                                                               
typedef struct TSI                                                                     // TETRA Subscriber Identity (TSI)
{                                                                              
  UINT16 Mcc;                                                                  // 00..01 : Mobile Country Code (MCC) (10 bit) 0 - MCC of the own home network
  UINT16 Mnc;                                                                  // 02..03 : Mobile Network Code (MNC) (14 bit) 0 - MNC of the own home network
  UINT32 Ssi;                                                                  // 04..07 : Short Subscriber Identity (SSI) (24 bit) (0..16777215)
} TSI;
                                                                               
typedef struct Number                                                                  
{                                                                              
  //  Number();                                                                    
  //  ~Number() {}                                                                 
  UINT8 m_uiLen;                                                               
  BYTE  m_byDigits[15];                                                        
} Number;
                                                                               
struct TetraFlexLogApiAddress                                                  
{                                                                              
    UINT8                           TypeOfAddress;                             // 00     : Type of address: 0= TSI, 1= User Number
    UINT8                           Spare1[3];                                 // 01..03 : (Spare)
    //*** Tetra Subscriber Identity (TSI) ***                                  
    struct                                                                     // 04..11 : TETRA Subscriber Identity (TSI)
    {                                                                          
      UINT32 Ssi;                                                              // 04..07 : Short Subscriber Identity (SSI) (24 bit) (0..16777215)
      UINT16 Mnc;                                                              // 08..09 : Mobile Network Code (MNC) (14 bit) 0 - MNC of the own home network
      UINT16 Mcc;                                                              // 10..11 : Mobile Country Code (MCC) (10 bit) 0 - MCC of the own home network
    } TetraSubscriberIdentity;                                                 
                                                                               
    //*** User Number ***                                                      
    struct                                                                     // 12..39:  User Number
    {                                                                          
        UINT8 NumberOfDigits;                                                  // 12     : Number of digits
        UINT8 Spare5[3];                                                       // 13..15 : (Spare, to align the both unions and to make Digit[] start at a valid Word-address)   
        UINT8 Digit[24];                                                       // 16..39 : Digits (ASCII: '0'..'9', '*', '#' or '+')
    } UserNumber;                                                              
};                                                                             
                                                                               
                                                                               
//Common message header in every UDP message:                                  
typedef struct TetraFlexLogApiMessageHeader                                            
{                                                                              
  UINT32  ProtocolSignature;                                                   //  0..3  : Protocol signature is 31474F4Ch
  UINT16  SequenceCounter;                                                     //  4..5  : Increments with 1 for ech new information message
  UINT8   ApiVersion;                                                          //     6 :  API Protocol Version
  UINT8   MsgId;                                                               //     7 :  Message ID
} TetraFlexLogApiMessageHeader;



//
// LogApi KeepAlive
//
typedef struct LogApiKeepAlive
{
  //  LogApiKeepAlive();
  //  ~LogApiKeepAlive() {}
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  UINT8                         m_uiLogServerNo;                               
  UINT8                         m_uiTimeout;                                   
  UINT8                         m_uiSpare1;                                    
  UINT8                         m_uiSpare2;                                    
  UINT32                        m_uiSpare3;                                    
  BYTE                          m_bySwVer[4];                                  
  BYTE                          m_bySwVerString[20];                           
  BYTE                          m_byLogServerDescr[64];                        
} LogApiKeepAlive;
                                                                               
                                                                               
typedef struct LogApiDuplexCallChange                                                  
{                                                                              
  //  LogApiDuplexCallChange();                                                    
  //  ~LogApiDuplexCallChange() {}                                                 
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  UINT32                        m_uiCallId;
  IndividualCallChangeAction    m_uiAction;
  UINT8                         m_uiTimeout;
  UINT8                         m_uiSpare1;
  UINT8                         m_uiSpare2;

  TSI                           m_A_Tsi;
  Number                        m_A_Number;
  BYTE                          m_A_Descr[64];

  TSI                           m_B_Tsi;
  Number                        m_B_Number;
  BYTE                          m_B_Descr[64];
} LogApiDuplexCallChange;



typedef struct LogApiDuplexCallRelease
{
  //  LogApiDuplexCallRelease();
  //  ~LogApiDuplexCallRelease() {}
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 

  UINT32                        m_uiCallId;
  IndiCallReleaseCauseEnum      m_uiReleaseCause;
  UINT8                         m_uiSpare1;
  UINT8                         m_uiSpare2;
  UINT8                         m_uiSpare3;
} LogApiDuplexCallRelease;




typedef struct LogApiSimplexCallStartChange
{
  //  LogApiSimplexCallStartChange();
  //  ~LogApiSimplexCallStartChange() {}

  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  UINT32                        m_uiCallId;                                    
  IndividualCallChangeAction    m_uiAction;                                    
  UINT8                         m_uiTimeoutValue;                              
  UINT8                         m_uiSpare1;                                    
  UINT8                         m_uiSpare2;                                    
                                                                               
  TSI                           m_A_Tsi;                                       
  Number                        m_A_Number;                                    
  BYTE                          m_A_Descr[64];                                 
                                                                               
  TSI                           m_B_Tsi;                                       
  Number                        m_B_Number;                                    
  BYTE                          m_B_Descr[64];                                 
} LogApiSimplexCallStartChange;                                                                             
                                                                               
                                                                               
typedef struct LogApiSimplexCallPttChange                                              
{                                                                              
  //  LogApiSimplexCallPttChange();                                                
  //  ~LogApiSimplexCallPttChange() {}                                             
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  UINT32                        m_uiCallId;                                    
  SimplexPttEnum                m_uiTalkingParty;                              // 0: None, 1: A, 2: B
  UINT8                         m_uiSpare1;
  UINT8                         m_uiSpare2;
  UINT8                         m_uiSpare3;
} LogApiSimplexCallPttChange;

typedef struct LogApiSimplexCallRelease
{
  //  LogApiSimplexCallRelease();
  //  ~LogApiSimplexCallRelease() {}
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  UINT32                        m_uiCallId;                                    
  IndiCallReleaseCauseEnum      m_uiReleaseCause;                              // 0: , 1: A, 2: B
  UINT8                         m_uiSpare1;                                    
  UINT8                         m_uiSpare2;                                    
  UINT8                         m_uiSpare3;                                    
} LogApiSimplexCallRelease;                                                                             
                                                                               
                                                                               
typedef struct LogApiGroupCallStartChange                                              
{                                                                              
  //  LogApiGroupCallStartChange();                                                
  //  ~LogApiGroupCallStartChange() {}                                             
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 

  UINT32                        m_uiCallId;
  GroupCallChangeAction         m_uiAction;
  UINT8                         m_uiTimeoutValue;
  UINT8                         m_uiSpare1;
  UINT8                         m_uiSpare2;

  TSI                           m_Group_Tsi;
  Number                        m_Group_Number;
  BYTE                          m_Group_Descr[64];
} LogApiGroupCallStartChange;


typedef struct LogApiGroupCallPttActive
{
  //  LogApiGroupCallPttActive();
  //  ~LogApiGroupCallPttActive() {}
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  UINT32                        m_uiCallId;                                    
  UINT32                        m_uiSpare4;                                    
                                                                               
  TSI                           m_TP_Tsi;                                      
  Number                        m_TP_Number;                                   
  BYTE                          m_TP_Descr[64];                                
} LogApiGroupCallPttActive;                                                                             
                                                                               
                                                                               
typedef struct LogApiGroupCallPttIdle                                                  
{                                                                              
  //  LogApiGroupCallPttIdle();                                                    
  //  ~LogApiGroupCallPttIdle() {}                                                 
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 

  UINT32                        m_uiCallId;
  UINT32                        m_uiSpare4;
} LogApiGroupCallPttIdle;


typedef struct LogApiGroupCallRelease 
{
  //  LogApiGroupCallRelease();
  //  ~LogApiGroupCallRelease() {}
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  UINT32                        m_uiCallId;                                    
  GroupCallReleaseCauseEnum     m_uiReleaseCause;                              
  UINT8                         m_uiSpare1;                                    
  UINT8                         m_uiSpare2;                                    
  UINT8                         m_uiSpare3;                                    
} LogApiGroupCallRelease;                                                                             
                                                                               
typedef struct LogApiStatusSDS                                                         
{                                                                              
  //  LogApiStatusSDS();                                                           
  //  ~LogApiStatusSDS() {}                                                        
                                                                               
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  TSI                           m_A_Tsi;                                       
  Number                        m_A_Number;                                    
  BYTE                          m_A_Descr[64];                                 
  TSI                           m_B_Tsi;                                       
  Number                        m_B_Number;                                    
  BYTE                          m_B_Descr[64];                                 
  UINT16                        m_uiPrecodedStatusValue;                       
} LogApiStatusSDS;                                                                             
                                                                               
typedef struct LogApiTextSDS                                                           
{                                                                              
  //  LogApiTextSDS();                                                             
  //  ~LogApiTextSDS() {}                                                          
                                                                               
  TetraFlexLogApiMessageHeader  Header;                                        // 00..07 
                                                                               
  TSI                           m_A_Tsi;                                       
  Number                        m_A_Number;                                    
  BYTE                          m_A_Descr[64];                                 
  TSI                           m_B_Tsi;                                       
  Number                        m_B_Number;                                    
  BYTE                          m_B_Descr[64];                                 
                                                                               
  BYTE                          m_TextData[512];                               
} LogApiTextSDS;                                                                             
                                                                               
                                                                               
typedef struct LogApiVoice                                                             
{                                                                              
  //  LogApiVoice();                                                               
  //  ~LogApiVoice() {}                                                            
                                                                               // Byte     Short Descr
  UINT32                        m_uiProtocolSignature;                         // 00..03
  UINT8                         m_uiApiProtocolVersion;                        // 04
  StreamOriginatorEnum          m_uiStreamOriginator;                          // 05       0: Default(GroupCall), 1: A-Sub (IndiCall), 2: B-Sub (IndiCall)
  UINT16                        m_uiOriginatingNode;                           // 06..07   Node Number 1..999
  UINT32                        m_uiCallId;                                    // 08..11   Call Identifier
  UINT16                        m_uiSourceAndIndex;                            // 12..13   B15....: Reserved
                                                                               //          B14..11: Source Kind: 0:Radio, 1: VGW, 2: AppGW, 3: TerminalGW
                                                                               //          B10..00: Source index: Radio: TS Index, GW: Connection Index
  UINT16                        m_uiStreamRandomId;                            // 14..15   Random Number generated by Source. Used if Destination to detect switch to new stream.
  UINT8                         m_uiPacketSeq;                                 // 16       B7.....: 0=First 128 pckts, 1=Following pckts
                                                                               //          B6..B0 : Sequence number(0..127 cyclic, 0 for first packet)
  UINT8                         m_uiSpare1;                                    // 17       Spare
  PayloadInfo                   m_uiPayload1Info;                              // 18       Payload 1 information, 
                                                                               //          0: No payload included  (  0 bytes)
                                                                               //          1: TETRA STCH/U         ( 16 bytes)
                                                                               //          2: TETRA TCH/S          ( 18 bytes)
                                                                               //          3: TETRA TCH/7.2        ( 27 bytes)
                                                                               //          4: TETRA TCH/4.8        ( 18 bytes)
                                                                               //          5: TETRA TCH/2.4        (  9 bytes)
                                                                               //          7: G.711 A-Law          (240 bytes)
  PayloadInfo                   m_uiPayload2Info;                              // 19       Payload 2 information, see Payload 1 information description.
                                                                               // 20..n    Optional Payload 1 data
                                                                               
                                                                               //  n..n    Optional Payload 2 data
} LogApiVoice;

/* Linea administrativa CVS */
/* $Id: LogApiMsgDef.h,v 1.1 2015/02/03 17:21:13 lmibanez Exp $ */
