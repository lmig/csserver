DIR_CPPFLAGS= \
  -I/usr/local/iap/postgresql/include -I$(TOP_PACKAGES)/zeromq=4_0_5-Linux/include -I$(TOP_PACKAGES)/czmq=3_0_0-Linux/include
  
MYLIBS = \
  -L$(TOP_PACKAGES)/czmq=3_0_0-Linux/lib -L$(TOP_PACKAGES)/zeromq=4_0_5-Linux/lib -lczmq -lzmq -luuid  \
  -L/usr/local/iap/postgresql/lib -lpq -lmd5

