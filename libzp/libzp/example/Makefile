RPATH = /usr/local/zeppelin/lib/
CLEAN_FILES = # deliberately empty, so we can append below.
CXX = g++
LDFLAGS= -Wl,-rpath=$(RPATH) -lpthread -lrt -lprotobuf
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2
PROFILING_FLAGS = -pg
ARFLAGS = rs
OPT=

.PHONY: clean all

all: zp_simple zp_timeout zp_get zp_mget zp_create_table zp_async zp_parallel

ifndef ZP_PATH 
  $(warning Warning: missing zp path, using default)
  ZP_PATH = $(CURDIR)/../..
endif
ZP_INCLUDE_DIR=$(ZP_PATH)
ZP_LIBRARY=$(ZP_PATH)/libzp/lib/libzp.a

ifndef PINK_PATH
  $(warning Warning: missing pink path, using default)
  PINK_PATH=$(CURDIR)/../../../third/pink
endif
PINK_INCLUDE_DIR=$(PINK_PATH)
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink.a

ifndef SLASH_PATH
  $(warning Warning: missing slash path, using default)
	SLASH_PATH=$(CURDIR)/../../../third/slash
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a

CXXFLAGS+= -I$(PINK_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) -I$(ZP_INCLUDE_DIR)
DEP_LIBS = $(ZP_LIBRARY) $(PINK_LIBRARY) $(SLASH_LIBRARY)

LDFLAGS+= $(DEP_LIBS)

zp_simple: $(ZP) zp_simple.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_timeout: $(ZP) zp_timeout.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_get: $(ZP) zp_get.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_mget: $(ZP) zp_mget.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_create_table: $(ZP) zp_create_table.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_async: $(ZP) zp_async.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_parallel: $(ZP) zp_parallel.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean: 
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./zp_simple ./zp_timeout ./zp_get ./zp_mget ./zp_create_table ./zp_async ./zp_parallel
