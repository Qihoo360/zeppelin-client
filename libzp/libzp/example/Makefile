CLEAN_FILES = # deliberately empty, so we can append below.
CXX = g++
LDFLAGS= -lpthread -lrt -lprotobuf
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2
PROFILING_FLAGS = -pg
ARFLAGS = rs
OPT=

.PHONY: clean all

all: zp_simple zp_timeout zp_get zp_mget zp_async zp_table zp_hashtag

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

zp_simple: zp_simple.cc $(DEP_LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_timeout: zp_timeout.cc $(DEP_LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_get: zp_get.cc $(DEP_LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_mget: zp_mget.cc $(DEP_LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# zp_create_table: zp_create_table.cc $(DEP_LIBS)
# 	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_async: zp_async.cc $(DEP_LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_table: zp_table.cc $(DEP_LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

zp_hashtag: zp_hashtag.cc $(DEP_LIBS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# zp_parallel: zp_parallel.cc $(DEP_LIBS)
# 	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean: 
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./zp_simple ./zp_timeout ./zp_get ./zp_mget ./zp_create_table ./zp_async ./zp_parallel ./zp_table ./zp_hashtag
