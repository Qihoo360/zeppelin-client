CXX=g++
CC=gcc
LDFLAGS= -L/usr/local/protobuf/lib -lpthread -lrt -lprotobuf
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2
CFLAGS= -g -fno-builtin-memcmp -msse -msse4.2
PROFILING_FLAGS=-pg
ARFLAGS = rs
OPT=

# Set the default DEBUG_LEVEL to 0
DEBUG_LEVEL?=0

ifeq ($(MAKECMDGOALS),dbg)
  DEBUG_LEVEL=2 # compatible with rocksdb
endif

# compile with -O2 if for release
# if we're compiling for release, compile without debug code (-DNDEBUG) and
# don't treat warnings as errors
ifeq ($(DEBUG_LEVEL),0)
DISABLE_WARNING_AS_ERROR=1
OPT += -O2 -fno-omit-frame-pointer -DNDEBUG
else
$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
OPT += -O0 -D__XDEBUG__ $(PROFILING_FLAGS)
DEBUG_SUFFIX = "_debug"
endif

# ----------------Dependences-------------------
ifndef ZP_PATH 
  ZP_PATH = $(CURDIR)/../libzp
endif
ZP_INCLUDE_DIR=$(ZP_PATH)
ZP_LIBRARY=$(ZP_PATH)/libzp/lib/libzp$(DEBUG_SUFFIX).a

ifndef PINK_PATH
  PINK_PATH=$(CURDIR)/../third/pink
endif
PINK_INCLUDE_DIR=$(PINK_PATH)
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink$(DEBUG_SUFFIX).a

ifndef SLASH_PATH
	SLASH_PATH=$(CURDIR)/../third/slash
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash$(DEBUG_SUFFIX).a

ifeq ($(MAKECMDGOALS),rocksdb_to_zp)
ifndef ROCKSDB_PATH
  $(error Error: missing rocksdb path!)
endif
ROCKSDB_INCLUDE_DIR=$(ROCKSDB_PATH)/include
ROCKSDB_LIBRARY=$(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).a
LDFLAGS += -lz -lbz2 -lsnappy
endif
# ----------------------------------------------

CXXFLAGS+= -I$(PINK_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) -I$(ZP_INCLUDE_DIR)
DEP_LIBS = $(ZP_LIBRARY) $(PINK_LIBRARY) $(SLASH_LIBRARY)

AM_DEFAULT_VERBOSITY = 0

AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =
AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC     " $@;
am__v_CC_1 =
AM_V_LD = $(am__v_LD_$(V))
am__v_LD_ = $(am__v_LD_$(AM_DEFAULT_VERBOSITY))
am__v_LD_0 = @echo "  LD      " $@;
am__v_LD_1 =

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  -Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings -Wpointer-arith 

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

CXXFLAGS += $(WARNING_FLAGS) $(OPT) -Wreorder -Wswitch -Wsign-promo -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers
CFLAGS += $(WARNING_FLAGS) $(OPT)

.PHONY: clean distclean dbg all deps

all: zp_benchmark zp_checkup zp_expansion zp_info zp_manager zp_parade zp_dprd_test

dbg: zp_benchmark zp_checkup zp_expansion zp_info zp_manager zp_parade zp_dprd_test

$(DEP_LIBS):
	$(AM_V_at)make -C $(SLASH_PATH)/slash DEBUG_LEVEL=$(DEBUG_LEVEL)
	$(AM_V_at)make -C $(PINK_PATH)/pink DEBUG_LEVEL=$(DEBUG_LEVEL) SLASH_PATH=$(SLASH_PATH)
	$(AM_V_at)make -C $(ZP_PATH)/libzp DEBUG_LEVEL=$(DEBUG_LEVEL) SLASH_PATH=$(SLASH_PATH) PINK_PATH=$(PINK_PATH)

zp_benchmark: zp_benchmark.cc $(DEP_LIBS)
	$(AM_V_LD)$(CXX) $^ -o $@ $(CXXFLAGS) $(LDFLAGS)

zp_checkup: zp_checkup.cc utils/json.cc utils/json.h $(DEP_LIBS)
	$(AM_V_LD)$(CXX) $^ -o $@ $(CXXFLAGS) $(LDFLAGS)

zp_expansion: zp_expansion.cc $(DEP_LIBS)
	$(AM_V_LD)$(CXX) $^ -o $@ $(CXXFLAGS) $(LDFLAGS)

zp_info: zp_info.cc utils/json.cc utils/json.h $(DEP_LIBS)
	$(AM_V_LD)$(CXX) $^ -o $@ $(CXXFLAGS) $(LDFLAGS)

zp_manager: zp_manager.cc utils/linenoise.o utils/distribution.o $(DEP_LIBS)
	$(AM_V_LD)$(CXX) $^ -o $@ $(CXXFLAGS) $(LDFLAGS)

zp_dprd_test: utils/dprd_test.cc utils/wrapper.o utils/dprd_type.o $(DEP_LIBS)
	$(AM_V_LD)$(CXX) $^ -o $@ $(CXXFLAGS) $(LDFLAGS)

utils/linenoise.o: utils/linenoise.c utils/linenoise.h
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

utils/distribution.o: utils/distribution.cc
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

utils/dprd_type.o: utils/dprd_type.cc
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

utils/wrapper.o: utils/wrapper.cc
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

zp_parade: zp_parade.cc $(DEP_LIBS)
	$(AM_V_LD)$(CXX) $^ -o $@ $(CXXFLAGS) $(LDFLAGS)

rocksdb_to_zp: rocksdb_to_zp.cc $(DEP_LIBS) $(ROCKSDB_LIBRARY)
	$(AM_V_LD)$(CXX) $^ -o $@ $(CXXFLAGS) -I$(ROCKSDB_INCLUDE_DIR) $(LDFLAGS)

clean:
	rm -f zp_benchmark zp_checkup zp_expansion zp_info zp_manager zp_parade rocksdb_to_zp zp_dprd_test
	find . -name "*.[oda]*" ! -exec rm -f {} \;
	find . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

distclean: clean
	$(AM_V_at)make -C $(SLASH_PATH)/slash clean
	$(AM_V_at)make -C $(PINK_PATH)/pink clean
	$(AM_V_at)make -C $(ZP_PATH)/libzp clean
