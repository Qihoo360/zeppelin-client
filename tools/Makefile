RPATH = /usr/local/zeppelin/lib/
CXX=g++
LDFLAGS= -Wl,-rpath=$(RPATH)
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2
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
  $(warning Warning: missing zp path, using default)
  ZP_PATH = $(CURDIR)/../libzp
endif
ZP_INCLUDE_DIR=$(ZP_PATH)
ZP_LIBRARY=$(ZP_PATH)/libzp/lib/libzp$(DEBUG_SUFFIX).a

ifndef PINK_PATH
  $(warning Warning: missing pink path, using default)
  PINK_PATH=$(CURDIR)/../third/pink
endif
PINK_INCLUDE_DIR=$(PINK_PATH)
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink$(DEBUG_SUFFIX).a

ifndef SLASH_PATH
  $(warning Warning: missing slash path, using default)
  SLASH_PATH=$(CURDIR)/../third/slash
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash$(DEBUG_SUFFIX).a

ifneq ($(MAKECMDGOALS),clean)
ifndef ROCKSDB_PATH
  $(error Error: missing rocksdb path!)
endif
ROCKSDB_INCLUDE_DIR=$(ROCKSDB_PATH)/include
ROCKSDB_LIBRARY=$(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).a
endif

CXXFLAGS+= -I$(PINK_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) -I$(ZP_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR)
LDFLAGS+= $(ZP_LIBRARY) $(PINK_LIBRARY) $(SLASH_LIBRARY) $(ROCKSDB_LIBRARY)

AM_DEFAULT_VERBOSITY = 0

AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =

AM_V_LD = $(am__v_LD_$(V))
am__v_LD_ = $(am__v_LD_$(AM_DEFAULT_VERBOSITY))
am__v_LD_0 = @echo "  LD      " $@;
am__v_LD_1 =

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  -Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings -Wpointer-arith \
	-Wreorder -Wswitch -Wsign-promo -Woverloaded-virtual -Wnon-virtual-dtor \
	-Wno-missing-field-initializers

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

CXXFLAGS += $(WARNING_FLAGS) $(OPT)
LDFLAGS += -lprotobuf -lpthread -lz -lbz2 -lsnappy -lrt

.PHONY: clean dbg all

BINARIES = zp_parade zp_benchmark zp_expansion rocksdb_to_zp
OUTPUT=$(CURDIR)/output

all: $(BINARIES)
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)mkdir -p $(OUTPUT)
	$(AM_V_at)mkdir -p $(OUTPUT)/bin
	$(AM_V_at)mkdir -p $(OUTPUT)/log
	cp $(BINARIES) $(OUTPUT)/bin/
	$(AM_V_at)rm -rf $(BINARIES)
	$(AM_V_at)echo "Success, go, go, go..."

dbg: $(BINARIES)

rocksdb_to_zp: rocksdb_to_zp.cc
	$(AM_V_LD)$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

zp_parade: zp_parade.cc
	$(AM_V_LD)$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

zp_benchmark: zp_benchmark.cc
	$(AM_V_LD)$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

zp_expansion: zp_expansion.cc
	$(AM_V_LD)$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(BINARIES)
	rm -rf $(OUTPUT)
	find . -name "*.[oda]*" ! -path "./third/*" -exec rm -f {} \;
	find . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;
