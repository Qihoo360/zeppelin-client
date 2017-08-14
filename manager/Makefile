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

CXXFLAGS+= -I$(PINK_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) -I$(ZP_INCLUDE_DIR)
LDFLAGS+= $(ZP_LIBRARY) $(PINK_LIBRARY) $(SLASH_LIBRARY)

AM_DEFAULT_VERBOSITY = 0

AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC     " $@;
am__v_CC_1 =
AM_V_CXX = $(am__v_CXX_$(V))
am__v_CXX_ = $(am__v_CXX_$(AM_DEFAULT_VERBOSITY))
am__v_CXX_0 = @echo "  CXX     " $@;
am__v_CXX_1 =
AM_V_LD = $(am__v_LD_$(V))
am__v_LD_ = $(am__v_LD_$(AM_DEFAULT_VERBOSITY))
am__v_LD_0 = @echo "  LD      " $@;
am__v_LD_1 =

AM_LINK = $(AM_V_LD)$(CXX) $^ -o $@ $(LDFLAGS)

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  -Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings -Wpointer-arith 

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

CXXFLAGS += $(WARNING_FLAGS) $(OPT) -Wreorder -Wswitch -Wsign-promo -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers
CFLAGS += $(WARNING_FLAGS) $(OPT)

.PHONY: clean dbg all

all: zp_manager

dbg: zp_manager

zp_manager: zp_manager.o linenoise.o
	$(AM_LINK)

zp_manager.o: zp_manager.cc zp_manager_help.h linenoise.h
	$(AM_V_CXX)$(CXX) $(CXXFLAGS) -c $< -o $@

linenoise.o: linenoise.c linenoise.h
	$(AM_V_CC)$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f zp_manager
	find . -name "*.[oda]*" ! -path "./third/*" -exec rm -f {} \;
	find . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;
