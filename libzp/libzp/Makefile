RPATH = /usr/local/zeppelin/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O2 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-sign-compare
endif

ifndef SLASH_PATH
SLASH_PATH = $(realpath ../../third/slash)
endif

ifndef PINK_PATH 
PINK_PATH = $(realpath ../../third/pink)
endif

SRC_DIR := ./src/
INC_DIR := ./include/
OUTPUT := ./lib/

INCLUDE_PATH = -I../ \
							 -I$(SLASH_PATH) \
							 -I$(PINK_PATH)

LIB_PATH = -L$(SLASH_PATH)/pink/lib/ \
					 -L$(PINK_PATH)/slash/lib/ 

LIBS = -lpthread \
			 -lprotobuf \
			 -lpink \
			 -lslash

LIBRARY = libzp.a

.PHONY: all clean distclean

BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

PINK = $(PINK_PATH)/pink/lib/libpink.a
SLASH = $(SLASH_PATH)/slash/lib/libslash.a

all: $(LIBRARY)
	@echo "Success, go, go, go..."

$(LIBRARY): $(PINK) $(SLASH) $(OBJS)
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	rm -rf $@
ifeq ($(__COMBINE), 1)
	cd $(OUTPUT) ; ar x $(PINK) ;ar x $(SLASH)
	ar -rcs $@ $(OBJS) $(OUTPUT)/*.o
	rm -f $(OUTPUT)/*.o
else
	ar -rcs $@ $(OBJS)
endif
	mv $@ $(OUTPUT)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) $(LIBS)

$(SLASH):
	make -C $(SLASH_PATH)/slash/ __PERF=$(__PERF)

$(PINK):
	make -C $(PINK_PATH)/pink/ __PERF=$(__PERF) SLASH_PATH=$(SLASH_PATH)

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)

distclean: clean
	make -C $(SLASH_PATH)/slash/ clean
	make -C $(PINK_PATH)/pink/ clean
