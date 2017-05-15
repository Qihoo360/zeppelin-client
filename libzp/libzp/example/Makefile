RPATH = /usr/local/zeppelin/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

CXX = g++
ifeq ($(__PERF), 1)
CXXFLAGS = -O2 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-unused-variable
else
	CXXFLAGS = -O2 -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-unused-variable
endif

OBJECT = zp_mget zp_create_table
SRC_DIR = ./
OUTPUT = ./output

ifndef ZP_PATH 
ZP_PATH = $(realpath ../..)
endif

ifndef SLASH_PATH
SLASH_PATH = $(realpath ../../../third/slash)
endif

ifndef PINK_PATH 
PINK_PATH = $(realpath ../../../third/pink)
endif

ZP = $(ZP_PATH)/libzp/lib/libzp.a

LIB_PATH = -L$(ZP_PATH)/libzp/lib/

LIBS = -lzp \
			 -lprotobuf \
			 -lpthread

INCLUDE_PATH = -I$(ZP_PATH) \
							 -I$(PINK_PATH) \
							 -I$(SLASH_PATH)

.PHONY: all clean

all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir -p $(OUTPUT)
	mkdir -p $(OUTPUT)/bin
	cp $(OBJECT) $(OUTPUT)/bin/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."

zp_mget: $(ZP) zp_mget.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

zp_create_table: $(ZP) zp_create_table.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

$(ZP):
	make -C __COMBINE=1 $(ZP_PATH)/libzp/ __PERF=$(__PERF) SLASH_PATH=$(SLASH_PATH) PINK_PATH=$(PINK_PATH)

clean: 
	rm -rf $(OUTPUT)
	rm -f $(SRC_DIR)/*.o
	rm -rf $(OBJECT)

