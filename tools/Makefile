RPATH = /usr/local/zeppelin/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

CXX = g++
ifeq ($(__PERF), 1)
CXXFLAGS = -O2 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-unused-variable
else
	CXXFLAGS = -O2 -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-unused-variable
endif

ifndef ZP_PATH 
ZP_PATH = $(realpath ../libzp)
endif

ifndef SLASH_PATH
SLASH_PATH = $(realpath ../third/slash)
endif

ifndef PINK_PATH 
PINK_PATH = $(realpath ../third/pink)
endif

OBJECT = zp_parade zp_benchmark zp_expansion zp_empty_trash
SRC_DIR = ./
OUTPUT = ./output

LIB_PATH = -L$(SLASH_PATH)/slash/lib/ \
		   -L$(PINK_PATH)/pink/lib/ \
		   -L$(ZP_PATH)/libzp/lib/

LIBS = -lzp \
			 -lprotobuf \
			 -lpink \
			 -lslash \
			 -lpthread

INCLUDE_PATH = -I$(ZP_PATH)/ \
		   -I$(PINK_PATH)/  \
		   -I$(SLASH_PATH)/ \

.PHONY: all clean distclean

ZP = $(ZP_PATH)/libzp/lib/libzp.a
PINK = $(PINK_PATH)/pink/lib/libpink.a
SLASH = $(SLASH_PATH)/slash/lib/libslash.a
LINENOISE = linenoise/linenoise.o

all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir -p $(OUTPUT)
	mkdir -p $(OUTPUT)/bin
	mkdir -p $(OUTPUT)/log
	cp $(OBJECT) $(OUTPUT)/bin/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."

zp_empty_trash: $(ZP) $(PINK) $(SLASH) zp_empty_trash.cc
	$(CXX) $(CXXFLAGS) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

zp_parade: $(ZP) $(PINK) $(SLASH) zp_parade.cc
	$(CXX) $(CXXFLAGS) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

zp_benchmark: $(ZP) $(PINK) $(SLASH) zp_benchmark.cc
	$(CXX) $(CXXFLAGS) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

zp_expansion: $(ZP) $(PINK) $(SLASH) zp_expansion.cc
	$(CXX) $(CXXFLAGS) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

$(ZP):
	make -C $(ZP_PATH)/libzp/ __PERF=$(__PERF) SLASH_PATH=$(SLASH_PATH) PINK_PATH=$(PINK_PATH)

$(SLASH):
	make -C $(SLASH_PATH)/slash/ __PERF=$(__PERF)

$(PINK):
	make -C $(PINK_PATH)/pink/ __PERF=$(__PERF) SLASH_PATH=$(SLASH_PATH)

$(LINENOISE):
	make -C linenoise


clean: 
	rm -rf $(OUTPUT)
	rm -rf $(SRC_DIR)/log
	rm -f $(SRC_DIR)/*.o
	rm -f linenoise/*.o
	rm -rf $(OBJECT)

distclean:
	make -C $(PINK_PATH)/pink/ clean
	make -C $(SLASH_PATH)/slash/ clean
	make -C $(ZP_PATH)/libzp/ clean

