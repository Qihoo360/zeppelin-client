RPATH = /usr/local/zeppelin/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

CXX = g++
ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O2 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-sign-compare
endif

ifndef SLASH_PATH
SLASH_PATH = ../../slash
endif

ifndef PINK_PATH 
PINK_PATH = ../../pink
endif

ifndef GLOG_PATH 
GLOG_PATH = ../../glog
endif

SRC_DIR := ./src/
INC_DIR := ./include/
OUTPUT := ./lib/


INCLUDE_PATH = -I../ \
							 -I src/ \
							 -I$(GLOG_PATH)/src \
							 -I$(SLASH_PATH) \
							 -I$(PINK_PATH)


LIB_PATH = -L/usr/local/lib/google/ \
					 -L$(SLASH_PATH)/slash/lib/ \
					 -L$(PINK_PATH)/pink/lib/ 



LIBS = -lpthread \
			 -lprotobuf \
			 -lglog \
			 -lslash \
			 -lpink

LIBRARY = libzp.a


.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(LIBRARY)
	@echo "Success, go, go, go..."

$(LIBRARY): $(OBJS)
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	rm -rf $@
	ar -rcs $@ $(OBJS)
	mv $@ $(OUTPUT)


$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) -Wl,-Bdynamic $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) $(LIBS)
clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
