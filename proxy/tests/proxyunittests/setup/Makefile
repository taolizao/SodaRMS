CC=gcc

include Makefile.define

CFLAGS=-Wall -O0 -g -fprofile-arcs -ftest-coverage
LDFLAGS=-lcunit -static -lrtstub -lproto -lhashkit -lyaml -lpthread -lm -fprofile-arcs -ftest-coverage -DHAVE_CONFIG_H -D_GNU_SOURCE -D_XOPEN_SOURCE 
#INC=-I/usr/local/include/CUnit
INC=-I$(topdir)/include/cunitinc
LIB=-L../libs


LIB+=-L../../../src/proto
LIB+=-L../../../contrib/yaml-0.1.4/src/.libs/
LIB+=-L../../../src/hashkit

INC += -I$(INCDIR)
INC += -I../../../src
INC += -I../../../contrib/yaml-0.1.4/include
INC += -I../../../contrib/yaml-0.1.4
INC += -I../../../src/proto
INC += -I../../../src/hashkit
INC += -I../../..

# OBJECTS=$(SRC_OBJS) $(CASES_OBJS) $(STUB_OBJS) $(SETUP_OBJS) $(NC_OBJS)
EXECUTE=test_main

all: $(SOURCES) $(EXECUTE)

$(EXECUTE): $(OBJECTS)
	$(CC) -o $@ $(INC) $(LIB) $(OBJECTS) $(LDFLAGS)
	mv $(EXECUTE) $(BINDIR)
#	mv $(OBJECTS) $(GCNOS) $(EXECUTE) $(BINDIR)

%.o:%.c
	$(CC) -c $< -o $@ $(CFLAGS) $(INC) $(LIB)

clean:
	rm -f $(BINDIR)/* $(OBJECTS) $(GCNOS) $(GCDAS) $(GCOVS)
	find ../ -name "*.xml" | xargs rm -rf
	find ../ -name "*.log" | xargs rm -rf

