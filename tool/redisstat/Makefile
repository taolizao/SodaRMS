CC=gcc

CFLAGS=-Wall -O -g3 -static
LDFLAGS=-lm -lpthread

SRC_SOURCES=${wildcard *.c}
SRC_SOURCES+=${wildcard ../base_tool/*.c}
SRC_OBJS=${patsubst %.c,%.o,$(SRC_SOURCES)}

LLIBS=../../deps/hiredis/libhiredis.a

INC=-I./ -I../../deps/hiredis -I../base_tool
EXECUTE=bdrp-benchmark

all: $(EXECUTE)

$(EXECUTE): $(SRC_OBJS)
	$(CC) -o $@ $(INC) $(SRC_OBJS) $(LLIBS) $(LDFLAGS)

%.o:%.c
	$(CC) -c $< -o $@ $(CFLAGS) $(INC)

clean:
	rm -f $(SRC_OBJS) $(EXECUTE)

