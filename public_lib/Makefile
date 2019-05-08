CC=gcc

CFLAGS=-Wall -O2 -g -static
LDFLAGS=-lm -lpthread

SRC_SOURCES=${wildcard src/*.c}
SRC_OBJS=${patsubst %.c,%.o,$(SRC_SOURCES)}

INC=-I./include
EXECUTE=libbasetool.a

all: $(SRC_OBJS)
	ar cr $(EXECUTE) $(SRC_OBJS)

%.o:%.c
	$(CC) -c $< -o $@ $(CFLAGS) $(INC)

clean:
	rm -f $(SRC_OBJS) $(EXECUTE)

