
OPTIMIZATION?=-O0

# Default settings
WARN=-Wall -W
OPT=$(OPTIMIZATION)
DEBUG=-g -ggdb

# Default allocator
MALLOC=jemalloc

FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS)
FINAL_LDFLAGS=$(LDFLAGS) $(DEBUG) -DHAVE_CONFIG_H
FINAL_LIBS=-lpthread -lm -pthread

# Include paths to dependencies
FINAL_CFLAGS+= -I../contrib/yaml-0.1.4/include/
FINAL_LIBS+= ../contrib/yaml-0.1.4/lib/libyaml.a


ifeq ($(MALLOC),jemalloc)
	FINAL_CFLAGS+= -I../contrib/jemalloc-4.0.4/include/
	FINAL_LIBS+= ../contrib/jemalloc-4.0.4/lib/libjemalloc_pic.a
endif

REDIS_CC=$(CC) $(FINAL_CFLAGS)
REDIS_LD=$(CC) $(FINAL_LDFLAGS)


BDRP_PROXY_NAME=nutcracker
PROXY_OBJ=nc_crc16.o nc_crc32.o nc_fnv.o nc_hsieh.o nc_jenkins.o nc_ketama.o nc_md5.o nc_modula.o nc_murmur.o nc_one_at_a_time.o nc_random.o nc_slot.o nc.o nc_array.o nc_client.o nc_conf.o nc_connection.o nc_core.o nc_event.o nc_log.o nc_mbuf.o nc_message.o nc_proxy.o nc_rbtree.o nc_request.o nc_response.o nc_sentinel.o nc_server.o nc_signal.o nc_stats.o nc_string.o nc_util.o nc_whitelist.o nc_memcache.o nc_redis.o

all: $(BDRP_PROXY_NAME) 
	@echo ""
	@echo "make dep; make"
	@echo ""

.PHONY: all

%.o: %.c
	$(REDIS_CC) -c $<


# Deps (use make dep to generate this)
include Makefile.dep

dep:
	$(REDIS_CC) -MM *.c > Makefile.dep

.PHONY: dep


$(BDRP_PROXY_NAME): $(PROXY_OBJ)
	$(REDIS_LD) -o $@  $^ $(FINAL_LIBS)


clean:
	rm -rf $(BDRP_PROXY_NAME)  *.o *.gcda *.gcno *.gcov redis.info lcov-html

.PHONY: clean


valgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc"

