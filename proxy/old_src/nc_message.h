/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_MESSAGE_H_
#define _NC_MESSAGE_H_

#include "nc_core.h"

#define PERM_R   ( 0x1 << 28 )   //the highest 4 bits are:0001(unsigned int)
#define PERM_W   ( 0x1 << 29 )   //the highest 4 bits are:0010(unsigned int)
#define PERM_X   (0x1 << 30)

#define MSG_INFO_SENTINEL_STRING       "*2\r\n$4\r\ninfo\r\n$8\r\nsentinel\r\n"
#define MSG_INFO_REPLICATION_STRING    "*2\r\n$4\r\ninfo\r\n$11\r\nreplication\r\n"
#define MSG_SUB_SWITCH_REDIRECT_STRING "*3\r\n$9\r\nsubscribe\r\n$14\r\n+switch-master\r\n$19\r\n+redirect-to-master\r\n"

typedef void (*msg_parse_t)(struct msg *);
typedef rstatus_t (*msg_fragment_t)(struct msg *, uint32_t, struct msg_tqh *);
typedef void (*msg_coalesce_t)(struct msg *r);
typedef rstatus_t (*msg_add_auth_t)(struct context *ctx, struct conn *c_conn, struct conn *s_conn);
typedef rstatus_t (*msg_reply_t)(struct msg *r);


typedef enum msg_parse_result {
    MSG_PARSE_OK,                         /* parsing ok */
    MSG_PARSE_ERROR,                      /* parsing error */
    //MSG_PARSE_UNAUTHORIZATION,            /* the permission is not corresponding to the ip's perm*/
    MSG_PARSE_REPAIR,                     /* more to parse -> repair parsed & unparsed data */
    MSG_PARSE_AGAIN,                      /* incomplete -> parse again */
} msg_parse_result_t;

#define MSG_TYPE_CODEC(ACTION)                                                  \
    ACTION( MSG_UNKNOWN , 0, 0, "unknown")                                      \
    ACTION( MSG_REQ_REDIS_DEL , PERM_W , 1, "del")                              \
    ACTION( MSG_REQ_REDIS_EXISTS , PERM_R , 2, "exists")                        \
    ACTION( MSG_REQ_REDIS_EXPIRE , PERM_W , 3, "expire")                        \
    ACTION( MSG_REQ_REDIS_EXPIREAT , PERM_W , 4, "expireat")                    \
    ACTION( MSG_REQ_REDIS_PEXPIRE , PERM_W , 5, "pexpire")                      \
    ACTION( MSG_REQ_REDIS_PEXPIREAT , PERM_W , 6, "pexpireat")                  \
    ACTION( MSG_REQ_REDIS_PERSIST , PERM_W , 7, "persist")                      \
    ACTION( MSG_REQ_REDIS_PTTL , PERM_R , 8, "pttl")                            \
    ACTION( MSG_REQ_REDIS_TTL , PERM_R , 9, "ttl")                              \
    ACTION( MSG_REQ_REDIS_TYPE ,  PERM_R , 10, "type")                          \
    ACTION( MSG_REQ_REDIS_APPEND , PERM_W , 11, "append")                       \
    ACTION( MSG_REQ_REDIS_BITCOUNT , PERM_R , 12, "bitcount")                   \
    ACTION( MSG_REQ_REDIS_DECR , PERM_W , 13, "decr")                           \
    ACTION( MSG_REQ_REDIS_DECRBY , PERM_W , 14, "decrby")                       \
    ACTION( MSG_REQ_REDIS_DUMP , PERM_R , 15, "dump")                           \
    ACTION( MSG_REQ_REDIS_GET , PERM_R , 16, "get")                             \
    ACTION( MSG_REQ_REDIS_GETBIT , PERM_R , 17, "getbit")                       \
    ACTION( MSG_REQ_REDIS_GETRANGE , PERM_R , 18, "getrange")                   \
    ACTION( MSG_REQ_REDIS_GETSET , PERM_W , 19, "getset")                       \
    ACTION( MSG_REQ_REDIS_INCR , PERM_W , 20, "incr")                           \
    ACTION( MSG_REQ_REDIS_INCRBY , PERM_W , 21, "incrby")                       \
    ACTION( MSG_REQ_REDIS_INCRBYFLOAT , PERM_W ,22, "incrbyflot")               \
    ACTION( MSG_REQ_REDIS_MGET , PERM_R , 23, "mget")                           \
    ACTION( MSG_REQ_REDIS_PSETEX , PERM_W , 24, "psetex")                       \
    ACTION( MSG_REQ_REDIS_RESTORE , PERM_W , 25, "restore")                     \
    ACTION( MSG_REQ_REDIS_SET , PERM_W , 26, "set")                             \
    ACTION( MSG_REQ_REDIS_SETBIT , PERM_W , 27, "setbit")                       \
    ACTION( MSG_REQ_REDIS_SETEX , PERM_W , 28, "setex")                         \
    ACTION( MSG_REQ_REDIS_SETNX , PERM_W , 29, "setnx")                         \
    ACTION( MSG_REQ_REDIS_SETRANGE , PERM_W , 30, "setrange")                   \
    ACTION( MSG_REQ_REDIS_STRLEN , PERM_W , 31, "strlen")                       \
    ACTION( MSG_REQ_REDIS_HDEL , PERM_W , 32, "hdel")                           \
    ACTION( MSG_REQ_REDIS_HEXISTS , PERM_R , 33, "hexists")                     \
    ACTION( MSG_REQ_REDIS_HGET , PERM_R , 34, "hget")                           \
    ACTION( MSG_REQ_REDIS_HGETALL , PERM_R , 35, "hgetall")                     \
    ACTION( MSG_REQ_REDIS_HINCRBY ,  PERM_W , 36, "hincrby")                    \
    ACTION( MSG_REQ_REDIS_HINCRBYFLOAT , PERM_W , 37, "hincrbyfloat")           \
    ACTION( MSG_REQ_REDIS_HKEYS , PERM_R , 38, "hkeys")                         \
    ACTION( MSG_REQ_REDIS_HLEN , PERM_R , 39, "hlen")                           \
    ACTION( MSG_REQ_REDIS_HMGET , PERM_R , 40, "hmget")                         \
    ACTION( MSG_REQ_REDIS_HMSET , PERM_W , 41, "hmset")                         \
    ACTION( MSG_REQ_REDIS_HSET , PERM_W , 42, "hset")                           \
    ACTION( MSG_REQ_REDIS_HSETNX , PERM_W , 43, "hsetnx")                       \
    ACTION( MSG_REQ_REDIS_HVALS , PERM_R , 44, "hvals")                         \
    ACTION( MSG_REQ_REDIS_LINDEX , PERM_R , 45, "lindex")                       \
    ACTION( MSG_REQ_REDIS_LINSERT , PERM_W , 46, "linsert")                     \
    ACTION( MSG_REQ_REDIS_LLEN , PERM_R , 47, "llen")                           \
    ACTION( MSG_REQ_REDIS_LPOP , PERM_W , 48, "lpop")                           \
    ACTION( MSG_REQ_REDIS_LPUSH , PERM_W , 49, "lpush")                         \
    ACTION( MSG_REQ_REDIS_LPUSHX , PERM_W , 50, "lpushx")                       \
    ACTION( MSG_REQ_REDIS_LRANGE , PERM_R , 51, "lrange")                       \
    ACTION( MSG_REQ_REDIS_LREM , PERM_W , 52, "lrem")                           \
    ACTION( MSG_REQ_REDIS_LSET , PERM_W , 53, "lset")                           \
    ACTION( MSG_REQ_REDIS_LTRIM ,  PERM_W , 54, "ltrim")                        \
    ACTION( MSG_REQ_REDIS_RPOP , PERM_W , 55, "rpop")                           \
    ACTION( MSG_REQ_REDIS_RPOPLPUSH , PERM_W , 56, "rpoplpush")                 \
    ACTION( MSG_REQ_REDIS_RPUSH , PERM_W , 57, "rpush")                         \
    ACTION( MSG_REQ_REDIS_RPUSHX , PERM_W , 58, "rpushx")                       \
    ACTION( MSG_REQ_REDIS_SADD , PERM_W , 59, "sadd")                           \
    ACTION( MSG_REQ_REDIS_SCARD , PERM_R , 60, "scard")                         \
    ACTION( MSG_REQ_REDIS_SDIFF , PERM_R , 61, "sdiff")                         \
    ACTION( MSG_REQ_REDIS_SDIFFSTORE , PERM_W , 62, "sdiffstore")               \
    ACTION( MSG_REQ_REDIS_SINTER , PERM_R , 63, "sinter")                       \
    ACTION( MSG_REQ_REDIS_SINTERSTORE , PERM_W , 64, "sinterstore")             \
    ACTION( MSG_REQ_REDIS_SISMEMBER , PERM_R , 65, "sismember")                 \
    ACTION( MSG_REQ_REDIS_SMEMBERS , PERM_R , 66, "smembers")                   \
    ACTION( MSG_REQ_REDIS_SMOVE , PERM_W , 67, "smove")                         \
    ACTION( MSG_REQ_REDIS_SPOP , PERM_W , 68, "spop")                           \
    ACTION( MSG_REQ_REDIS_SRANDMEMBER , PERM_R , 69, "srandmember")             \
    ACTION( MSG_REQ_REDIS_SREM , PERM_W , 70, "srem")                           \
    ACTION( MSG_REQ_REDIS_SUNION , PERM_R , 71, "sunion")                       \
    ACTION( MSG_REQ_REDIS_SUNIONSTORE , PERM_W , 72, "sunionstore")             \
    ACTION( MSG_REQ_REDIS_ZADD , PERM_W , 73, "zadd")                           \
    ACTION( MSG_REQ_REDIS_ZCARD , PERM_R , 74, "zcard")                         \
    ACTION( MSG_REQ_REDIS_ZCOUNT , PERM_R , 75, "zcount")                       \
    ACTION( MSG_REQ_REDIS_ZINCRBY , PERM_W , 76, "zincrby")                     \
    ACTION( MSG_REQ_REDIS_ZINTERSTORE , PERM_W , 77, "zinterstore")             \
    ACTION( MSG_REQ_REDIS_ZRANGE , PERM_R , 78, "zrange")                       \
    ACTION( MSG_REQ_REDIS_ZRANGEBYSCORE , PERM_R , 79, "zrangebyscore")         \
    ACTION( MSG_REQ_REDIS_ZRANK , PERM_W , 80, "zrank")                         \
    ACTION( MSG_REQ_REDIS_ZREM , PERM_W , 81, "zrem")                           \
    ACTION( MSG_REQ_REDIS_ZREMRANGEBYRANK , PERM_W , 82, "zremrangebyrank")     \
    ACTION( MSG_REQ_REDIS_ZREMRANGEBYSCORE , PERM_W , 83, "zremrangebyscore")   \
    ACTION( MSG_REQ_REDIS_ZREVRANGE , PERM_R , 84, "zrevrange")                 \
    ACTION( MSG_REQ_REDIS_ZREVRANGEBYSCORE , PERM_R , 85, "zrevrangebyscore")   \
    ACTION( MSG_REQ_REDIS_ZREVRANK , PERM_R , 86, "zrevrank")                   \
    ACTION( MSG_REQ_REDIS_ZSCORE , PERM_R , 87, "zscore")                       \
    ACTION( MSG_REQ_REDIS_ZUNIONSTORE , PERM_W , 88, "zunionstore")             \
    ACTION( MSG_REQ_REDIS_EVAL , PERM_W , 89, "eval")                           \
    ACTION( MSG_REQ_REDIS_EVALSHA , PERM_W , 90, "evalsha")                     \
    ACTION( MSG_REQ_REDIS_MSET , PERM_W , 91, "mset")                           \
    ACTION( MSG_REQ_REDIS_PING , PERM_R , 92, "ping")                           \
    ACTION( MSG_REQ_REDIS_QUIT , PERM_R , 93, "quit")                           \
    ACTION( MSG_REQ_REDIS_SORT , PERM_R , 94, "sort")                           \
    ACTION( MSG_REQ_REDIS_PFCOUNT , PERM_R , 95, "pfcount")                     \
    ACTION( MSG_REQ_REDIS_ZLEXCOUNT , PERM_R , 96, "zlexcount")                 \
    ACTION( MSG_REQ_REDIS_ZREMRANGEBYLEX , PERM_W , 97, "zremrangebyex")        \
    ACTION( MSG_REQ_REDIS_HSCAN , PERM_R , 98, "hscan")                         \
    ACTION( MSG_REQ_REDIS_SSCAN , PERM_R , 99, "sscan")                         \
    ACTION( MSG_REQ_REDIS_PFADD , PERM_W , 100, "pfadd")                        \
    ACTION( MSG_REQ_REDIS_PFMERGE , PERM_W , 101, "pfmerge")                    \
    ACTION( MSG_REQ_REDIS_ZRANGEBYLEX , PERM_R , 102, "zrangebylex")            \
    ACTION( MSG_REQ_REDIS_ZSCAN , PERM_R , 103, "zscan")                        \
    ACTION( MSG_REQ_REDIS_SCRIPT , PERM_W , 104, "script")                      \
    ACTION( MSG_REQ_REDIS_AUTH , PERM_W , 105, "auth")                          \
    ACTION( MSG_REQ_REDIS_GEOADD, PERM_W, 106, "geoadd")                        \
    ACTION( MSG_REQ_REDIS_GEOPOS, PERM_R, 107, "geopos")                        \
    ACTION( MSG_REQ_REDIS_GEODIST, PERM_R, 108, "geodist")                      \
    ACTION( MSG_REQ_REDIS_GEORADIUS, PERM_R, 109, "georadius")                  \
    ACTION( MSG_REQ_REDIS_GEORADIUSBYMEMBER, PERM_R, 110, "georadiusbymember")  \
    ACTION( MSG_REQ_REDIS_GEOHASH, PERM_R, 111, "geohash")                      \
    ACTION( MSG_REQ_REDIS_BROADCAST, PERM_X, 112, "broadcast")                    \
    ACTION( MSG_REQ_REDIS_NUM, 0, 113, "null")                                  \
                                                                                \
    ACTION( MSG_REQ_MC_GET, PERM_R, 200, "mc_get")                              \
    ACTION( MSG_REQ_MC_GETS, PERM_R, 201, "mc_gets")                            \
    ACTION( MSG_REQ_MC_DELETE, PERM_W, 202, "mc_delete")                        \
    ACTION( MSG_REQ_MC_CAS, PERM_W, 203, "mc_cas")                              \
    ACTION( MSG_REQ_MC_SET, PERM_W, 204, "mc_set")                              \
    ACTION( MSG_REQ_MC_ADD, PERM_W, 205, "mc_add")                              \
    ACTION( MSG_REQ_MC_REPLACE, PERM_W, 206, "mc_replace")                      \
    ACTION( MSG_REQ_MC_APPEND, PERM_W, 207, "mc_append")                        \
    ACTION( MSG_REQ_MC_PREPEND, PERM_W, 208, "mc_prepend")                      \
    ACTION( MSG_REQ_MC_INCR, PERM_W, 209, "mc_incr")                            \
    ACTION( MSG_REQ_MC_DECR, PERM_W, 210, "mc_ecr")                             \
    ACTION( MSG_REQ_MC_QUIT, PERM_R, 211, "mc_quit")                            \
                                                                                \
    ACTION( MSG_RSP_REDIS_STATUS, 0, 500, "rsp_status")                         \
    ACTION( MSG_RSP_REDIS_ERROR, 0, 501, "rsp_error")                           \
    ACTION( MSG_RSP_REDIS_INTEGER, 0, 502, "rsp_integer")                       \
    ACTION( MSG_RSP_REDIS_BULK, 0, 503, "rsp_bulk")                             \
    ACTION( MSG_RSP_REDIS_MULTIBULK, 0, 504, "rsp_multibulk")                   \
                                                                                \
    ACTION( MSG_RSP_MC_NUM, 0, 505, "rsp_num")                                  \
    ACTION( MSG_RSP_MC_STORED, 0, 506, "rsp_stored")                            \
    ACTION( MSG_RSP_MC_NOT_STORED, 0, 507, "rsp_notstored")                     \
    ACTION( MSG_RSP_MC_EXISTS, 0, 508, "rsp_exists")                            \
    ACTION( MSG_RSP_MC_NOT_FOUND, 0, 509, "rsp_notfound")                       \
    ACTION( MSG_RSP_MC_END, 0, 510, "rsp_end")                                  \
    ACTION( MSG_RSP_MC_VALUE, 0, 511, "rsp_value")                              \
    ACTION( MSG_RSP_MC_DELETED, 0, 512, "rsp_deleted")                          \
    ACTION( MSG_RSP_MC_ERROR, 0, 513, "rsp_error")                              \
    ACTION( MSG_RSP_MC_CLIENT_ERROR, 0, 514, "rsp_clienterror")                 \
    ACTION( MSG_RSP_MC_SERVER_ERROR, 0, 515, "rsp_servererror")                 \
                                                                                \
    ACTION( MSG_SENTINEL , 0, 0x7fffffff, "sentinel")                           \

#define DEFINE_ACTION(_name, _mode, _val, _cmdname) _name = _val | _mode,
typedef enum msg_type {
    MSG_TYPE_CODEC(DEFINE_ACTION)
} msg_type_t;
#undef DEFINE_ACTION

extern char* msg_name[];

struct keypos {
    uint8_t             *start;           /* key start pos */
    uint8_t             *end;             /* key end pos */
};

#define MSG_DUMP_DATA_LEN    128
#define NC_MULTIBULK_DEPTH               3

struct msg {
    TAILQ_ENTRY(msg)     c_tqe;           /* link in client q */
    TAILQ_ENTRY(msg)     s_tqe;           /* link in server q */
    TAILQ_ENTRY(msg)     m_tqe;           /* link in send q / free q */

	/* variable init_time added by taolizao */
	uint64_t             init_time;       /* save struct msg init time in microsecond */
	uint64_t             id;              /* message id */
    struct msg           *peer;           /* message peer */
    struct conn          *owner;          /* message owner - client | server */

    struct rbnode        tmo_rbe;         /* entry in rbtree */

    struct mhdr          mhdr;            /* message mbuf header */
    uint32_t             mlen;            /* message length */

    int                  state;           /* current parser state */
    int                  depth;
    uint8_t              *pos;            /* parser position marker */
    uint8_t              *token;          /* token marker */

    msg_parse_t          parser;          /* message parser */
    msg_parse_result_t   result;          /* message parsing result */

    msg_fragment_t       fragment;        /* message fragment */
    msg_reply_t          reply;           /* generate message reply (example: ping) */
    msg_add_auth_t       add_auth;        /* add auth message when we forward msg */
    

    msg_coalesce_t       pre_coalesce;    /* message pre-coalesce */
    msg_coalesce_t       post_coalesce;   /* message post-coalesce */

    msg_type_t           type;            /* message type */

    struct array         *keys;           /* array of keypos, for req */

    uint32_t             vlen;            /* value length (memcache) */
    uint8_t              *end;            /* end marker (memcache) */

    uint8_t              *narg_start;     /* narg start (redis) */
    uint8_t              *narg_end;       /* narg end (redis) */
    uint32_t             narg;            /* # arguments (redis) */
    uint32_t             rnarg;           /* running # arg used by parsing fsa (redis) */
    uint32_t             rnargs[NC_MULTIBULK_DEPTH];
    uint32_t             rlen;            /* running length in parsing fsa (redis) */
    uint32_t             integer;         /* integer reply value (redis) */

    struct msg           *frag_owner;     /* owner of fragment message */
    uint32_t             nfrag;           /* # fragment */
    uint64_t             frag_id;         /* id of fragmented message */
    uint32_t             nfrag_done;      /* # fragment done */    
    struct msg           **frag_seq;      /* sequence of fragment message, map from keys to fragments*/    

    err_t                err;             /* errno on error? */
    unsigned             error:1;         /* error? */
    unsigned             ferror:1;        /* one or more fragments are in error? */
    unsigned             request:1;       /* request? or response? */
    unsigned             quit:1;          /* quit request? */
    unsigned             noreply:1;       /* noreply? */
    unsigned             noforward:1;
    unsigned             done:1;          /* done? */
    unsigned             fdone:1;         /* all fragments are done? */
    unsigned             swallow:1;       /* swallow response? */
    unsigned             redis:1;         /* redis? */
    unsigned             mseterr:1;   /* if the mset is fail? */ 
    unsigned             ticket:1;
    int                  dump_len;
    uint8_t              dump_data[MSG_DUMP_DATA_LEN];
};

TAILQ_HEAD(msg_tqh, msg);

struct msg *msg_tmo_min(void);
void msg_tmo_insert(struct msg *msg, struct conn *conn);
void msg_tmo_delete(struct msg *msg);

void msg_init(void);
void msg_deinit(void);
struct msg *msg_get(struct conn *conn, bool request, bool redis);
void msg_put(struct msg *msg);
struct msg *msg_get_error(bool redis, err_t err);
void msg_dump(struct msg *msg);
bool msg_empty(struct msg *msg);
void msg_read_line(struct msg* msg, struct mbuf *line_buf, int line_num);
rstatus_t msg_recv(struct context *ctx, struct conn *conn);
rstatus_t msg_send(struct context *ctx, struct conn *conn);
rstatus_t msg_fill(struct msg *msg, char *fill_str);

uint64_t msg_gen_frag_id(void);
uint32_t msg_backend_idx(struct msg *msg, uint8_t *key, uint32_t keylen);
struct mbuf *msg_ensure_mbuf(struct msg *msg, size_t len);
rstatus_t msg_append(struct msg *msg, uint8_t *pos, size_t n);
rstatus_t msg_prepend(struct msg *msg, uint8_t *pos, size_t n);
rstatus_t msg_prepend_format(struct msg *msg, const char *fmt, ...);

void req_quota_putback(struct context *ctx, struct msg *msg);
struct msg *req_get(struct context *ctx, struct conn *conn);
void req_put(struct context *ctx, struct msg *msg);
bool req_done(struct conn *conn, struct msg *msg);
bool req_error(struct conn *conn, struct msg *msg);
void req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
struct msg *req_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);
struct msg *req_send_next(struct context *ctx, struct conn *conn);
void req_send_done(struct context *ctx, struct conn *conn, struct msg *msg);
rstatus_t req_construct(struct context *ctx, struct conn *conn, char *cmd_str);

struct msg *rsp_get(struct conn *conn);
void rsp_put(struct msg *msg);
struct msg *rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);
struct msg *rsp_send_next(struct context *ctx, struct conn *conn);
void rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg);


void msg_put_mbuf(struct msg *msg);
void msg_dump_data(struct msg *msg, uint8_t *buf, int len);
rstatus_t msg_clone(struct msg *dst, struct msg *src);
void msg_put_mbuf(struct msg *msg);

#endif
