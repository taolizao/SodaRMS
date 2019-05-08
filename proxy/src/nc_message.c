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

#include <stdio.h>
#include <stdlib.h>

#include <sys/uio.h>

#include "nc_core.h"
#include "nc_server.h"
#include "nc_util.h"
#include "nc_proto.h"

#ifndef IOV_MAX
#define IOV_MAX 128
#endif

#if (IOV_MAX > 128)
#define NC_IOV_MAX 128
#else
#define NC_IOV_MAX IOV_MAX
#endif

/*
 *            nc_message.[ch]
 *         message (struct msg)
 *            +        +            .
 *            |        |            .
 *            /        \            .
 *         Request    Response      .../ nc_mbuf.[ch]  (mesage buffers)
 *      nc_request.c  nc_response.c .../ nc_memcache.c; nc_redis.c (message parser)
 *
 * Messages in nutcracker are manipulated by a chain of processing handlers,
 * where each handler is responsible for taking the input and producing an
 * output for the next handler in the chain. This mechanism of processing
 * loosely conforms to the standard chain-of-responsibility design pattern
 *
 * At the high level, each handler takes in a message: request or response
 * and produces the message for the next handler in the chain. The input
 * for a handler is either a request or response, but never both and
 * similarly the output of an handler is either a request or response or
 * nothing.
 *
 * Each handler itself is composed of two processing units:
 *
 * 1). filter: manipulates output produced by the handler, usually based
 *     on a policy. If needed, multiple filters can be hooked into each
 *     location.
 * 2). forwarder: chooses one of the backend servers to send the request
 *     to, usually based on the configured distribution and key hasher.
 *
 * Handlers are registered either with Client or Server or Proxy
 * connections. A Proxy connection only has a read handler as it is only
 * responsible for accepting new connections from client. Read handler
 * (conn_recv_t) registered with client is responsible for reading requests,
 * while that registered with server is responsible for reading responses.
 * Write handler (conn_send_t) registered with client is responsible for
 * writing response, while that registered with server is responsible for
 * writing requests.
 *
 * Note that in the above discussion, the terminology send is used
 * synonymously with write or OUT event. Similarly recv is used synonymously
 * with read or IN event
 *
 *             Client+             Proxy           Server+
 *                              (nutcracker)
 *                                   .
 *       msg_recv {read event}       .       msg_recv {read event}
 *         +                         .                         +
 *         |                         .                         |
 *         \                         .                         /
 *         req_recv_next             .             rsp_recv_next
 *           +                       .                       +
 *           |                       .                       |       Rsp
 *           req_recv_done           .           rsp_recv_done      <===
 *             +                     .                     +
 *             |                     .                     |
 *    Req      \                     .                     /
 *    ===>     req_filter*           .           *rsp_filter
 *               +                   .                   +
 *               |                   .                   |
 *               \                   .                   /
 *               req_forward-//  (a) . (c)  \\-rsp_forward
 *                                   .
 *                                   .
 *       msg_send {write event}      .      msg_send {write event}
 *         +                         .                         +
 *         |                         .                         |
 *    Rsp' \                         .                         /     Req'
 *   <===  rsp_send_next             .             req_send_next     ===>
 *           +                       .                       +
 *           |                       .                       |
 *           \                       .                       /
 *           rsp_send_done-//    (d) . (b)    //-req_send_done
 *
 *
 * (a) -> (b) -> (c) -> (d) is the normal flow of transaction consisting
 * of a single request response, where (a) and (b) handle request from
 * client, while (c) and (d) handle the corresponding response from the
 * server.
 */

static uint64_t msg_id;          /* message id counter */
static uint64_t frag_id;         /* fragment id counter */
static uint32_t nfree_msgq;      /* # free msg q */
static uint32_t nall_msg;
static struct msg_tqh free_msgq; /* free msg q */
static struct rbtree tmo_rbt;    /* timeout rbtree */
static struct rbnode tmo_rbs;    /* timeout rbtree sentinel */

#define DEFINE_ACTION(_name, _mode, _val, _cmdname) _cmdname,
char* msg_name[] = {
    MSG_TYPE_CODEC(DEFINE_ACTION)
};
#undef DEFINE_ACTION

static struct msg *
msg_from_rbe(struct rbnode *node)
{
    struct msg *msg;
    int offset;

    offset = offsetof(struct msg, tmo_rbe);
    msg = (struct msg *)((char *)node - offset);

    return msg;
}

struct msg *
msg_tmo_min(void)
{
    struct rbnode *node;

    node = rbtree_min(&tmo_rbt);
    if (node == NULL) {
        return NULL;
    }

    return msg_from_rbe(node);
}

void
msg_tmo_insert(struct msg *msg, struct conn *conn)
{
    struct rbnode *node;
    int timeout;

    ASSERT(msg->request);
    ASSERT(!msg->quit && !msg->noreply);

    timeout = server_timeout(conn);
    if (timeout <= 0) {
        return;
    }

    node = &msg->tmo_rbe;
    node->key = nc_msec_now() + timeout;
    node->data = conn;

    rbtree_insert(&tmo_rbt, node);

    log_debug(LOG_VERB, "insert msg %"PRIu64" into tmo rbt with expiry of "
              "%d msec", msg->id, timeout);
}

void
msg_tmo_delete(struct msg *msg)
{
    struct rbnode *node;

    node = &msg->tmo_rbe;

    /* already deleted */

    if (node->data == NULL) {
        return;
    }

    rbtree_delete(&tmo_rbt, node);

    log_debug(LOG_VERB, "delete msg %"PRIu64" from tmo rbt", msg->id);
}

static struct msg *
_msg_get(void)
{
    struct msg *msg;

    if (!TAILQ_EMPTY(&free_msgq)) {
        ASSERT(nfree_msgq > 0);

        msg = TAILQ_FIRST(&free_msgq);
        nfree_msgq--;
        TAILQ_REMOVE(&free_msgq, msg, m_tqe);
        goto done;
    }

    msg = nc_alloc(sizeof(*msg));
    if (msg == NULL) {
        return NULL;
    }

done:
    //FIXME
    nall_msg++;
    /* c_tqe, s_tqe, and m_tqe are left uninitialized */
    msg->init_time = nc_get_current_timestamp_in_usec();
    msg->id = ++msg_id;
    msg->peer = NULL;
    msg->owner = NULL;

    rbtree_node_init(&msg->tmo_rbe);

    STAILQ_INIT(&msg->mhdr);
    msg->mlen = 0;

    msg->state = 0;
    msg->pos = NULL;
    msg->token = NULL;

    msg->parser = NULL;
    msg->result = MSG_PARSE_OK;

    msg->fragment = NULL;
    msg->add_auth = NULL;
    msg->reply = NULL;
    msg->pre_coalesce = NULL;
    msg->post_coalesce = NULL;

    msg->type = MSG_UNKNOWN;

    msg->keys = array_create(1, sizeof(struct keypos));
    if (msg->keys == NULL) {
        nc_free(msg);
        return NULL;
    }

    msg->vlen = 0;
    msg->end = NULL;

    msg->frag_owner = NULL;
    msg->nfrag = 0;
    msg->depth = 0;
    msg->frag_id = 0;
    msg->frag_seq = NULL;
    msg->nfrag_done = 0;

    msg->narg_start = NULL;
    msg->narg_end = NULL;
    msg->narg = 0;
    msg->rnarg = 0;
    msg->rlen = 0;
    msg->integer = 0;
    msg->dump_len = 0;

    msg->err = 0;
    msg->error = 0;
    msg->ferror = 0;
    msg->request = 0;
    msg->quit = 0;
    msg->noreply = 0;
    msg->noforward = 0;
    msg->done = 0;
    msg->fdone = 0;
    msg->swallow = 0;
    msg->redis = 0;
    msg->mseterr = 0;
    msg->ticket = 0;

    return msg;
}

void msg_pipe_print_warn(struct conn *conn)
{
    int log_level;
    int print_mod;
    static int64_t last = 0, print_cnt = 0, now;

    ASSERT(conn->pipeline_num > 0);
    if(conn->pipeline_num <= CONN_PIPELINE_LIMIT0) {
        return;
    } else if(conn->pipeline_num > CONN_PIPELINE_LIMIT0 && 
            conn->pipeline_num <= CONN_PIPELINE_LIMIT1) {
        print_mod = 100000;
        log_level = LOG_INFO;
    } else if(conn->pipeline_num > CONN_PIPELINE_LIMIT1 && 
            conn->pipeline_num <= CONN_PIPELINE_LIMIT2) {
        print_mod = 10000;
        log_level = LOG_NOTICE;
    } else {
        print_mod = 10000;
        log_level = LOG_WARN;
    }
    now = nc_msec_now();
    if(now - last > 5000) {
        last = now;
        print_cnt = 0;
    }
    if(print_cnt % print_mod == 0) {
        log_debug(log_level, "conn %d from %s use pipeline %d", 
                conn->sd, conn->peer_ip, conn->pipeline_num);
    }
    print_cnt++;
}

struct msg *
msg_get(struct conn *conn, bool request, bool redis)
{
    struct msg *msg;

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    //mget, mset, del, ping等命令的rsp不会和具体某个server conn绑定
    //这类req->owner指向client conn, 因此需要根据request判断
    if(request && conn && conn->client) {
        conn->pipeline_num++;
        if(conn->pipeline_num > conn->pipeline_max_num) {
            conn->pipeline_max_num = conn->pipeline_num;
        }
        msg_pipe_print_warn(conn);
    }

    msg->owner = conn;
    msg->request = request ? 1 : 0;
    msg->redis = redis ? 1 : 0;

    if (redis) {
        if (request) {
            msg->parser = redis_parse_req;
        } else {
            msg->parser = redis_parse_rsp;
        }
        msg->fragment = redis_fragment;
        msg->pre_coalesce = redis_pre_coalesce;
        msg->post_coalesce = redis_post_coalesce;
        msg->add_auth = redis_add_auth;
        msg->reply = redis_reply;
    } else {
        if (request) {
            msg->parser = memcache_parse_req;
        } else {
            msg->parser = memcache_parse_rsp;
        }
        msg->fragment = memcache_fragment;
        msg->pre_coalesce = memcache_pre_coalesce;
        msg->post_coalesce = memcache_post_coalesce;
    }

    if (conn != NULL) {
        log_debug(LOG_VVERB, "get msg %p id %"PRIu64" request %d owner sd %d",
                msg, msg->id, msg->request, conn->sd);
    }

    return msg;
}

struct msg *
msg_get_error(bool redis, err_t err)
{
    struct msg *msg;
    struct mbuf *mbuf;
    int n;
    char *errstr = err ? strerror(err) : "unknown";
    char *protstr = redis ? "-ERR" : "SERVER_ERROR";

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->state = 0;
    msg->type = MSG_RSP_MC_SERVER_ERROR;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        msg_put(msg);
        return NULL;
    }
    mbuf_insert(&msg->mhdr, mbuf);

    n = nc_scnprintf(mbuf->last, mbuf_size(mbuf), "%s %s"CRLF, protstr, errstr);
    mbuf->last += n;
    msg->mlen = (uint32_t)n;

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" len %"PRIu32" error '%s'",
              msg, msg->id, msg->mlen, errstr);

    return msg;
}

static void
msg_free(struct msg *msg)
{
    ASSERT(STAILQ_EMPTY(&msg->mhdr));

    log_debug(LOG_VVERB, "free msg %p id %"PRIu64"", msg, msg->id);
    nc_free(msg);
}

void
msg_put(struct msg *msg)
{
    log_debug(LOG_VVERB, "put msg %p id %"PRIu64"", msg, msg->id);
    struct conn *conn = msg->owner;

    if(!msg->swallow && msg->request && conn && conn->client) {
        ASSERT(conn->pipeline_num > 0);
        conn->pipeline_num--;
    }

    while (!STAILQ_EMPTY(&msg->mhdr)) {
        struct mbuf *mbuf = STAILQ_FIRST(&msg->mhdr);
        mbuf_remove(&msg->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    if (msg->frag_seq) {
        nc_free(msg->frag_seq);
        msg->frag_seq = NULL;
    }

    if (msg->keys) {
        msg->keys->nelem = 0; /* a hack here */
        array_destroy(msg->keys);
        msg->keys = NULL;
    }

    nall_msg--;
    nfree_msgq++;
    TAILQ_INSERT_HEAD(&free_msgq, msg, m_tqe);
}

void msg_dump_data(struct msg *msg, uint8_t *buf, int len)
{
    int left = MSG_DUMP_DATA_LEN - msg->dump_len;
    if(left <= 0) {
        return;
    }
    len = len > left ? left : len;
    nc_memcpy(msg->dump_data + msg->dump_len, buf, len);
    msg->dump_len += len;
    if(left > len) {
        msg->dump_data[msg->dump_len] = ' ';
        msg->dump_len++;
    }
}

void msg_put_mbuf(struct msg *msg)
{
    while (!STAILQ_EMPTY(&msg->mhdr)) {
        struct mbuf *mbuf = STAILQ_FIRST(&msg->mhdr);
        mbuf_remove(&msg->mhdr, mbuf);
        mbuf_put(mbuf);
    }
}

void
msg_dump(struct msg *msg)
{
    struct mbuf *mbuf;

    loga("msg dump id %"PRIu64" request %d len %"PRIu32" type %d done %d "
         "error %d (err %d)", msg->id, msg->request, msg->mlen, msg->type,
         msg->done, msg->error, msg->err);

    STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
        uint8_t *p, *q;
        long int len;

        p = mbuf->start;
        q = mbuf->last;
        len = q - p;

        loga_hexdump(p, len, "mbuf with %ld bytes of data", len);
    }
}

void
msg_init(void)
{
    log_debug(LOG_DEBUG, "msg size %d", sizeof(struct msg));
    msg_id = 0;
    frag_id = 0;
    nfree_msgq = 0;
    nall_msg = 0;
    TAILQ_INIT(&free_msgq);
    rbtree_init(&tmo_rbt, &tmo_rbs);
}

void
msg_deinit(void)
{
    struct msg *msg, *nmsg;

    for (msg = TAILQ_FIRST(&free_msgq); msg != NULL;
         msg = nmsg, nfree_msgq--) {
        ASSERT(nfree_msgq > 0);
        nmsg = TAILQ_NEXT(msg, m_tqe);
        msg_free(msg);
    }
    ASSERT(nfree_msgq == 0);
}

bool
msg_empty(struct msg *msg)
{
    return msg->mlen == 0 ? true : false;
}

uint32_t
msg_backend_idx(struct msg *msg, uint8_t *key, uint32_t keylen)
{
    struct conn *conn = msg->owner;
    struct server_pool *pool = conn->owner;

    return server_pool_idx(pool, key, keylen);
}

struct mbuf *
msg_ensure_mbuf(struct msg *msg, size_t len)
{
    struct mbuf *mbuf;

    if (STAILQ_EMPTY(&msg->mhdr) ||
        mbuf_size(STAILQ_LAST(&msg->mhdr, mbuf, next)) < len) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NULL;
        }
        mbuf_insert(&msg->mhdr, mbuf);
    } else {
        mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    }
    return mbuf;
}

/*
 * append small(small than a mbuf) content into msg
 */
rstatus_t
msg_append(struct msg *msg, uint8_t *pos, size_t n)
{
    struct mbuf *mbuf;

    ASSERT(n <= mbuf_data_size());

    mbuf = msg_ensure_mbuf(msg, n);
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(n <= mbuf_size(mbuf));

    mbuf_copy(mbuf, pos, n);
    msg->mlen += (uint32_t)n;
    return NC_OK;
}

/*
 * prepend small(small than a mbuf) content into msg
 */
rstatus_t
msg_prepend(struct msg *msg, uint8_t *pos, size_t n)
{
    struct mbuf *mbuf;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(n <= mbuf_size(mbuf));

    mbuf_copy(mbuf, pos, n);
    msg->mlen += (uint32_t)n;

    STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);
    return NC_OK;
}

/*
 * prepend small(small than a mbuf) content into msg
 */
rstatus_t
msg_prepend_format(struct msg *msg, const char *fmt, ...)
{
    struct mbuf *mbuf;
    int32_t n;
    va_list args;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    va_start(args, fmt);
    n = nc_vscnprintf(mbuf->last, mbuf_size(mbuf), fmt, args);
    va_end(args);

    mbuf->last += n;
    msg->mlen += (uint32_t)n;

    ASSERT(mbuf_size(mbuf) >= 0);
    STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);
    return NC_OK;
}

inline uint64_t
msg_gen_frag_id(void)
{
    return ++frag_id;
}

/* read a line from msg's mbuf, then write to line_buf,
 * if line_num is n, we will skip front n - 1 lines in the msg
 */
void
msg_read_line(struct msg* msg, struct mbuf *line_buf, int line_num)
{
    struct mbuf *mbuf;
    uint32_t mlen, buf_size, copy_size, search_size;
    uint8_t *p;

    mbuf_rewind(line_buf);

    p = NULL;
    for (mbuf = STAILQ_FIRST(&msg->mhdr);
         mbuf != NULL && p == NULL; ) {

        if (mbuf_empty(mbuf)) {
            mbuf = STAILQ_NEXT(mbuf, next);
            continue;
        }

        mlen = mbuf_length(mbuf);
        buf_size = mbuf_size(line_buf);

        search_size = mlen < buf_size ? mlen : buf_size;

        p = nc_strchr(mbuf->pos, mbuf->pos + search_size, LF);
        if(p == NULL) {
            if (line_num == 1 && search_size >= buf_size) {
                log_error("msg read line exceed buf size.");
                return;
            }
            else {
                copy_size = search_size;
            }
        }
        else {
            copy_size = (uint8_t *)p - mbuf->pos + 1;
        }

        /* line_num equals 1 means this line is wanted, so copy it.
         * or else if the p is not null, let line_num minus 1,
         * stand for skiped one line.
         */
        if (line_num == 1) {
            nc_memcpy(line_buf->last, mbuf->pos, copy_size);
            line_buf->last += copy_size;
        } else if (p != NULL) {
            line_num--;
            p = NULL;
        }
        mbuf->pos += copy_size;
    }
}

static rstatus_t
msg_parsed(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *nmsg;
    struct mbuf *mbuf, *nbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (msg->pos == mbuf->last) {
        /* no more data to parse */
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
    }

    /*
     * Input mbuf has un-parsed data. Split mbuf of the current message msg
     * into (mbuf, nbuf), where mbuf is the portion of the message that has
     * been parsed and nbuf is the portion of the message that is un-parsed.
     * Parse nbuf as a new message nmsg in the next iteration.
     */
    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }

    if(msg->request) {
        nmsg = req_get(ctx, msg->owner);
    } else {
        nmsg = msg_get(msg->owner, msg->request, conn->redis);
    }
    if (nmsg == NULL) {
        mbuf_put(nbuf);
        return NC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    /* update length of current (msg) and new message (nmsg) */
    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    conn->recv_done(ctx, conn, msg, nmsg);

    return NC_OK;
}

static rstatus_t
msg_repair(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct mbuf *nbuf;

    NUT_NOTUSED(ctx);
    NUT_NOTUSED(conn);

    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }
    mbuf_insert(&msg->mhdr, nbuf);
    msg->pos = nbuf->pos;

    return NC_OK;
}

static rstatus_t
msg_parse(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    if (msg_empty(msg)) {
        /* no data to parse */
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
    }

    msg->parser(msg);

    switch (msg->result) {
    case MSG_PARSE_OK:
        status = msg_parsed(ctx, conn, msg);
        break;

    case MSG_PARSE_REPAIR:
        status = msg_repair(ctx, conn, msg);
        break;

    case MSG_PARSE_AGAIN:
        status = NC_OK;
        break;

    default:
        status = NC_ERROR;
        conn->err = errno;
        break;
    }

    return conn->err != 0 ? NC_ERROR : status;
}

static rstatus_t
msg_recv_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *nmsg;
    struct mbuf *mbuf;
    size_t msize;
    ssize_t n;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NC_ENOMEM;
        }
        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }
    ASSERT(mbuf->end - mbuf->last > 0);

    msize = mbuf_size(mbuf);

    n = conn_recv(conn, mbuf->last, msize);
    if (n < 0) {
        if (n == NC_EAGAIN) {
            return NC_OK;
        }
        return NC_ERROR;
    }

    ASSERT((mbuf->last + n) <= mbuf->end);
    mbuf->last += n;
    msg->mlen += (uint32_t)n;

    for (;;) {
        status = msg_parse(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }

        /* get next message to parse */
        nmsg = conn->recv_next(ctx, conn, false);
        if (nmsg == NULL || nmsg == msg) {
            /* no more data to parse */
            break;
        }

        msg = nmsg;
    }

    return NC_OK;
}


rstatus_t
msg_recv_stop(struct context *ctx, struct conn *conn)
{
    ASSERT(conn && conn->client);

    if(!mem_exceed_soft_limit || !conn->recv_active) {
        return NC_ERROR;
    }
    if(conn->pipeline_num >= NUT_PIPELINE_LIMIT) {
        log_debug(LOG_WARN, "stop recv on conn sd %d from client(%s) pipeline %d rb %zu sb %zu", 
                conn->sd, conn->peer_ip, 
                conn->pipeline_num, conn->recv_bytes, conn->send_bytes);

        event_del_in(ctx->ep, conn);
        conn->recv_ready = 0;
        return NC_OK;
    }
    return NC_ERROR;
}

rstatus_t
msg_recv_start(struct context *ctx, struct conn *conn)
{
    if(!conn->client) {
        return NC_OK;
    }

    if(conn->recv_active) {
        return NC_OK;
    }
    if((!mem_exceed_soft_limit && conn->pipeline_num < NUT_PIPELINE_LIMIT) ||
            conn->pipeline_num < CONN_PIPELINE_LIMIT0) {
        log_debug(LOG_NOTICE, "continue recv on conn sd %d from client(%s) "
                "pipeline %d rb %zu sb %zu", 
                conn->sd, conn->peer_ip, 
                conn->pipeline_num, conn->recv_bytes, conn->send_bytes);
        event_add_in(ctx->ep, conn);
    }
    return NC_OK;
}

rstatus_t
msg_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    conn->recv_ready = 1;
    do {
        if(mem_exceed_soft_limit && conn->client && NC_OK == msg_recv_stop(ctx, conn)) {
            break;
        }
        msg = conn->recv_next(ctx, conn, true);
        if (msg == NULL) {
            return NC_OK;
        }

        status = msg_recv_chain(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return NC_OK;
}

static rstatus_t
msg_send_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg_tqh send_msgq;            /* send msg q */
    struct msg *nmsg;                    /* next msg */
    struct mbuf *mbuf, *nbuf;            /* current and next mbuf */
    size_t mlen;                         /* current mbuf data length */
    struct iovec *ciov, iov[NC_IOV_MAX]; /* current iovec */
    struct array sendv;                  /* send iovec */
    size_t nsend, nsent;                 /* bytes to send; bytes sent */
    size_t limit;                        /* bytes to send limit */
    ssize_t n;                           /* bytes sent by sendv */

    TAILQ_INIT(&send_msgq);

    array_set(&sendv, iov, sizeof(iov[0]), NC_IOV_MAX);

    /* preprocess - build iovec */

    nsend = 0;
    /*
     * readv() and writev() returns EINVAL if the sum of the iov_len values
     * overflows an ssize_t value Or, the vector count iovcnt is less than
     * zero or greater than the permitted maximum.
     */
    limit = SSIZE_MAX;

    for (;;) {
        ASSERT(conn->smsg == msg);

        TAILQ_INSERT_TAIL(&send_msgq, msg, m_tqe);

        for (mbuf = STAILQ_FIRST(&msg->mhdr);
             mbuf != NULL && array_n(&sendv) < NC_IOV_MAX && nsend < limit;
             mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);
            if ((nsend + mlen) > limit) {
                mlen = limit - nsend;
            }

            ciov = array_push(&sendv);
            ciov->iov_base = mbuf->pos;
            ciov->iov_len = mlen;

            nsend += mlen;
        }

        if (array_n(&sendv) >= NC_IOV_MAX || nsend >= limit) {
            break;
        }

        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            break;
        }
    }

    //ASSERT(!TAILQ_EMPTY(&send_msgq) && nsend != 0);

    /*
     * (nsend == 0) is possible in redis multi-del
     * see PR: https://github.com/twitter/twemproxy/pull/225
     */
    conn->smsg = NULL;
    if (!TAILQ_EMPTY(&send_msgq) && nsend != 0) {
        n = conn_sendv(conn, &sendv, nsend);
    } else {
        n = 0;
    }

    nsent = n > 0 ? (size_t)n : 0;

    /* postprocess - process sent messages in send_msgq */

    for (msg = TAILQ_FIRST(&send_msgq); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, m_tqe);

        TAILQ_REMOVE(&send_msgq, msg, m_tqe);

        if (nsent == 0) {
            if (msg->mlen == 0) {
                conn->send_done(ctx, conn, msg);
            }
            continue;
        }

        /* adjust mbufs of the sent message */
        for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);
            if (nsent < mlen) {
                /* mbuf was sent partially; process remaining bytes later */
                mbuf->pos += nsent;
                ASSERT(mbuf->pos < mbuf->last);
                nsent = 0;
                break;
            }

            /* mbuf was sent completely; mark it empty */
            mbuf->pos = mbuf->last;
            nsent -= mlen;
        }

        /* message has been sent completely, finalize it */
        if (mbuf == NULL) {
            conn->send_done(ctx, conn, msg);
        }
    }

    ASSERT(TAILQ_EMPTY(&send_msgq));

    if (n >= 0) {
        return NC_OK;
    }

    return (n == NC_EAGAIN) ? NC_OK : NC_ERROR;
}

rstatus_t
msg_send(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    ASSERT(conn->send_active);

    conn->send_ready = 1;
    do {
        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            /* nothing to send */
            return NC_OK;
        }

        status = msg_send_chain(ctx, conn, msg);
        msg_recv_start(ctx, conn);
        if (status != NC_OK) {
            return status;
        }

    } while (conn->send_ready);

    return NC_OK;
}

static struct mbuf *
msg_get_mbuf(struct msg *msg) 
{
    struct mbuf *mbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NULL;
        }

        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }
    ASSERT(mbuf->end - mbuf->last > 0);
    return mbuf;
}

rstatus_t
msg_fill(struct msg *msg, char *fill_str)
{
    size_t str_size;
    struct mbuf *mbuf;
    size_t msize;
    size_t copy_size;

    str_size = nc_strlen(fill_str);
    do {
        mbuf = msg_get_mbuf(msg);
        if (mbuf == NULL) {
            return NC_ENOMEM;
        }

        msize = mbuf_size(mbuf);
        copy_size = str_size <= msize ? str_size : msize;
        nc_memcpy(mbuf->last, fill_str, copy_size);

        mbuf->last += copy_size;
        msg->mlen += (uint32_t)copy_size;

        str_size -= copy_size;
        fill_str += copy_size;
    } while (str_size > 0);

    return NC_OK;
}

rstatus_t
msg_clone(struct msg *dst, struct msg *src)
{
    struct mbuf *mbuf = NULL;
    struct mbuf *nbuf = NULL;
    STAILQ_FOREACH(mbuf, &src->mhdr, next) {
        nbuf = mbuf_get();
        if(NULL == nbuf) {
            return NC_ENOMEM;
        }
        size_t size = mbuf->last - mbuf->start;
        mbuf_copy(nbuf, mbuf->start, size);
        mbuf_insert(&dst->mhdr, nbuf);
        dst->mlen += size;
    }
    return NC_OK;
}

