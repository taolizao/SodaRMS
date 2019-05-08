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

#include "nc_core.h"
#include "nc_server.h"

static bool 
req_quota_verify(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct server_pool *pool = conn->owner;

    if (pool->token > 0) {
        pool->token--; 
        msg->ticket = 1;
        stats_pool_incr(ctx, conn->owner, consuming_token);
    } else if(pool->token == 0) {
        pool->quota_is_insufficient++;

        if (pool->quota_is_insufficient <= 10) {
            log_error("The quota of server_pool'%.*s' has been used up",
                    pool->name.len, pool->name.data);
        }
        if (pool->quota_is_insufficient == MAX_QUOTA_LIMITAION_WARNING) {
            pool->quota_is_insufficient = 0;
        }

        msg->done = 1;
        req_put(ctx, msg);

        /* close the remote connection ,or else the proxy will 
         * hold a large school of CLOSE_WAIT connection */
        conn->eof = 1; 
        return false;
    }

    return true;
}

void
req_quota_putback(struct context *ctx, struct msg *msg)
{
    struct conn *conn = msg->owner;

    if (!msg->swallow && msg->owner != NULL && !msg->owner->sentinel) {
        struct server_pool *pool = conn->owner;
        if (pool != NULL) {
            if(msg->ticket) {
                pool->token++;
                stats_pool_decr(ctx, conn->owner, consuming_token);
                msg->ticket = 0;
                log_debug(LOG_VVERB,"put back the token:%d,pool name is %.*s",
                        pool->token, pool->name.len, pool->name.data);
            }
        }
    }
}

struct msg *
req_get(struct context *ctx, struct conn *conn)
{
    struct msg *msg;
    struct server_pool *pool = conn->owner;

    ASSERT(conn->client && !conn->proxy);

    msg = msg_get(conn, true, conn->redis);
    if (msg == NULL) {
        conn->err = errno;
    }

    /* The switch control is ON and the connection is active */
    if (pool->flow_control == 1) {
        if (req_quota_verify(ctx, conn, msg) == false) {
            return NULL;
        } 
    }
    return msg;
}

void
req_put(struct context *ctx, struct msg *msg)
{
    struct msg *pmsg; /* peer message (response) */

    ASSERT(msg->request);

    pmsg = msg->peer;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->peer = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }

    req_quota_putback(ctx, msg);

    msg_tmo_delete(msg);

    msg_put(msg);
}

/*
 * Return true if request is done, false otherwise
 *
 * A request is done, if we received response for the given request.
 * A request vector is done if we received responses for all its
 * fragments.
 */
bool
req_done(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg, *pmsg; /* current and previous message */
    uint64_t id;             /* fragment id */
    uint32_t nfragment;      /* # fragment */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);

    if (!msg->done) {
        return false;
    }

    id = msg->frag_id;
    if (id == 0) {
        return true;
    }

    if (msg->fdone) {
        /* request has already been marked as done */
        return true;
    }

    if (msg->nfrag_done < msg->nfrag) {
        return false;
    }    

    /* check all fragments of the given request vector are done */

    for (pmsg = msg, cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    for (pmsg = msg, cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    /*
     * At this point, all the fragments including the last fragment have
     * been received.
     *
     * Mark all fragments of the given request vector to be done to speed up
     * future req_done calls for any of fragments of this request
     *
     *  any fragment mseterr err will set the err to 
     *  "READONLY You can't write against a read only slave.*
     */

    msg->fdone = 1;
    nfragment = 0;

    for (pmsg = msg, cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
        
        if(cmsg->mseterr == 1) {
            msg->mseterr = 1;
        }
    }

    for (pmsg = msg, cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;

        if(cmsg->mseterr == 1) {
            msg->mseterr = 1;
        }
    }

    ASSERT(msg->frag_owner->nfrag == nfragment);

    msg->post_coalesce(msg->frag_owner);

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is done", conn->sd, id, nfragment);

    return true;
}

/*
 * Return true if request is in error, false otherwise
 *
 * A request is in error, if there was an error in receiving response for the
 * given request. A multiget request is in error if there was an error in
 * receiving response for any its fragments.
 */
bool
req_error(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg; /* current message */
    uint64_t id;
    uint32_t nfragment;

    ASSERT(msg->request && req_done(conn, msg));

    if (msg->error) {
        return true;
    }

    id = msg->frag_id;
    if (id == 0) {
        return false;
    }

    if (msg->ferror) {
        /* request has already been marked to be in error */
        return true;
    }

    /* check if any of the fragments of the given request are in error */

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    return false;

ferror:

    /*
     * Mark all fragments of the given request to be in error to speed up
     * future req_error calls for any of fragments of this request
     */

    msg->ferror = 1;
    nfragment = 1;

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is in error", conn->sd, id, nfragment);

    return true;
}

void
req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    /*
     * timeout clock starts ticking the instant the message is enqueued into
     * the server in_q; the clock continues to tick until it either expires
     * or the message is dequeued from the server out_q
     *
     * noreply request are free from timeouts because client is not intrested
     * in the reponse anyway!
     */
    if (!msg->noreply) {
        msg_tmo_insert(msg, conn);
    }

    TAILQ_INSERT_TAIL(&conn->imsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, in_queue);
    stats_server_incr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, in_queue);
    stats_server_decr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
}

void
req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, out_queue);
    stats_server_incr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

void
req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
}

void
req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    msg_tmo_delete(msg);

    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, out_queue);
    stats_server_decr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

struct msg *
req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT(conn->client && !conn->proxy);

    if (conn->eof) {
        msg = conn->rmsg;

        /* client sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(msg->request && !msg->done);

            log_error("eof c %d discarding incomplete req %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            req_put(ctx, msg);
        }

        /*
         * TCP half-close enables the client to terminate its half of the
         * connection (i.e. the client no longer sends data), but it still
         * is able to receive data from the proxy. The proxy closes its
         * half (by sending the second FIN) when the client has no
         * outstanding requests
         */
        if (!conn->active(conn)) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }
        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = req_get(ctx, conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}

static rstatus_t
req_make_reply(struct context *ctx, struct conn *conn, struct msg *req)
{
    struct msg *msg;

    msg = msg_get(conn, true, conn->redis); /* reply */
    if (msg == NULL) {
        conn->err = errno;
        return NC_ENOMEM;
    }

    req->peer = msg;
    msg->peer = req;
    msg->request = 0;

    req->done = 1;
    conn->enqueue_outq(ctx, conn, req);
    return NC_OK;
}

static bool 
req_judge_perm(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct server_pool *pool = conn->owner;

    if (ctx->whitelist == 1) {
        if((msg->type & msg->owner->perm) == 0) {
            conn->done = 1;
            pool->perm_warning++;
            
            if (pool->perm_warning <= 10) {
                log_error("The perm is not right!Drop the messages!The ip of client is %s",
                        conn->peer_ip);
            }

            if (pool->perm_warning == MAX_PERM_WARNING) {
                pool->perm_warning = 0;
            }
            
            return false;
        }
    }
    return true;
}

static bool
req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct instance *nci = ctx->nci;
    int idx = nci->gray_idx, msg_id = msg->type & 0xff;
    ASSERT(conn->client && !conn->proxy);


    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        req_put(ctx, msg);
        return true;
    }

    /*
     * Handle "quit\r\n" (memcache) or "*1\r\n$4\r\nquit\r\n" (redis), which
     * is the protocol way of doing a passive close. The connection is closed
     * as soon as all pending replies have been written to the client.
     */ 
    if (msg->quit) {
        log_debug(LOG_INFO, "filter quit req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        if (conn->rmsg != NULL) {
            log_debug(LOG_INFO, "discard invalid req %"PRIu64" len %"PRIu32" "
                      "from c %d sent after quit req", conn->rmsg->id,
                      conn->rmsg->mlen, conn->sd);
        }
        conn->eof = 1;
        conn->recv_ready = 0;
        msg->noforward = 1;
        /*req_put(ctx, msg);*/
        return false;
    }

    if(REDIS_CMDS_BLACK == nci->cmd_gray[idx][msg_id]) {
        log_debug(LOG_WARN, "recv a cmd that is not whitelisted : %.*s, client info:%s", 
                msg->dump_len, msg->dump_data, conn->peer_ip);
    }

    /*
     * Hanlde "PING\r\n"
     *
     */
    /*if (msg->type == MSG_REQ_REDIS_PING) {*/
        /*log_debug(LOG_INFO, "filter ping req %"PRIu64" from c %d", msg->id, conn->sd);*/
        /*if (req_reply(ctx, conn, msg, "+PONG\r\n") != NC_OK) {*/
            /*req_put(ctx, msg);*/
        /*}*/
        /*return true;*/
    /*}*/
    if(!conn_authenticated(conn)) {
        msg->noforward = 1;
    }

    /* check client permission */
	if (!req_judge_perm(ctx, conn, msg)) {
        req_put(ctx, msg);
		return true;
    }

    return false;
}

static void
req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    ASSERT(conn->client && !conn->proxy);

    log_debug(LOG_INFO, "forward req %"PRIu64" len %"PRIu32" type %d from "
              "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
              strerror(errno));

    msg->done = 1;
    msg->error = 1;
    msg->err = errno;

    /* noreply request don't expect any response */
    if (msg->noreply) {
        req_put(ctx, msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->ep, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }
    }
}

static void
req_forward_stats(struct context *ctx, struct server_pool *pool, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);

    NUT_NOTUSED(ctx);
    NUT_NOTUSED(server);
    NUT_NOTUSED(pool);

    stats_server_incr(ctx, server, requests);
    stats_server_incr_by(ctx, server, request_bytes, msg->mlen);

    if (msg->type & PERM_W) {
        stats_pool_incr(ctx, pool, write_qps);
    } else {
        stats_pool_incr(ctx, pool, read_qps);
    }
    stats_pool_incr(ctx, pool, total_qps);
}

static void
req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg, int idx)
{
    rstatus_t status;
    struct conn *s_conn;
    //struct server_pool *pool;
    uint8_t *key;
    uint32_t keylen;
    struct keypos *kpos;

    ASSERT(c_conn->client && !c_conn->proxy);

    /* enqueue message (request) into client outq, if response is expected */
    if (!msg->noreply) {
        c_conn->enqueue_outq(ctx, c_conn, msg);
    }

    //pool = c_conn->owner;
    if (MSG_REQ_REDIS_BROADCAST == msg->type) {
        s_conn = server_pool_conn(ctx, c_conn->owner, key, keylen, msg->type, idx);
    } else {
        ASSERT(array_n(msg->keys) > 0);
        kpos = array_get(msg->keys, 0);
        key = kpos->start;
        keylen = (uint32_t)(kpos->end - kpos->start);    

        s_conn = server_pool_conn(ctx, c_conn->owner, key, keylen, msg->type, -1);
    }
    if (s_conn == NULL) {
        req_forward_error(ctx, c_conn, msg);
        return;
    }
    ASSERT(!s_conn->client && !s_conn->proxy);

    /* enqueue the message (request) into server inq */
    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
        status = event_add_out(ctx->ep, s_conn);
        if (status != NC_OK) {
            req_forward_error(ctx, c_conn, msg);
            s_conn->err = errno;
            return;
        }
    }

    if (!conn_authenticated(s_conn)) {
        status = server_add_auth(ctx, c_conn, s_conn);
        if (status != NC_OK) {
            req_forward_error(ctx, c_conn, msg);
            s_conn->err = errno;
            return;
        }
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);

    req_forward_stats(ctx, c_conn->owner, s_conn->owner, msg);

    log_debug(LOG_VERB, "forward from c %d to s %d req %"PRIu64" len %"PRIu32
              " type %d with key '%.*s'", c_conn->sd, s_conn->sd, msg->id,
              msg->mlen, msg->type, keylen, key);
}

void
req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    rstatus_t status;
    struct server_pool *pool;
    struct msg_tqh frag_msgq;
    struct msg *sub_msg;
    struct msg *tmsg; 			/* tmp next message */
    uint32_t idx = 0;
    uint32_t number = 0;

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
    ASSERT(nmsg == NULL || nmsg->request);

    log_debug(LOG_VERB,"Just 4 test,the permission of client is:%u, msg type is %d",
            msg->owner->perm, msg->type);

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (req_filter(ctx, conn, msg)) {
        return;
    }
    if(msg->noforward) {
        status = req_make_reply(ctx, conn, msg);
        if (status != NC_OK) {
            conn->err = errno;
            return;
        }

        status = msg->reply(msg);
        if (status != NC_OK) {
            conn->err = errno;
            return;
        }

        status = event_add_out(ctx->ep, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }

        return;
    }

    /* do fragment */
    pool = conn->owner;
    TAILQ_INIT(&frag_msgq);
    if (msg->type == MSG_REQ_REDIS_BROADCAST) {
        number = array_n(&pool->server);   
        log_debug(LOG_DEBUG,"recv a BROADCAST command from client %d %s", conn->sd, conn->peer_ip);
    } else {
        number = pool->ncontinuum;
    }
    status = msg->fragment(msg, number, &frag_msgq);
    if (status != NC_OK) {
        if (!msg->noreply) {
            conn->enqueue_outq(ctx, conn, msg);
        }
        req_forward_error(ctx, conn, msg);
        return;
    }

    /* if no fragment happened */
    if (TAILQ_EMPTY(&frag_msgq)) {
        req_forward(ctx, conn, msg, 0);
        return;
    }

    status = req_make_reply(ctx, conn, msg);
    if (status != NC_OK) {
        if (!msg->noreply) {
            conn->enqueue_outq(ctx, conn, msg);
        }
        req_forward_error(ctx, conn, msg);
        return;
    }

    idx = 0;
    for (sub_msg = TAILQ_FIRST(&frag_msgq); sub_msg != NULL; sub_msg = tmsg) {
        tmsg = TAILQ_NEXT(sub_msg, m_tqe);

        TAILQ_REMOVE(&frag_msgq, sub_msg, m_tqe);
        req_forward(ctx, conn, sub_msg, idx++);
    }

    ASSERT(TAILQ_EMPTY(&frag_msgq));
    return;
}

struct msg *
req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(!conn->client && !conn->proxy);

    if (conn->connecting) {
        server_connected(ctx, conn);
    }

    nmsg = TAILQ_FIRST(&conn->imsg_q);
    if (nmsg == NULL) {
        /* nothing to send as the server inq is empty */
        status = event_del_out(ctx->ep, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(msg->request && !msg->done);
        nmsg = TAILQ_NEXT(msg, s_tqe);
    }

    conn->smsg = nmsg;

    if (nmsg == NULL) {
        return NULL;
    }

    ASSERT(nmsg->request && !nmsg->done);

    log_debug(LOG_VVERB, "send next req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", nmsg->id, nmsg->mlen, nmsg->type, conn->sd);

    return nmsg;
}

void
req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(!conn->client && !conn->proxy);
    ASSERT(msg != NULL && conn->smsg == NULL);
    ASSERT(msg->request && !msg->done);
    ASSERT(msg->owner != conn);

    log_debug(LOG_VVERB, "send done req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", msg->id, msg->mlen, msg->type, conn->sd);

    /* dequeue the message (request) from server inq */
    conn->dequeue_inq(ctx, conn, msg);

    /*
     * noreply request instructs the server not to send any response. So,
     * enqueue message (request) in server outq, if response is expected.
     * Otherwise, free the noreply request
     */
    msg_put_mbuf(msg);
    if (!msg->noreply) {
        conn->enqueue_outq(ctx, conn, msg);
    } else {
        req_put(ctx, msg);
    }
}

rstatus_t
req_construct(struct context *ctx, struct conn *conn, char *cmd_str)
{
    rstatus_t status;
    struct msg *construct_req;

    /* the construct req don't have conn */
    construct_req = msg_get(NULL, true, 1);
    if (construct_req == NULL) {
        log_error("get msg for construct req(%s) failed", cmd_str);
        return NC_ERROR;
    }

    /* the construct req don't have client conn to reply.
     * we know the response order, so we don't need the peer request.
     * mark it noreply to release it when sent or socket error.
     */
    construct_req->noreply = 1;

    status = msg_fill(construct_req, cmd_str);
    if (status != NC_OK) {
        msg_put(construct_req);
        return status;
    }

    /* enqueue the message (request) into server inq */
    if (TAILQ_EMPTY(&conn->imsg_q)) {
        status = event_add_out(ctx->ep, conn);
        if (status != NC_OK) {
            conn->err = errno;
            msg_put(construct_req);
            return NC_ERROR;
        }
    }
    conn->enqueue_inq(ctx, conn, construct_req);
    
    return NC_OK;
}
