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
#include "nc_conf.h"
#include "nc_server.h"
#include "nc_proto.h"

#define DEFINE_ACTION(_hash, _name) string(#_name),
static struct string hash_strings[] = {
    HASH_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_hash, _name) hash_##_name,
static hash_t hash_algos[] = {
    HASH_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_dist, _name) string(#_name),
static struct string dist_strings[] = {
    DIST_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

static struct command conf_commands[] = {
    { string("listen"),
      conf_set_listen,
      offsetof(struct conf_pool, listen) },

    { string("hash"),
      conf_set_hash,
      offsetof(struct conf_pool, hash) },

    { string("hash_tag"),
      conf_set_hashtag,
      offsetof(struct conf_pool, hash_tag) },

    { string("distribution"),
      conf_set_distribution,
      offsetof(struct conf_pool, distribution) },

    { string("timeout"),
      conf_set_num,
      offsetof(struct conf_pool, timeout) },

    { string("backlog"),
      conf_set_num,
      offsetof(struct conf_pool, backlog) },

    { string("client_connections"),
      conf_set_num,
      offsetof(struct conf_pool, client_connections) },

    { string("redis"),
      conf_set_bool,
      offsetof(struct conf_pool, redis) },

    { string("preconnect"),
      conf_set_bool,
      offsetof(struct conf_pool, preconnect) },

    { string("auto_eject_hosts"),
      conf_set_bool,
      offsetof(struct conf_pool, auto_eject_hosts) },

    { string("server_connections"),
      conf_set_num,
      offsetof(struct conf_pool, server_connections) },

    { string("server_retry_timeout"),
      conf_set_num,
      offsetof(struct conf_pool, server_retry_timeout) },

    { string("server_failure_limit"),
      conf_set_num,
      offsetof(struct conf_pool, server_failure_limit) },

    { string("servers"),
      conf_add_server,
      offsetof(struct conf_pool, server) },

    { string("flow_control"),
      conf_set_bool,
      offsetof(struct conf_pool, flow_control) },

    { string("quota"),
      conf_set_num,
      offsetof(struct conf_pool, quota) },

    { string("password"), 
      conf_set_password,
      offsetof(struct conf_pool, password) },

    { string("client_auth"), 
      conf_set_password,
      offsetof(struct conf_pool, client_auth)},
    null_command
};

static void
conf_server_init(struct conf_server *cs)
{
    string_init(&cs->pname);
    string_init(&cs->name);
    cs->port = 0;
    cs->weight = 0;

    memset(&cs->info, 0, sizeof(cs->info));

    cs->master = NULL;

    cs->valid = 0;

    log_debug(LOG_VVERB, "init conf server %p", cs);
}

static void
conf_server_deinit(struct conf_server *cs)
{
    string_deinit(&cs->pname);
    string_deinit(&cs->name);

    if (cs->master == NULL) {
        while (array_n(&cs->slave) != 0) {
            conf_server_deinit(array_pop(&cs->slave));
        }
        array_deinit(&cs->slave);
    }

    cs->valid = 0;
    log_debug(LOG_VVERB, "deinit conf server %p", cs);
}

rstatus_t
conf_server_each_transform(void *elem, void *data)
{
    rstatus_t status;
    struct conf_server *cs = elem;
    struct array *server = data;
    struct server *s;
    uint32_t nslave;


    ASSERT(cs->valid);

    s = array_push(server);
    ASSERT(s != NULL);

    s->idx = array_idx(server, s);
    s->owner = NULL;
    s->conf_server = cs;

    s->pname = cs->pname;
    s->name = cs->name;
    s->port = (uint16_t)cs->port;
    s->weight = (uint32_t)cs->weight;

    s->family = cs->info.family;
    s->addrlen = cs->info.addrlen;
    s->addr = (struct sockaddr *)&cs->info.addr;

    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
    s->failure_count = 0;

    /* set the master to NULL, it will be seted to
     * its master after the function, if it's a slave.
     */
    s->master = NULL;
    s->status = SERVER_STATUS_ONLINE;
    s->slave_select = 0;

    log_debug(LOG_VERB, "transform to server %"PRIu32" '%.*s'",
              s->idx, s->pname.len, s->pname.data);

    /* if the serve is master, try to transform its slaves */
    if (cs->master == NULL) {
        nslave = array_n(&cs->slave);
        
        /* init slave array even master don't have slave.
         * we will use this array to detect if the master
         * have slaves.
         */
        status = array_init(&s->slave, nslave > 0 ? nslave : 1,
                            sizeof(struct server));
        if (status != NC_OK) {
            return status;
        }

        if (nslave > 0) {
            /* transform slave conf server to slave server */
            status = array_each(&cs->slave, conf_server_each_transform, &s->slave);
            if (status != NC_OK) {
                return status;
            }
            ASSERT(array_n(&s->slave) == nslave);

            /* set slave server's master to the server */
            status = array_each(&s->slave, server_each_set_master, s);
            if (status != NC_OK) {
                return status;
            }
        }
    }

    return NC_OK;
}

static rstatus_t
conf_pool_init(struct conf_pool *cp, struct string *name)
{
    rstatus_t status;

    string_init(&cp->name);
    string_init(&cp->password);
    string_init(&cp->client_auth);

    string_init(&cp->listen.pname);
    string_init(&cp->listen.name);
    cp->listen.port = 0;
    memset(&cp->listen.info, 0, sizeof(cp->listen.info));
    cp->listen.valid = 0;

    cp->hash = CONF_UNSET_HASH;
    string_init(&cp->hash_tag);
    cp->distribution = CONF_UNSET_DIST;

    cp->timeout = CONF_UNSET_NUM;
    cp->backlog = CONF_UNSET_NUM;

    cp->client_connections = CONF_UNSET_NUM;

    cp->redis = CONF_UNSET_NUM;
    cp->preconnect = CONF_UNSET_NUM;
    cp->auto_eject_hosts = CONF_UNSET_NUM;
    cp->server_connections = CONF_UNSET_NUM;
    cp->server_retry_timeout = CONF_UNSET_NUM;
    cp->server_failure_limit = CONF_UNSET_NUM;
    cp->quota = CONF_UNSET_NUM;
    cp->flow_control = CONF_UNSET_NUM;
    string_init(&cp->client_auth);

    array_null(&cp->server);

    cp->valid = 0;

    status = string_duplicate(&cp->name, name);
    if (status != NC_OK) {
        return status;
    }

    status = array_init(&cp->server, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        string_deinit(&cp->name);
        return status;
    }

    log_debug(LOG_VVERB, "init conf pool %p, '%.*s'", cp, name->len, name->data);

    return NC_OK;
}

static void
conf_pool_deinit(struct conf_pool *cp)
{
    string_deinit(&cp->name);
    string_deinit(&cp->password);
    string_deinit(&cp->client_auth);

    string_deinit(&cp->listen.pname);
    string_deinit(&cp->listen.name);

    while (array_n(&cp->server) != 0) {
        conf_server_deinit(array_pop(&cp->server));
    }
    array_deinit(&cp->server);

    log_debug(LOG_VVERB, "deinit conf pool %p", cp);
}

rstatus_t
conf_pool_each_transform(void *elem, void *data)
{
    rstatus_t status;
    struct conf_pool *cp = elem;
    struct array *server_pool = data;
    struct server_pool *sp;

    ASSERT(cp->valid);

    sp = array_push(server_pool);
    ASSERT(sp != NULL);

    sp->idx = array_idx(server_pool, sp);
    sp->ctx = NULL;

    sp->p_conn = NULL;
    sp->nc_conn_q = 0;
    TAILQ_INIT(&sp->c_conn_q);

    array_null(&sp->server);
    sp->ncontinuum = 0;
    sp->nserver_continuum = 0;
    sp->continuum = NULL;
    sp->nlive_server = 0;
    sp->next_rebuild = 0LL;

    sp->name = cp->name;
    sp->password = cp->password;
    sp->server_need_auth = sp->password.len ? 1 : 0;
    sp->client_auth = cp->client_auth;
    sp->client_need_auth = sp->client_auth.len ? 1 : 0;
    sp->addrstr = cp->listen.pname;
    sp->port = (uint16_t)cp->listen.port;

    sp->family = cp->listen.info.family;
    sp->addrlen = cp->listen.info.addrlen;
    sp->addr = (struct sockaddr *)&cp->listen.info.addr;

    sp->key_hash_type = cp->hash;
    sp->key_hash = hash_algos[cp->hash];
    sp->dist_type = cp->distribution;
    sp->hash_tag = cp->hash_tag;

    sp->redis = cp->redis ? 1 : 0;
    sp->timeout = cp->timeout;
    sp->backlog = cp->backlog;

    sp->client_connections = (uint32_t)cp->client_connections;

    sp->server_connections = (uint32_t)cp->server_connections;
    sp->server_retry_timeout = (int64_t)cp->server_retry_timeout * 1000LL;
    sp->server_failure_limit = (uint32_t)cp->server_failure_limit;
    sp->auto_eject_hosts = cp->auto_eject_hosts ? 1 : 0;
    sp->preconnect = cp->preconnect ? 1 : 0;

    /* quota assignment */
    sp->quota = cp->quota;
    sp->token = cp->quota;
    sp->flow_control = cp->flow_control;

    /* the connection will be set to 0 initially */
    sp->client_connections_cnt = 0;

    status = server_init(&sp->server, &cp->server, sp);
    if (status != NC_OK) {
        return status;
    }

    log_debug(LOG_VERB, "transform to pool %"PRIu32" '%.*s'", sp->idx,
              sp->name.len, sp->name.data);

    return NC_OK;
}

static void
conf_dump(struct conf *cf)
{
    uint32_t i, j, npool, nserver;
    struct conf_pool *cp;
    struct string *s;

    npool = array_n(&cf->pool);
    if (npool == 0) {
        return;
    }

    log_debug(LOG_VVERB, "%"PRIu32" pools in configuration file '%s'", npool,
              cf->fname);

    for (i = 0; i < npool; i++) {
        cp = array_get(&cf->pool, i);

        log_debug(LOG_VVERB, "%.*s", cp->name.len, cp->name.data);
        log_debug(LOG_VVERB, "%.*s", cp->password.len, cp->password.data);
        log_debug(LOG_VVERB, "  listen: %.*s",
                  cp->listen.pname.len, cp->listen.pname.data);
        log_debug(LOG_VVERB, "  timeout: %d", cp->timeout);
        log_debug(LOG_VVERB, "  backlog: %d", cp->backlog);
        log_debug(LOG_VVERB, "  hash: %d", cp->hash);
        log_debug(LOG_VVERB, "  hash_tag: \"%.*s\"", cp->hash_tag.len,
                  cp->hash_tag.data);
        log_debug(LOG_VVERB, "  distribution: %d", cp->distribution);
        log_debug(LOG_VVERB, "  client_connections: %d",
                  cp->client_connections);
        log_debug(LOG_VVERB, "  redis: %d", cp->redis);
        log_debug(LOG_VVERB, "  preconnect: %d", cp->preconnect);
        log_debug(LOG_VVERB, "  auto_eject_hosts: %d", cp->auto_eject_hosts);
        log_debug(LOG_VVERB, "  server_connections: %d",
                  cp->server_connections);
        log_debug(LOG_VVERB, "  server_retry_timeout: %d",
                  cp->server_retry_timeout);
        log_debug(LOG_VVERB, "  server_failure_limit: %d",
                  cp->server_failure_limit);
        log_debug(LOG_VVERB, "  quota: %d", cp->quota);
        log_debug(LOG_VVERB, "  flow_control: %d",
                  cp->flow_control);

        nserver = array_n(&cp->server);
        log_debug(LOG_VVERB, "  servers: %"PRIu32"", nserver);

        for (j = 0; j < nserver; j++) {
            s = array_get(&cp->server, j);
            log_debug(LOG_VVERB, "    %.*s", s->len, s->data);
        }
    }
}

static rstatus_t
conf_write(FILE *fh, const char *fmt, ...)
{
    size_t len, size, n;
    char buf[256];
    va_list args;

    len = 0;
    size = sizeof(buf);
    va_start(args, fmt);
    len = nc_vscnprintf(buf, size, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = fwrite(buf, len, (size_t)1, fh);
    if (n == 0) {
        return NC_ERROR;
    }

    return NC_OK;
}

void
conf_rewrite(struct context *ctx)
{
    uint32_t i, j, k, npool, nserver, nslave;
    struct conf *cf;
    struct conf_pool *cp;
    struct conf_server *cs, *slave;
    struct string true_str, false_str, bool_str;
    FILE *fh;
    char tmp_conf_file[256];

    cf = ctx->cf;

    npool = array_n(&cf->pool);
    if (npool == 0) {
        return;
    }

    log_debug(LOG_VVERB, "%"PRIu32" pools in configuration file '%s'", npool,
              cf->fname);

    /* open tmp conf file to rewrite */
    nc_snprintf(tmp_conf_file, 256, "%s.%d.tmp", cf->fname, (int) getpid());
    fh = fopen(tmp_conf_file,"w");
    if (fh == NULL) {
        log_error("conf: failed to open tmp configuration file to rewrite");
        return;
    }

    string_set_text(&true_str, "true");
    string_set_text(&false_str, "false");

    for (i = 0; i < npool; i++) {
        cp = array_get(&cf->pool, i);

        conf_write(fh, "%.*s:", cp->name.len, cp->name.data);
        conf_write(fh, "  listen: %.*s",
                  cp->listen.pname.len, cp->listen.pname.data);
        conf_write(fh, "  hash: %.*s",
                hash_strings[cp->hash].len, hash_strings[cp->hash].data);
        if (cp->hash_tag.len > 0) {
            conf_write(fh, "  hash_tag: \"%.*s\"", cp->hash_tag.len,
                    cp->hash_tag.data);
        }
        conf_write(fh, "  distribution: %.*s",
                dist_strings[cp->distribution].len, dist_strings[cp->distribution].data);

        bool_str = cp->preconnect ? true_str : false_str;
        conf_write(fh,"  preconnect: %.*s", bool_str.len, bool_str.data);

        bool_str = cp->auto_eject_hosts ? true_str : false_str;
        conf_write(fh, "  auto_eject_hosts: %.*s", bool_str.len, bool_str.data);

        bool_str = cp->redis ? true_str : false_str;
        conf_write(fh, "  redis: %.*s", bool_str.len, bool_str.data);
        if (cp->timeout >= 0) {
            conf_write(fh, "  timeout: %d", cp->timeout);
        }
        conf_write(fh, "  backlog: %d", cp->backlog);
        conf_write(fh, "  client_connections: %d",
                  cp->client_connections);
        conf_write(fh, "  server_connections: %d",
                  cp->server_connections);
        conf_write(fh, "  server_retry_timeout: %d",
                  cp->server_retry_timeout);
        conf_write(fh, "  server_failure_limit: %d",
                  cp->server_failure_limit);
        if(cp->password.len > 0) {
            conf_write(fh, "  password: %.*s", cp->password.len, cp->password.data);
        }
        if(cp->client_auth.len > 0) {
            conf_write(fh, "  client_auth: %.*s", cp->client_auth.len, cp->client_auth.data);
        }

        nserver = array_n(&cp->server);
        conf_write(fh, "  servers:");

        for (j = 0; j < nserver; j++) {
            cs = array_get(&cp->server, j);
            if (cs->name.len >= cs->pname.len
                    || nc_strncmp(cs->pname.data, cs->name.data, cs->name.len)) {
                conf_write(fh, "   - %.*s",
                        cs->name.len, cs->name.data);
                conf_write(fh, "     %.*s",
                        cs->pname.len, cs->pname.data);
            } else {
                conf_write(fh, "   - %.*s",
                        cs->pname.len, cs->pname.data);
            }

            nslave = array_n(&cs->slave);
            for (k = 0; k < nslave; k++) {
                slave = array_get(&cs->slave, k);
                conf_write(fh, "     %.*s",
                        slave->pname.len, slave->pname.data);
            }

        }

        /* write a empty line to conf when a pool is writen except the last one. */
        if (i < npool - 1) {
            conf_write(fh, "");
        }
    }

    fclose(fh);

    if (rename(tmp_conf_file, cf->fname) == -1) {
        log_error("Error moving temp conf file on the final destination: %s", strerror(errno));
        unlink(tmp_conf_file);
    }
}

static rstatus_t
conf_yaml_init(struct conf *cf)
{
    int rv;

    ASSERT(!cf->valid_parser);

    rv = fseek(cf->fh, 0L, SEEK_SET);
    if (rv < 0) {
        log_error("conf: failed to seek to the beginning of file '%s': %s",
                  cf->fname, strerror(errno));
        return NC_ERROR;
    }

    rv = yaml_parser_initialize(&cf->parser);
    if (!rv) {
        log_error("conf: failed (err %d) to initialize yaml parser",
                  cf->parser.error);
        return NC_ERROR;
    }

    yaml_parser_set_input_file(&cf->parser, cf->fh);
    cf->valid_parser = 1;

    return NC_OK;
}

static void
conf_yaml_deinit(struct conf *cf)
{
    if (cf->valid_parser) {
        yaml_parser_delete(&cf->parser);
        cf->valid_parser = 0;
    }
}

static rstatus_t
conf_token_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_token);

    rv = yaml_parser_scan(&cf->parser, &cf->token);
    if (!rv) {
        log_error("conf: failed (err %d) to scan next token", cf->parser.error);
        return NC_ERROR;
    }
    cf->valid_token = 1;

    return NC_OK;
}

static void
conf_token_done(struct conf *cf)
{
    ASSERT(cf->valid_parser);

    if (cf->valid_token) {
        yaml_token_delete(&cf->token);
        cf->valid_token = 0;
    }
}

static rstatus_t
conf_event_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_event);

    rv = yaml_parser_parse(&cf->parser, &cf->event);
    if (!rv) {
        log_error("conf: failed (err %d) to get next event", cf->parser.error);
        return NC_ERROR;
    }
    cf->valid_event = 1;

    return NC_OK;
}

static void
conf_event_done(struct conf *cf)
{
    if (cf->valid_event) {
        yaml_event_delete(&cf->event);
        cf->valid_event = 0;
    }
}

static rstatus_t
conf_push_scalar(struct conf *cf)
{
    rstatus_t status;
    struct string *value;
    uint8_t *scalar;
    uint32_t scalar_len;

    scalar = cf->event.data.scalar.value;
    scalar_len = (uint32_t)cf->event.data.scalar.length;

    log_debug(LOG_VVERB, "push '%.*s'", scalar_len, scalar);

    value = array_push(&cf->arg);
    if (value == NULL) {
        return NC_ENOMEM;
    }
    string_init(value);

    status = string_copy(value, scalar, scalar_len);
    if (status != NC_OK) {
        array_pop(&cf->arg);
        return status;
    }

    return NC_OK;
}

static void
conf_pop_scalar(struct conf *cf)
{
    struct string *value;

    value = array_pop(&cf->arg);
    log_debug(LOG_VVERB, "pop '%.*s'", value->len, value->data);
    string_deinit(value);
}

static rstatus_t
conf_handler(struct conf *cf, void *data)
{
    struct command *cmd;
    struct string *key, *value;
    uint32_t narg;

    if (array_n(&cf->arg) == 1) {
        value = array_top(&cf->arg);
        log_debug(LOG_VVERB, "conf handler on '%.*s'", value->len, value->data);
        return conf_pool_init(data, value);
    }

    narg = array_n(&cf->arg);
    value = array_get(&cf->arg, narg - 1);
    key = array_get(&cf->arg, narg - 2);

    log_debug(LOG_VVERB, "conf handler on %.*s: %.*s", key->len, key->data,
              value->len, value->data);

    for (cmd = conf_commands; cmd->name.len != 0; cmd++) {
        char *rv;

        if (string_compare(key, &cmd->name) != 0) {
            continue;
        }

        rv = cmd->set(cf, cmd, data);
        if (rv != CONF_OK) {
            log_error("conf: directive \"%.*s\" %s", key->len, key->data, rv);
            return NC_ERROR;
        }

        return NC_OK;
    }

    log_error("conf: directive \"%.*s\" is unknown", key->len, key->data);

    return NC_ERROR;
}

static rstatus_t
conf_begin_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next begin event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_MAPPING_START_EVENT:
            ASSERT(cf->depth < CONF_MAX_DEPTH);
            cf->depth++;
            done = true;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);

    } while (!done);

    return NC_OK;
}

static rstatus_t
conf_end_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next end event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done);

    conf_yaml_deinit(cf);

    return NC_OK;
}

static rstatus_t
conf_parse_core(struct conf *cf, void *data)
{
    rstatus_t status;
    bool done, leaf, new_pool;

    ASSERT(cf->sound);

    status = conf_event_next(cf);
    if (status != NC_OK) {
        return status;
    }

    log_debug(LOG_VVERB, "next event %d depth %"PRIu32" seq %d", cf->event.type,
              cf->depth, cf->seq);

    done = false;
    leaf = false;
    new_pool = false;

    switch (cf->event.type) {
    case YAML_MAPPING_END_EVENT:
        cf->depth--;
        if (cf->depth == 1) {
            conf_pop_scalar(cf);
        } else if (cf->depth == 0) {
            done = true;
        }
        break;

    case YAML_MAPPING_START_EVENT:
        cf->depth++;
        break;

    case YAML_SEQUENCE_START_EVENT:
        cf->seq = 1;
        break;

    case YAML_SEQUENCE_END_EVENT:
        conf_pop_scalar(cf);
        cf->seq = 0;
        break;

    case YAML_SCALAR_EVENT:
        status = conf_push_scalar(cf);
        if (status != NC_OK) {
            break;
        }

        /* take appropriate action */
        if (cf->seq) {
            /* for a sequence, leaf is at CONF_MAX_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH);
            leaf = true;
        } else if (cf->depth == CONF_ROOT_DEPTH) {
            /* create new conf_pool */
            data = array_push(&cf->pool);
            if (data == NULL) {
                status = NC_ENOMEM;
                break;
           }
           new_pool = true;
        } else if (array_n(&cf->arg) == cf->depth + 1) {
            /* for {key: value}, leaf is at CONF_MAX_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH);
            leaf = true;
        }
        break;

    default:
        NOT_REACHED();
        break;
    }

    conf_event_done(cf);

    if (status != NC_OK) {
        return status;
    }

    if (done) {
        /* terminating condition */
        return NC_OK;
    }

    if (leaf || new_pool) {
        status = conf_handler(cf, data);

        if (leaf) {
            conf_pop_scalar(cf);
            if (!cf->seq) {
                conf_pop_scalar(cf);
            }
        }

        if (status != NC_OK) {
            return status;
        }
    }

    return conf_parse_core(cf, data);
}

static rstatus_t
conf_parse(struct conf *cf)
{
    rstatus_t status;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(array_n(&cf->arg) == 0);

    status = conf_begin_parse(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_parse_core(cf, NULL);
    if (status != NC_OK) {
        return status;
    }

    status = conf_end_parse(cf);
    if (status != NC_OK) {
        return status;
    }

    cf->parsed = 1;

    return NC_OK;
}

/*****************************************************************************************
++ Open configuration file and initialize spaces and values for conf instance newly created
******************************************************************************************/
static struct conf *
conf_open(char *filename)
{
    rstatus_t status;
    struct conf *cf;
    FILE *fh;

    // open configuration file
    fh = fopen(filename, "r");
    if (fh == NULL) {
        log_error("conf: failed to open configuration '%s': %s", filename,
                  strerror(errno));
        return NULL;
    }

    // allocate space for conf structure
    cf = nc_alloc(sizeof(*cf));
    if (cf == NULL) {
        fclose(fh);
        return NULL;
    }

    // initialize CONF_DEFAULT_ARGS(3) string elements for cf->arg
    status = array_init(&cf->arg, CONF_DEFAULT_ARGS, sizeof(struct string));
    if (status != NC_OK) {
        nc_free(cf);
        fclose(fh);
        return NULL;
    }

    // initialize CONF_DEFAULT_POOL(8) for cf->pool
    status = array_init(&cf->pool, CONF_DEFAULT_POOL, sizeof(struct conf_pool));
    if (status != NC_OK) {
        array_deinit(&cf->arg);
        nc_free(cf);
        fclose(fh);
        return NULL;
    }

    cf->fname = filename;
    cf->fh = fh;
    cf->depth = 0;
    /* parser, event, and token are initialized later */
    cf->seq = 0;
    cf->valid_parser = 0;
    cf->valid_event = 0;
    cf->valid_token = 0;
    cf->sound = 0;
    cf->parsed = 0;
    cf->valid = 0;

    log_debug(LOG_VVERB, "opened conf '%s'", filename);

    return cf;
}

static rstatus_t
conf_validate_document(struct conf *cf)
{
    rstatus_t status;
    uint32_t count;
    bool done;

    // reset cf->fh to start and initialize yaml parser
    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    count = 0;
    done = false;
    do {
        yaml_document_t document;
        yaml_node_t *node;
        int rv;

        rv = yaml_parser_load(&cf->parser, &document);
        if (!rv) {
            log_error("conf: failed (err %d) to get the next yaml document",
                      cf->parser.error);
            conf_yaml_deinit(cf);
            return NC_ERROR;
        }

        node = yaml_document_get_root_node(&document);
        if (node == NULL) {
            done = true;
        } else {
            count++;
        }

        yaml_document_delete(&document);
    } while (!done);

    conf_yaml_deinit(cf);

    if (count != 1) {
        log_error("conf: '%s' must contain only 1 document; found %"PRIu32" "
                  "documents", cf->fname, count);
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
conf_validate_tokens(struct conf *cf)
{
    rstatus_t status;
    bool done, error;
    int type;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    error = false;
    do {
        status = conf_token_next(cf);
        if (status != NC_OK) {
            return status;
        }
        type = cf->token.type;

        switch (type) {
        case YAML_NO_TOKEN:
            error = true;
            log_error("conf: no token (%d) is disallowed", type);
            break;

        case YAML_VERSION_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: version directive token (%d) is disallowed", type);
            break;

        case YAML_TAG_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: tag directive token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_START_TOKEN:
            error = true;
            log_error("conf: document start token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_END_TOKEN:
            error = true;
            log_error("conf: document end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_START_TOKEN:
            error = true;
            log_error("conf: flow sequence start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_END_TOKEN:
            error = true;
            log_error("conf: flow sequence end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_START_TOKEN:
            error = true;
            log_error("conf: flow mapping start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_END_TOKEN:
            error = true;
            log_error("conf: flow mapping end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_ENTRY_TOKEN:
            error = true;
            log_error("conf: flow entry token (%d) is disallowed", type);
            break;

        case YAML_ALIAS_TOKEN:
            error = true;
            log_error("conf: alias token (%d) is disallowed", type);
            break;

        case YAML_ANCHOR_TOKEN:
            error = true;
            log_error("conf: anchor token (%d) is disallowed", type);
            break;

        case YAML_TAG_TOKEN:
            error = true;
            log_error("conf: tag token (%d) is disallowed", type);
            break;

        case YAML_BLOCK_SEQUENCE_START_TOKEN:
        case YAML_BLOCK_MAPPING_START_TOKEN:
        case YAML_BLOCK_END_TOKEN:
        case YAML_BLOCK_ENTRY_TOKEN:
            break;

        case YAML_KEY_TOKEN:
        case YAML_VALUE_TOKEN:
        case YAML_SCALAR_TOKEN:
            break;

        case YAML_STREAM_START_TOKEN:
            break;

        case YAML_STREAM_END_TOKEN:
            done = true;
            log_debug(LOG_VVERB, "conf '%s' has valid tokens", cf->fname);
            break;

        default:
            error = true;
            log_error("conf: unknown token (%d) is disallowed", type);
            break;
        }

        conf_token_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? NC_OK : NC_ERROR;
}

static rstatus_t
conf_validate_structure(struct conf *cf)
{
    rstatus_t status;
    int type, depth;
    uint32_t i, count[CONF_MAX_DEPTH + 1];
    bool done, error, seq;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    error = false;
    seq = false;
    depth = 0;
    for (i = 0; i < CONF_MAX_DEPTH + 1; i++) {
        count[i] = 0;
    }

    /*
     * Validate that the configuration conforms roughly to the following
     * yaml tree structure:
     *
     * keyx:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     *
     * keyy:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     */
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        type = cf->event.type;

        log_debug(LOG_VVERB, "next event %d depth %d seq %d", type, depth, seq);

        switch (type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_MAPPING_START_EVENT:
            if (depth == CONF_ROOT_DEPTH && count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has more than one \"key:value\" at depth"
                          " %d", cf->fname, depth);
            } else if (depth >= CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has a depth greater than %d", cf->fname,
                          CONF_MAX_DEPTH);
            }
            depth++;
            break;

        case YAML_MAPPING_END_EVENT:
            if (depth == CONF_MAX_DEPTH) {
                if (seq) {
                    seq = false;
                } else {
                    error = true;
                    log_error("conf: '%s' missing sequence directive at depth "
                              "%d", cf->fname, depth);
                }
            }
            depth--;
            count[depth] = 0;
            break;

        case YAML_SEQUENCE_START_EVENT:
            if (seq) {
                error = true;
                log_error("conf: '%s' has more than one sequence directive",
                          cf->fname);
            } else if (depth != CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has sequence at depth %d instead of %d",
                          cf->fname, depth, CONF_MAX_DEPTH);
            } else if (count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has invalid \"key:value\" at depth %d",
                          cf->fname, depth);
            }
            seq = true;
            break;

        case YAML_SEQUENCE_END_EVENT:
            ASSERT(depth == CONF_MAX_DEPTH);
            count[depth] = 0;
            break;

        case YAML_SCALAR_EVENT:
            if (depth == 0) {
                error = true;
                log_error("conf: '%s' has invalid empty \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_ROOT_DEPTH && count[depth] != 0) {
                error = true;
                log_error("conf: '%s' has invalid mapping \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_MAX_DEPTH && count[depth] == 2) {
                /* found a "key: value", resetting! */
                count[depth] = 0;
            }
            count[depth]++;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? NC_OK : NC_ERROR;
}

static rstatus_t
conf_pre_validate(struct conf *cf)
{
    rstatus_t status;

    // check configuration file by document-based model
    status = conf_validate_document(cf);
    if (status != NC_OK) {
        return status;
    }

    // check configuration file by token-based model
    status = conf_validate_tokens(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_validate_structure(cf);
    if (status != NC_OK) {
        return status;
    }

    cf->sound = 1;

    return NC_OK;
}

static int
conf_server_name_cmp(const void *t1, const void *t2)
{
    const struct conf_server *s1 = t1, *s2 = t2;

    return string_compare(&s1->name, &s2->name);
}

static int
conf_pool_name_cmp(const void *t1, const void *t2)
{
    const struct conf_pool *p1 = t1, *p2 = t2;

    return string_compare(&p1->name, &p2->name);
}

static int
conf_pool_listen_cmp(const void *t1, const void *t2)
{
    const struct conf_pool *p1 = t1, *p2 = t2;

    return string_compare(&p1->listen.pname, &p2->listen.pname);
}

static rstatus_t
conf_validate_server(struct conf *cf, struct conf_pool *cp)
{
    uint32_t i, nserver;
    bool valid;

    NUT_NOTUSED(cf);

    nserver = array_n(&cp->server);
    if (nserver == 0) {
        log_error("conf: pool '%.*s' has no servers", cp->name.len,
                  cp->name.data);
        return NC_ERROR;
    }

    /*
     * Disallow duplicate servers - servers with identical "host:port:weight"
     * or "name" combination are considered as duplicates. When server name
     * is configured, we only check for duplicate "name" and not for duplicate
     * "host:port:weight"
     */
    array_sort(&cp->server, conf_server_name_cmp);
    for (valid = true, i = 0; i < nserver - 1; i++) {
        struct conf_server *cs1, *cs2;

        cs1 = array_get(&cp->server, i);
        cs2 = array_get(&cp->server, i + 1);

        if (string_compare(&cs1->name, &cs2->name) == 0) {
            log_error("conf: pool '%.*s' has servers with same name '%.*s'",
                      cp->name.len, cp->name.data, cs1->name.len, 
                      cs1->name.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
conf_validate_pool(struct conf *cf, struct conf_pool *cp)
{
    rstatus_t status;

    ASSERT(!cp->valid);
    ASSERT(!string_empty(&cp->name));

    if (!cp->listen.valid) {
        log_error("conf: directive \"listen:\" is missing");
        return NC_ERROR;
    }

    /* set default values for unset directives */

    if (cp->distribution == CONF_UNSET_DIST) {
        cp->distribution = CONF_DEFAULT_DIST;
    }

    if (cp->hash == CONF_UNSET_HASH) {
        cp->hash = CONF_DEFAULT_HASH;
    }

    if (cp->timeout == CONF_UNSET_NUM) {
        cp->timeout = CONF_DEFAULT_TIMEOUT;
    }

    if (cp->backlog == CONF_UNSET_NUM) {
        cp->backlog = CONF_DEFAULT_LISTEN_BACKLOG;
    }

    if (cp->client_connections == CONF_UNSET_NUM) {
        cp->client_connections = CONF_DEFAULT_CLIENT_CONNECTIONS;
    }

    if (cp->redis == CONF_UNSET_NUM) {
        cp->redis = CONF_DEFAULT_REDIS;
    }

    if (cp->preconnect == CONF_UNSET_NUM) {
        cp->preconnect = CONF_DEFAULT_PRECONNECT;
    }

    if (cp->auto_eject_hosts == CONF_UNSET_NUM) {
        cp->auto_eject_hosts = CONF_DEFAULT_AUTO_EJECT_HOSTS;
    }

    if (cp->server_connections == CONF_UNSET_NUM) {
        cp->server_connections = CONF_DEFAULT_SERVER_CONNECTIONS;
    } else if (cp->server_connections == 0) {
        log_error("conf: directive \"server_connections:\" cannot be 0");
        return NC_ERROR;
    }

    if (cp->server_retry_timeout == CONF_UNSET_NUM) {
        cp->server_retry_timeout = CONF_DEFAULT_SERVER_RETRY_TIMEOUT;
    }

    if (cp->server_failure_limit == CONF_UNSET_NUM) {
        cp->server_failure_limit = CONF_DEFAULT_SERVER_FAILURE_LIMIT;
    }

    if (cp->flow_control == CONF_UNSET_NUM) {
        cp->flow_control = CONF_DEFAULT_FLOW_CONTROL;
    }

    if (cp->quota == CONF_UNSET_NUM) {
        cp->quota = CONF_DEFAULT_QUATO;
    }
    status = conf_validate_server(cf, cp);
    if (status != NC_OK) {
        return status;
    }

    cp->valid = 1;

    return NC_OK;
}

static rstatus_t
conf_post_validate(struct conf *cf)
{
    rstatus_t status;
    uint32_t i, npool;
    bool valid;

    ASSERT(cf->sound && cf->parsed);
    ASSERT(!cf->valid);

    npool = array_n(&cf->pool);
    if (npool == 0) {
        log_error("conf: '%.*s' has no pools", cf->fname);
        return NC_ERROR;
    }

    /* validate pool */
    for (i = 0; i < npool; i++) {
        struct conf_pool *cp = array_get(&cf->pool, i);

        status = conf_validate_pool(cf, cp);
        if (status != NC_OK) {
            return status;
        }
    }

    /* disallow pools with duplicate listen: key values */
    array_sort(&cf->pool, conf_pool_listen_cmp);
    for (valid = true, i = 0; i < npool - 1; i++) {
        struct conf_pool *p1, *p2;

        p1 = array_get(&cf->pool, i);
        p2 = array_get(&cf->pool, i + 1);

        if (string_compare(&p1->listen.pname, &p2->listen.pname) == 0) {
            log_error("conf: pools '%.*s' and '%.*s' have the same listen "
                      "address '%.*s'", p1->name.len, p1->name.data,
                      p2->name.len, p2->name.data, p1->listen.pname.len,
                      p1->listen.pname.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    /* disallow pools with duplicate names */
    array_sort(&cf->pool, conf_pool_name_cmp);
    for (valid = true, i = 0; i < npool - 1; i++) {
        struct conf_pool *p1, *p2;

        p1 = array_get(&cf->pool, i);
        p2 = array_get(&cf->pool, i + 1);

        if (string_compare(&p1->name, &p2->name) == 0) {
            log_error("conf: '%s' has pools with same name %.*s'", cf->fname,
                      p1->name.len, p1->name.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    return NC_OK;
}

struct conf *
conf_create(char *filename)
{
    rstatus_t status;
    struct conf *cf;

    // open configuration file and initialize conf instance
    cf = conf_open(filename);
    if (cf == NULL) {
        return NULL;
    }

    /* validate configuration file before parsing */
    status = conf_pre_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    /* parse the configuration file */
    status = conf_parse(cf);
    if (status != NC_OK) {
        goto error;
    }

    /* validate parsed configuration */
    status = conf_post_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    conf_dump(cf);

    fclose(cf->fh);
    cf->fh = NULL;

    return cf;

error:
    fclose(cf->fh);
    cf->fh = NULL;
    conf_destroy(cf);
    return NULL;
}

void
conf_destroy(struct conf *cf)
{
    while (array_n(&cf->arg) != 0) {
        conf_pop_scalar(cf);
    }
    array_deinit(&cf->arg);

    while (array_n(&cf->pool) != 0) {
        conf_pool_deinit(array_pop(&cf->pool));
    }
    array_deinit(&cf->pool);

    nc_free(cf);
}

char *
conf_set_string(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}

char *
conf_set_password(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;
    const char const invalid_password_char[] = {',', '\'', '"', '#', '%', '&', 
        '!', '>', '[', ']', '{', '}', '~', ' ', '\n', '|'};

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);
    if(value->len > CONF_MAX_PASSWORD_LEN ||
            value->len < CONF_MIN_PASSWORD_LEN) {
        return "too long";
    }
    if(string_pbrk(value, invalid_password_char, sizeof(invalid_password_char)/sizeof(char))) {
        return "has an invalid char in password string";
    }

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}

char *
conf_set_listen(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct string *value;
    struct conf_listen *field;
    uint8_t *p, *name;
    uint32_t namelen;

    p = conf;
    field = (struct conf_listen *)(p + cmd->offset);

    if (field->valid == 1) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(&field->pname, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    if (value->data[0] == '/') {
        name = value->data;
        namelen = value->len;
    } else {
        uint8_t *q, *start, *port;
        uint32_t portlen;

        /* parse "hostname:port" from the end */
        p = value->data + value->len - 1;
        start = value->data;
        q = nc_strrchr(p, start, ':');
        if (q == NULL) {
            return "has an invalid \"hostname:port\" format string";
        }

        port = q + 1;
        portlen = (uint32_t)(p - port + 1);

        p = q - 1;

        name = start;
        namelen = (uint32_t)(p - start + 1);

        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port\" format string";
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = nc_resolve(&field->name, field->port, &field->info);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    field->valid = 1;

    return CONF_OK;
}

static char *
conf_parse_server(struct conf_server *field, struct string *value, uint8_t *name, uint32_t namelen)
{
    rstatus_t status;
    uint8_t *p, *q, *start;
    uint8_t *addr, *port, *weight;
    uint32_t k, delimlen, addrlen, portlen, weightlen;
    struct string address;
    char delim = ':';

    string_init(&address);

    /* parse "hostname:port:weight [name]" or "/path/unix_socket:weight [name]" from the end */
    p = value->data + value->len - 1;
    start = value->data;
    addr = NULL;
    addrlen = 0;
    weight = NULL;
    weightlen = 0;
    port = NULL;
    portlen = 0;

    delimlen = value->data[0] == '/' ? 1 : 2;

    for (k = 0; k < delimlen; k++) {
        q = nc_strrchr(p, start, delim);
        if (q == NULL) {
            break;
        }

        switch (k) {
        case 0:
            weight = q + 1;
            weightlen = (uint32_t)(p - weight + 1);
            break;

        case 1:
            port = q + 1;
            portlen = (uint32_t)(p - port + 1);
            break;

        default:
            NOT_REACHED();
        }

        p = q - 1;
    }

    if (k != delimlen) {
        return "has an invalid \"hostname:port:weight [name]\"or \"/path/unix_socket:weight [name]\" format string";
    }

    status = string_copy(&field->pname, value->data, value->len);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    addr = start;
    addrlen = (uint32_t)(p - start + 1);

    field->weight = nc_atoi(weight, weightlen);
    if (field->weight < 0) {
        return "has an invalid weight in \"hostname:port:weight [name]\" format string";
    }

    if (value->data[0] != '/') {
        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port:weight [name]\" format string";
        }
    }

    if (name == NULL) {
        /*
         * To maintain backward compatibility with libmemcached, we don't
         * include the port as the part of the input string to the consistent
         * hashing algorithm, when it is equal to 11211.
         */
        if (field->port == CONF_DEFAULT_KETAMA_PORT) {
            name = addr;
            namelen = addrlen;
        } else {
            name = addr;
            namelen = addrlen + 1 + portlen;
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = string_copy(&address, addr, addrlen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = nc_resolve(&address, field->port, &field->info);
    if (status != NC_OK) {
        string_deinit(&address);
        return CONF_ERROR;
    }

    string_deinit(&address);
    field->valid = 1;

    return CONF_OK;
}

char *
conf_add_server(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct array *a;
    struct string *value, server_value;
    struct conf_server *master, *field;
    uint8_t *p, *start, *value_end, *name;
    uint32_t namelen;
    char server_delim, detail_delim;
    char *rv;

    server_delim = ' ';
    detail_delim = ':';

    name = NULL;
    namelen = 0;

    p = conf;
    a = (struct array *)(p + cmd->offset);

    master = array_push(a);
    if (master == NULL) {
        return CONF_ERROR;
    }

    value = array_top(&cf->arg);

    /* find the name string, if the value has.
     * if the value has more than one string splited by ' ',
     * and the first string has no ':' in it, we think it's a name
     */

    /* find name string in value */
    start = value->data;
    value_end = value->data + value->len - 1;
    p = nc_strchr(start, value_end, server_delim);
    if (p != NULL && nc_strchr(start, p, detail_delim) == NULL) {
        name = start;
        namelen = p - start;
        start = p + 1;
    }

    conf_server_init(master);
    status = array_init(&master->slave, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        array_pop(a);
        return CONF_ERROR;
    }

    /* initialize server struct from config string.
     * the first string is the master, the other is slave.
     */
    while (start < value_end) {
        p = nc_strchr(start, value_end, server_delim);
        server_value.data = start;
        if (p == NULL) {
            server_value.len = value_end - start + 1;
        }
        else {
            server_value.len = p - start;
        }
        start += server_value.len + 1;

        /* if master->name size is 0, means it's the first for master info */
        if (master->name.len == 0) {
            field = master;
        }
        else {
            field = array_push(&master->slave);
            if (field == NULL) {
                return CONF_ERROR;
            }
            conf_server_init(field);
            field->master = master;
        }
        rv = conf_parse_server(field, &server_value, name, namelen);
        if (rv != CONF_OK) {
            return rv;
        }
    }

    if (master->name.len == 0) {
        return CONF_ERROR;
    }

    return CONF_OK;
}

char *
conf_set_num(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int num, *np;
    struct string *value;

    p = conf;
    np = (int *)(p + cmd->offset);

    if (*np != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    num = nc_atoi(value->data, value->len);
    if (num < 0) {
        return "is not a number";
    }

    *np = num;

    return CONF_OK;
}

char *
conf_set_bool(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int *bp;
    struct string *value, true_str, false_str;

    p = conf;
    bp = (int *)(p + cmd->offset);

    if (*bp != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);
    string_set_text(&true_str, "true");
    string_set_text(&false_str, "false");

    if (string_compare(value, &true_str) == 0) {
        *bp = 1;
    } else if (string_compare(value, &false_str) == 0) {
        *bp = 0;
    } else {
        return "is not \"true\" or \"false\"";
    }

    return CONF_OK;
}

char *
conf_set_hash(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    hash_type_t *hp;
    struct string *value, *hash;

    p = conf;
    hp = (hash_type_t *)(p + cmd->offset);

    if (*hp != CONF_UNSET_HASH) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (hash = hash_strings; hash->len != 0; hash++) {
        if (string_compare(value, hash) != 0) {
            continue;
        }

        *hp = hash - hash_strings;

        return CONF_OK;
    }

    return "is not a valid hash";
}

char *
conf_set_distribution(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    dist_type_t *dp;
    struct string *value, *dist;

    p = conf;
    dp = (dist_type_t *)(p + cmd->offset);

    if (*dp != CONF_UNSET_DIST) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (dist = dist_strings; dist->len != 0; dist++) {
        if (string_compare(value, dist) != 0) {
            continue;
        }

        *dp = dist - dist_strings;

        return CONF_OK;
    }

    return "is not a valid distribution";
}

char *
conf_set_hashtag(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    if (value->len != 2) {
        return "is not a valid hash tag string with two characters";
    }

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}
