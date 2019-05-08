/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "cluster.h"
#include "slowlog.h"
#include "bio.h"
#include "latency.h"
#include "git_version.h"
#include "rocks.h"
#include "lzf.h"
#include "rdb.h"
#include "dict.h"
#include "const.h"

#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>
#include <sys/socket.h>
#include <unistd.h>  // sysconf() - get CPU count

/*
** return C_OK if success
** return C_ERR if failed
*/
int rockssdscatlen(sds *s, const void *t, size_t len)
{
    sds newval = sdscatlen(*s, t, len);
    if (!newval) {
        return C_ERR;
    } else {
        *s = newval;
        return C_OK;
    }
}

int rocksNeedExchangeKey(sds key) {
    if (sdslen(key) > ROCKSDB_EXCHG_KEY_MAXLEN) {
        return 1;
    } else {
        return 0;
    }
}

int rocksRemoveKey(redisDb *db, 
                   unsigned long long desno, 
                   sds key, 
                   unsigned type)
{
    int rc = C_OK;    
    sds diskkey = sdsempty();

    if (rocksNeedExchangeKey(key)) {
        diskkey = sdscatfmt(diskkey, "%i_%u_%U", db->id, type, desno);
    } else {
        diskkey = sdscatfmt(diskkey, "%i_%u_%S", db->id, type, key);
    }

    rc = del_from_rocksdb(diskkey, sdslen(diskkey));
    if (rc != C_OK) {
        serverLog(LL_WARNING, "remove key(%s) from disk failed", diskkey);
    }

    sdsfree(diskkey);

    return rc;
}

int needSaveObjectOnDisk(int flag)
{
    size_t zmalloc_used = 0;
    size_t zmalloc_rss = 0;

    //if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) {
    //    serverLog(LL_DEBUG, "not need to save data in child progress");
    //    return 0;
    //}

    if (server.use_disk_store != DISK_STORAGE_USE) {
        return 0;
    }

    flag &= DISK_STORE_FAST;

    zmalloc_used = zmalloc_used_memory();          
    zmalloc_rss = zmalloc_get_rss();
    if (zmalloc_rss > server.membuf_size_upper) {
        if (zmalloc_used > server.membuf_size) {
            return 1;
        }
    } else {
        if (zmalloc_used > server.membuf_size) {
            return 1;
        }
    }

    return 0;
}

#if 0
int dictValNeedLoadIntoMemory(dictEntry *de)
{
    long long tdelta = 1;
    long long acchz = 0;
    
    if (de->v_ondisk == VAL_NOT_ON_DISK) {
        return 0;
    }

    if (server.dstore_need_loadmem_hz <= 0) {
        return 1;
    }

    tdelta = server.event_proc_loop_start_ms - de->v_wrdisk_time;
    
    if (tdelta >= 1000) {
        acchz = (de->v_diskacc_cnt * 1000) / tdelta;
    } else {
        acchz = de->v_diskacc_cnt;
    }

    if (acchz >= server.dstore_need_loadmem_hz) {
        return 1;
    } 

    return 0;
}
#endif

int getValTypeByEntry(dictEntry *de) 
{
    robj *val = NULL;
    
    if (dictIsEntryValOnDisk(de)) {
        return de->v_type;
    } else {
        val = dictGetVal(de);
        return val->type;
    }
}

dictEntry *getLruBestKey(redisDb *db)
{
    int k = 0;
    int count = DISK_STORE_KEY_LRU_GET_LOOP;
    dictEntry *de = NULL;
    dict *dbdict = db->dict;
    struct evictionPoolEntry *pool = NULL;
     
    pool = db->eviction_pool;
    while(count--) {
        evictionPoolPopulateWithDstoreCheck(dbdict, db->dict, 
                                            db->eviction_pool, 1);
        /* Go backward from best to worst element to evict. */
        for (k = MAXMEMORY_EVICTION_POOL_SIZE-1; k >= 0; k--) {
            if (pool[k].key == NULL) {
                continue;
            }

            de = dictFind(db->dict, pool[k].key);

            /* Remove the entry from the pool. */
            sdsfree(pool[k].key);                     
           
            /* Shift all elements on its right to left. */
            memmove(pool + k, pool + k + 1,
                sizeof(pool[0]) * (MAXMEMORY_EVICTION_POOL_SIZE - k - 1));
            /* Clear the element on the right which is empty
             * since we shifted one position to the left.  */
            pool[MAXMEMORY_EVICTION_POOL_SIZE-1].key = NULL;
            pool[MAXMEMORY_EVICTION_POOL_SIZE-1].idle = 0;

            /* If the key exists, is our pick. Otherwise it is
             * a ghost and we need to try the next element. */
            if (de) {
                if (dictFilterSelectedDe(de)) {
                    return de;
                } else {
                    continue;
                }                
            } else {
                /* Ghost... */
                continue;
            }
        }
    }

    return de;
}

/*
** return C_OK if success
** return C_ERR if failed
*/
int rocksSaveType(sds *psaveval, unsigned char type)
{
    return rockssdscatlen(psaveval, &type, 1);
}

/*
** return C_OK if success
** return C_ERR if failed
*/
int rocksSaveObjectType(sds *psaveval, robj *o) 
{        
    switch (o->type) {
    case OBJ_STRING:        
        return rocksSaveType(psaveval, RDB_TYPE_STRING);
        break;
    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            return rocksSaveType(psaveval, RDB_TYPE_LIST_QUICKLIST);
        } else {
            serverLog(LL_WARNING, "Unknown list encoding");
        }
        break;
    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET) {
            return rocksSaveType(psaveval, RDB_TYPE_SET_INTSET);
        } else if (o->encoding == OBJ_ENCODING_HT) {
            return rocksSaveType(psaveval, RDB_TYPE_SET);
        } else {
            serverLog(LL_WARNING, "Unknown set encoding");
        }
        break;
    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            return rocksSaveType(psaveval, RDB_TYPE_ZSET_ZIPLIST);
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            return rocksSaveType(psaveval, RDB_TYPE_ZSET);
        } else {
            serverLog(LL_WARNING, "Unknown sorted set encoding");
        }
        break;
    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            return rocksSaveType(psaveval, RDB_TYPE_HASH_ZIPLIST);
        } else if (o->encoding == OBJ_ENCODING_HT) {
            return rocksSaveType(psaveval, RDB_TYPE_HASH);
        } else {
            serverLog(LL_WARNING, "Unknown hash encoding");
        }
        break;
    default:
        serverLog(LL_WARNING, "Unknown object type");
        break;
    }
    
    return C_ERR; /* avoid warning */
}

/* 
** return -1 if failed
** return bytes if success
*/
int rocksSaveLen(sds *psaveval, uint32_t len)
{
    int rc = C_OK;
    unsigned char buf[2];
    size_t nwritten = 0;

    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        rc = rockssdscatlen(psaveval, buf, 1);
        if (rc != C_OK) {
            return -1;
        } else {
            nwritten = 1;
        }
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        rc = rockssdscatlen(psaveval, buf, 2);
        if (rc != C_OK) {
            return -1;
        } else {
            nwritten = 2;
        }
    } else {
        /* Save a 32 bit len */
        buf[0] = (RDB_32BITLEN << 6);
        rc = rockssdscatlen(psaveval, buf, 1);
        if (rc != C_OK) {
            return -1;
        }         
        
        len = htonl(len);
        rc = rockssdscatlen(psaveval, &len, 4);
        if (rc != C_OK) {
            return -1;
        } 

        nwritten = 1 + 4;
    }
    
    return nwritten;
}

/* 
** return -1 if failed
** return bytes if success
*/
ssize_t rocksSaveLzfBlob(sds *psaveval, void *data, 
                         size_t compress_len, size_t original_len) 
{
    unsigned char byte = 0;
    ssize_t len = 0;
    ssize_t nwritten = 0;
    int rc = C_OK;

    /* Data compressed! Let's save it on disk */
    byte = (RDB_ENCVAL << 6) | RDB_ENC_LZF;

    rc = rockssdscatlen(psaveval, &byte, 1);
    if (rc != C_OK) {
        return -1;
    }
    nwritten += 1;

    len = rocksSaveLen(psaveval, compress_len);
    if (len == -1) {
        return -1;
    }
    nwritten += len;

    len = rocksSaveLen(psaveval, original_len);
    if (len == -1) {
        return -1;
    }
    nwritten += len;

    rc = rockssdscatlen(psaveval, data, compress_len);
    if (rc != C_OK) {
        return -1;
    }
    nwritten += compress_len;
    
    return nwritten;
}

/* 
** return 0 if no need to compression 
** return -1 if do compression failed
** return bytes if do compression success
*/
ssize_t rocksSaveLzfStringObject(sds *psaveval, unsigned char *s, size_t len) {
    size_t comprlen = 0;
    size_t outlen = 0;
    ssize_t nwritten = 0;
    void *out = NULL;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= LZF_COMPRESS_SRC_MIN_LENGTH) {
        return 0;
    }
    
    outlen = len + LZF_COMPRESS_DST_APPEND_LENGTH;
    out = zmalloc(outlen + 1);
    if (!out) {
        return 0;
    }
    
    comprlen = lzf_compress(s, len, out, outlen);
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }
    
    nwritten = rocksSaveLzfBlob(psaveval, out, comprlen, len);    
    zfree(out);
    
    return nwritten;
}

/* 
** return -1 if failed
** return bytes if success
*/
ssize_t rocksSaveRawString(sds *psaveval, unsigned char *s, size_t len) 
{
    int enclen = 0;
    ssize_t n = 0;
    ssize_t nwritten = 0;
    unsigned char buf[5];
    int rc = C_OK;

    /* Try integer encoding */
    if (len <= 11) {
        enclen = rdbTryIntegerEncoding((char*)s, len, buf);
        if (enclen > 0) {
            rc = rockssdscatlen(psaveval, buf, enclen);
            if (rc != C_OK) {
                return -1;
            }

            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    if (server.rdb_compression && len > 20) {
        n = rocksSaveLzfStringObject(psaveval, s, len);
        if (n == -1) {
            return -1;
        }
        
        if (n > 0) {
            return n;
        }
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
    n = rocksSaveLen(psaveval, len);
    if (n == -1) {
        return -1;
    }
    nwritten += n;

    if (len > 0) {
        rc = rockssdscatlen(psaveval, s, len);
        if (rc != C_OK) {
            return -1;
        }
            
        nwritten += len;
    }
    
    return nwritten;
}

/* 
** return -1 if failed
** return bytes if success
*/
int rocksSaveLongLongAsStringObject(sds *psaveval, long long value) {
    unsigned char buf[32];
    ssize_t n = 0;
    ssize_t nwritten = 0;
    int enclen = 0;
    int rc = C_OK;

    enclen = rdbEncodeInteger(value, buf);
    if (enclen > 0) {
        rc = rockssdscatlen(psaveval, buf, enclen);
        if (rc != C_OK) {
            return -1;
        }
        
        return enclen;
    } else {
        /* Encode as string */
        enclen = ll2string((char*)buf, 32, value);
        serverAssert(enclen < 32);

        n = rocksSaveLen(psaveval, enclen);
        if (n == -1) {
            return -1;
        }

        rc = rockssdscatlen(psaveval, buf, enclen);
        if (rc != C_OK) {
            return -1;
        }

        nwritten += enclen;
    }
    
    return nwritten;
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 *
 * return -1 if failed
 * return bytes if success
 */
int rocksSaveDoubleValue(sds *psaveval, double val) {
    unsigned char buf[128];
    int len = 0;
    int rc = C_OK;

    if (isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val))) {
            ll2string((char*)buf + 1, sizeof(buf) - 1, (long long)val);
        } else {
            snprintf((char*)buf + 1, sizeof(buf) - 1, "%.17g", val);
        }
#else
        snprintf((char*)buf + 1, sizeof(buf) - 1, "%.17g", val);
#endif
            
        buf[0] = strlen((char*)buf+1);
        len = buf[0] + 1;
    }

    rc = rockssdscatlen(psaveval, buf, len);
    if (rc != C_OK) {
        return -1;
    }

    return len;
}

/* 
** return -1 if error occurs
** return bytes if success
*/
int rocksGenStringObjectVal(robj *val, sds *psaveval, int savetype)
{
    ssize_t nwritten = 0;
    int rc = C_OK;

    if (savetype == ROCKS_SAVE_STRING_TYPE) {
        rc = rocksSaveObjectType(psaveval, val);
        if (rc != C_OK) {
            serverLog(LL_WARNING, "save value object type failed");
            return -1;
        }
    }
    
    if (val->encoding == OBJ_ENCODING_INT) {
        nwritten = rocksSaveLongLongAsStringObject(
                                        psaveval, (long)val->ptr);  
    } else {
        serverAssertWithInfo(NULL, val, sdsEncodedObject(val));
        nwritten = rocksSaveRawString(psaveval, val->ptr, sdslen(val->ptr));
    }

    return nwritten;
}

/* 
** return -1 if error occurs
** return bytes if success
*/
int rocksGenListObjectVal(robj *val, sds *psaveval)
{
    int rc = C_OK;
    quicklist *ql = NULL;
    quicklistNode *node = NULL;
    void *data = NULL;
    ssize_t compress_len = 0;
    ssize_t n = 0;
    size_t nwritten = 0;
    
    if (val->encoding != OBJ_ENCODING_QUICKLIST) {
        serverPanic("Unknown list encoding");
        return -1;
    } 

    rc = rocksSaveObjectType(psaveval, val);
    if (rc != C_OK) {
        serverLog(LL_WARNING, "save value object type failed");
        return -1;
    }
        
    /* Save a list value */
    ql = val->ptr;
    node = ql->head;

    n = rocksSaveLen(psaveval, ql->len);
    if (n == -1) {
        return -1;
    }
    nwritten += n;

    do {
        if (quicklistNodeIsCompressed(node)) {
            compress_len = quicklistGetLzf(node, &data);
            if (compress_len == -1) {
                return -1;
            } 
            
            n = rocksSaveLzfBlob(psaveval, data, compress_len, node->sz);
            if (n == -1) {
                return -1;
            }            
        } else {
            n = rocksSaveRawString(psaveval, node->zl, node->sz);
            if (n == -1) {
                return -1;
            }
        }

        nwritten += n;
    } while ((node = node->next));

    return nwritten;
}

/* 
** return -1 if error occurs
** return bytes if success
*/
int rocksGenZsetObjectVal(robj *val, sds *psaveval)
{
    ssize_t n = 0;
    ssize_t nwritten = 0;
    size_t l = 0;
    zset *zs = NULL;
    dictIterator *di = NULL;
    dictEntry *de = NULL;
    robj *eleobj = NULL;
    double *score = NULL;
    int rc = C_OK;

    rc = rocksSaveObjectType(psaveval, val);
    if (rc != C_OK) {
        serverLog(LL_WARNING, "save value object type failed");
        return -1;
    }
    
    if (val->encoding == OBJ_ENCODING_ZIPLIST) {
        l = ziplistBlobLen((unsigned char*)val->ptr);

        n = rocksSaveRawString(psaveval, val->ptr, l);
        if (n == -1) {
            return -1;
        }
        
        nwritten += n;
    } else if (val->encoding == OBJ_ENCODING_SKIPLIST) {
        zs = val->ptr;
        di = dictGetIterator(zs->dict);

        n = rocksSaveLen(psaveval, dictSize(zs->dict));
        if (n == -1) {
            return -1;
        }
        
        nwritten += n;

        while((de = dictNext(di)) != NULL) {
            eleobj = dictGetKey(de);
            score = dictGetVal(de);

            n = rocksGenStringObjectVal(eleobj, psaveval, 
                                        ROCKS_NOT_SAVE_STRING_TYPE);
            if (n == -1) {
                return -1;
            }            
            nwritten += n;

            n = rocksSaveDoubleValue(psaveval, *score);
            if (n == -1) {
                return -1;
            }            
            nwritten += n;
        }
        dictReleaseIterator(di);
    } else {
        serverLog(LL_WARNING, "Unknown sorted set encoding");
        return -1;
    }

    return nwritten;
}

int rocksSaveZiplistHashObject(redisDb *db, 
                               unsigned long long desno, 
                               sds key, 
                               robj *val, 
                               sds *psavekey, 
                               sds *psaveval)
{
    int rc = C_OK;
    size_t l = 0;
    ssize_t n = 0;

    if (rocksNeedExchangeKey(key)) {
        *psavekey = sdscatfmt(*psavekey, "%i_%u_%U", db->id, val->type, desno);
    } else {
        *psavekey = sdscatfmt(*psavekey, "%i_%u_%S", db->id, val->type, key);
    } 
    
    rc = rocksSaveObjectType(psaveval, val);
    if (rc != C_OK) {
        serverLog(LL_WARNING, "rocksSaveObjectType for key(%s) failed", key);
        return C_ERR;
    }

    l = ziplistBlobLen((unsigned char*)val->ptr);
    n = rocksSaveRawString(psaveval, val->ptr, l);
    if (n == -1) {
        serverLog(LL_WARNING, "collect value object content failed");
        return C_ERR;
    }

    rc = write_to_rocksdb(*psavekey, sdslen(*psavekey), 
                          *psaveval, sdslen(*psaveval));
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                "write value of key(%s) to rocksdb failed", key);         
        return C_ERR;        
    } 
    
    return C_OK;
}

dictEntry *getHashLruBestKey(redisDb *db, dict *hdict)
{
    int k = 0;
    int count = DISK_STORE_KEY_LRU_GET_LOOP;
    dictEntry *de = NULL;
    struct evictionPoolEntry *pool = NULL;
     
    pool = db->hash_eviction_pool;
    while(count--) {
        evictionPoolPopulateHashWithDstoreCheck(hdict, hdict, 
                                            db->hash_eviction_pool, 1);
        /* Go backward from best to worst element to evict. */
        for (k = MAXMEMORY_EVICTION_POOL_SIZE - 1; k >= 0; k--) {
            if (pool[k].key == NULL) {
                continue;
            }

            de = dictFind(hdict, pool[k].key);

            /* Remove the entry from the pool. */
            //sdsfree(pool[k].key);   
            decrRefCount((robj *)pool[k].key);
            pool[k].key = NULL;
           
            /* Shift all elements on its right to left. */
            memmove(pool + k, pool + k + 1,
                sizeof(pool[0]) * (MAXMEMORY_EVICTION_POOL_SIZE - k - 1));
            /* Clear the element on the right which is empty
             * since we shifted one position to the left.  */
            pool[MAXMEMORY_EVICTION_POOL_SIZE-1].key = NULL;
            pool[MAXMEMORY_EVICTION_POOL_SIZE-1].idle = 0;

            /* If the key exists, is our pick. Otherwise it is
             * a ghost and we need to try the next element. */
            if (de) {
                if (dictIsEntryValOnDisk(de)) {
                    continue;
                } else {
                    return de;
                }
            } else {
                /* Ghost... */
                continue;
            }
        }
    }

    return de;
}

void rocksSaveAllHashField(redisDb *db,
                          unsigned long long desno,
                          sds key, 
                          robj *val)
{
    int rc = C_OK;
    int n = 0;
    robj *curkey = NULL;
    robj *curval = NULL;
    sds *diskkey = NULL;
    sds *diskval = NULL;
    dictIterator *di = dictGetIterator((dict *)val->ptr);
    dictEntry *de = NULL;
    
    while((de = dictNext(di)) != NULL) {       
        if (dictIsEntryValOnDisk(de)) {
            continue;
        }        

        diskkey = getClearedSharedKeySds();
        diskval = getClearedSharedValSds();
        
        curkey = dictGetKey(de);
        curval = dictGetVal(de);
        curkey = getDecodedObject(curkey);

        if (rocksNeedExchangeKey(key)) {
            if (rocksNeedExchangeKey((sds)curkey->ptr)) {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%U", 
                             db->id, val->type, desno, de->v_sno);
            } else {                
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%S", 
                         db->id, val->type, desno, (sds)curkey->ptr);
            }                 
        } else {
            if (rocksNeedExchangeKey((sds)curkey->ptr)) {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%U", 
                             db->id, val->type, key, de->v_sno);
            } else {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%S", 
                             db->id, val->type, key, (sds)curkey->ptr);
            }                 
        }                        
        n = rocksGenStringObjectVal(curval, diskval, 
                                    ROCKS_NOT_SAVE_STRING_TYPE);
        if (n == -1) {            
            serverLog(LL_WARNING, 
                "generate value sds for HASH(%s) field(%s) failed",
                key, (sds)curkey->ptr);
            decrRefCount(curkey);
            continue;
        }

        rc = write_to_rocksdb(*diskkey, sdslen(*diskkey), 
                              *diskval, sdslen(*diskval));
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                "write KEY(%s) ==> VALUE(%s) to rocksdb failed",
                key, *diskkey, *diskval);         
            decrRefCount(curkey);
            continue;
        }

        setEntryValOnDisk((dict *)val->ptr, de, curval->type);

        decrRefCount(curkey);        
    } 
}

void rocksSaveRealHashObject(redisDb *db, 
                            unsigned long long desno,
                            sds key, 
                            robj *val)
{
    int rc = C_OK;
    int n = 0;
    int count = 0;
    dictEntry *de = NULL;
    robj *curkey = NULL;
    robj *curval = NULL;
    sds *diskkey = NULL;
    sds *diskval = NULL;
    long long start = mstime();
        
    while(count < server.dstore_hash_loop_field_nr) {
        count++;

        if (mstime() - start >= server.dstore_key_timeout_ms) {
            break;
        }
        
        de = getHashLruBestKey(db, (dict *)val->ptr);
        if (!de) {
            continue;
        }
        
        if (dictIsEntryValOnDisk(de)) {
            continue;
        }        

        diskkey = getClearedSharedKeySds();
        diskval = getClearedSharedValSds();
        
        curkey = dictGetKey(de);
        curval = dictGetVal(de);
        curkey = getDecodedObject(curkey);

        if (rocksNeedExchangeKey(key)) {
            if (rocksNeedExchangeKey((sds)curkey->ptr)) {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%U", 
                             db->id, val->type, desno, de->v_sno);
            } else {                
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%S", 
                         db->id, val->type, desno, (sds)curkey->ptr);
            }                 
        } else {
            if (rocksNeedExchangeKey((sds)curkey->ptr)) {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%U", 
                             db->id, val->type, key, de->v_sno);
            } else {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%S", 
                             db->id, val->type, key, (sds)curkey->ptr);
            }                 
        }                     
        n = rocksGenStringObjectVal(curval, diskval,
                                    ROCKS_NOT_SAVE_STRING_TYPE);
        if (n == -1) {            
            serverLog(LL_WARNING, 
                "generate value sds for HASH(%s) field(%s) failed",
                key, (sds)curkey->ptr);
            decrRefCount(curkey);
            continue;
        }

        rc = write_to_rocksdb(*diskkey, sdslen(*diskkey), 
                              *diskval, sdslen(*diskval));
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                "write KEY(%s) ==> VALUE(%s) to rocksdb failed",
                key, *diskkey, *diskval);         
            decrRefCount(curkey);
            continue;
        }

        setEntryValOnDisk((dict *)val->ptr, de, curval->type);

        decrRefCount(curkey);        
    } 
}

/* 
** return -1 if error occurs
** return bytes if success
*/
int rocksGenHashObjectVal(robj *val, sds *psaveval)
{
    ssize_t n = 0;
    ssize_t nwritten = 0;
    size_t l = 0;
    dictIterator *di = NULL;
    dictEntry *de = NULL;
    robj *curkey = NULL;
    robj *curval = NULL;
    int rc = C_OK;

    rc = rocksSaveObjectType(psaveval, val);
    if (rc != C_OK) {
        serverLog(LL_WARNING, "save value object type failed");
        return -1;
    }
    
    /* Save a hash value */
    if (val->encoding == OBJ_ENCODING_ZIPLIST) {
        l = ziplistBlobLen((unsigned char*)val->ptr);

        n = rocksSaveRawString(psaveval, val->ptr, l);
        if (n == -1) {
            return -1;
        }
        nwritten += n;
    } else if (val->encoding == OBJ_ENCODING_HT) {    
        di = dictGetIterator(val->ptr);

        n = rocksSaveLen(psaveval, dictSize((dict*)val->ptr));
        if (n == -1) {
            dictReleaseIterator(di);
            return -1;
        }
        
        nwritten += n;

        while((de = dictNext(di)) != NULL) {
            curkey = dictGetKey(de);
            curval = dictGetVal(de);

            n = rocksGenStringObjectVal(curkey, psaveval,
                                        ROCKS_NOT_SAVE_STRING_TYPE);
            if (n == -1) {
                dictReleaseIterator(di);
                return -1;
            }
            nwritten += n;

            n = rocksGenStringObjectVal(curval, psaveval,
                                        ROCKS_NOT_SAVE_STRING_TYPE);
            if (n == -1) {
                dictReleaseIterator(di);
                return -1;
            }
            nwritten += n;
        }
        dictReleaseIterator(di);       
    } else {
        serverPanic("Unknown hash encoding");
        return -1;
    }

    return nwritten;
}

/* 
** return -1 if error occurs
** return bytes if success
*/
int rocksGenSetObjectVal(robj *val, sds *psaveval)
{
    ssize_t n = 0;
    ssize_t nwritten = 0;
    size_t l = 0;
    dict *set = NULL;
    dictIterator *di = NULL;
    dictEntry *de = NULL;
    robj *eleobj = NULL;  
    int rc = C_OK;

    rc = rocksSaveObjectType(psaveval, val);
    if (rc != C_OK) {
        serverLog(LL_WARNING, "save value object type failed");
        return -1;
    }
    
    /* Save a sorted set value */
    if (val->encoding == OBJ_ENCODING_INTSET) {
        l = intsetBlobLen((intset *)val->ptr);

        n = rocksSaveRawString(psaveval, val->ptr, l);
        if (n == -1) {
            return -1;
        }
        nwritten += n;
    } else if (val->encoding == OBJ_ENCODING_HT) {
        set = val->ptr;
        di = dictGetIterator(set);

        n = rocksSaveLen(psaveval, dictSize(set));
        if (n == -1) {
            return -1;
        }
        nwritten += n;

        while((de = dictNext(di)) != NULL) {        
            eleobj = dictGetKey(de);
            n = rocksGenStringObjectVal(eleobj, psaveval,
                                        ROCKS_NOT_SAVE_STRING_TYPE);
            if (n == -1) {
                return -1;
            }
            nwritten += n;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown sorted set encoding");
        return -1;
    }

    return nwritten;
}

/* 
** return C_ERR if failed
** return C_OK if success
*/
int saveStringObjectOnDisk(redisDb *db, 
                           unsigned long long desno, 
                           sds key, 
                           robj *val)
{
    int rc = C_OK;
    int nwritten = 0;
    sds *diskkey = getClearedSharedKeySds();
    sds *diskval = getClearedSharedValSds();

    if (rocksNeedExchangeKey(key)) {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%U", db->id, val->type, desno);
    } else {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%S", db->id, val->type, key);
    }
    
    nwritten = rocksGenStringObjectVal(val, diskval,
                                      ROCKS_SAVE_STRING_TYPE);
    if (nwritten == -1) {
        serverLog(LL_WARNING, 
                "generate rocksdb value for string obj(%s) failed", 
                (char *)val->ptr);       
        return C_ERR;
    }

    rc = write_to_rocksdb(*diskkey, sdslen(*diskkey), 
                          *diskval, sdslen(*diskval));
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                "write value of key(%s) to rocksdb failed", key);       
        return C_ERR;        
    }         
    
    return C_OK;
}

void setQuicklistNodeOnDisk(quicklistNode *node, sds *dstorekey)
{
    node->zl_ondisk = VAL_ON_DISK;  
    
    sdsfree(node->zl_dstore_key);
    node->zl_dstore_key = sdsdup(*dstorekey);
    
    zfree(node->zl);
    node->zl = NULL;

    if (node->ql) {
        ++node->ql->dlen;
    }
}

void updQuicklistNodeVal(quicklistNode *node, unsigned char *zl)
{
    node->zl_ondisk = VAL_NOT_ON_DISK;        
    node->zl = zl;

    if (node->ql && node->ql->dlen) {
        --node->ql->dlen;
    }
}

/* Returns 0 if not need, otherwise returns 1 */
int listNeedToStore(quicklist *ql)
{
    // invalid quicklist handle
    if (!ql) {
        return 0;
    }

    // quicklist header and tail not need to process
    if (ql->len < 3) {
        return 0;
    }

    // all nodes maybe stored onto disk
    if (ql->len <= ql->dlen) {
        ql->dlen = ql->len;
        return 0;
    }

    // number of quicklistNode stored in memory less than
    // server.dstore_list_node_inmem_max
    if (ql->len - ql->dlen <= server.dstore_list_node_inmem_max) {
        return 0;
    }

    return 1;
}

/* 
** return C_ERR if failed
** return C_OK if success
*/
int saveListObjectOnDisk(redisDb *db, 
                         unsigned long long desno,
                         sds key, 
                         robj *val, 
                         int withlimit)
{
    int rc = C_OK;
    quicklist *ql = NULL;
    void *data = NULL;
    ssize_t compress_len = 0;
    ssize_t n = 0;
    sds *diskkey = NULL;
    sds *diskval = NULL;
    int count = server.dstore_list_node_nr;
    long long start = mstime();
    
    if (val->encoding != OBJ_ENCODING_QUICKLIST) {
        serverLog(LL_WARNING, "Unknown list encoding:%d", val->encoding);
        return C_ERR;
    } 
       
    ql = val->ptr;
    
    /* we won't process the first quicklistNode and last quicklistNode */
    if (!listNeedToStore(ql)) {
        return C_OK;
    }

    /* skip the first quicklistNode */
    if (!ql->iterator) {
        ql->iterator = ql->head ? ql->head->next : NULL;
    }
    
    while (ql->iterator) {
        if (withlimit) {
            if (count <= 0) {
                break;
            }

            if (mstime() - start >= server.dstore_key_timeout_ms) {
                break;
            }
        }
              
        /* skip the last one */
        if (!ql->iterator->next) {
            ql->iterator = ql->head;
            break;
        }

        if (ql->iterator->zl_ondisk == VAL_ON_DISK) {
            ql->iterator = ql->iterator->next;
            continue;
        }

        diskkey = getClearedSharedKeySds();
        diskval = getClearedSharedValSds();

        if (rocksNeedExchangeKey(key)) {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%U", 
                             db->id, val->type, desno, ql->iterator->sno); 
        } else {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%U", 
                             db->id, val->type, key, ql->iterator->sno); 
        }                     
        
        if (quicklistNodeIsCompressed(ql->iterator)) {
            compress_len = quicklistGetLzf(ql->iterator, &data);
            if (compress_len == -1) {
                n = -1;
            } else {
                n = rocksSaveLzfBlob(diskval, data, 
                                     compress_len, ql->iterator->sz);  
            }
        } else {
            n = rocksSaveRawString(diskval, 
                                   ql->iterator->zl, ql->iterator->sz);
        }

        if (n == -1) {
            serverLog(LL_WARNING, 
                "dump LIST(%s) quicklistnode(idx:%lu) to rocksdb failed",
                key, ql->iterator->sno); 
            ql->iterator = ql->iterator->next;
            continue;
        }   

        rc = write_to_rocksdb(*diskkey, sdslen(*diskkey), 
                              *diskval, sdslen(*diskval));
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                "write KEY(%s) ==> VALUE(%s) to rocksdb failed",
                key, *diskkey, *diskval);  
            ql->iterator = ql->iterator->next;
            continue;       
        }

        setQuicklistNodeOnDisk(ql->iterator, diskkey);
        
        if (withlimit)  {
            count--;
        }

        ql->iterator = ql->iterator->next;
    } 

    return C_OK;
}

/* 
** return C_ERR if failed
** return C_OK if success
*/
int saveWholeListObjectOnDisk(redisDb *db, 
                              unsigned long long desno, 
                              sds key, 
                              robj *val)
{
    int rc = C_OK;
    sds *diskkey = getClearedSharedKeySds();
    sds *diskval = getClearedSharedValSds();

    if (rocksNeedExchangeKey(key)) {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%U", db->id, val->type, desno);
    } else {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%S", db->id, val->type, key);
    }

    rc = rocksGenListObjectVal(val, diskval);
    if (rc == -1) {
        serverLog(LL_WARNING, 
                "generate rocksdb value for list obj(%s) failed", key);       
        return C_ERR;
    }

    rc = write_to_rocksdb(*diskkey, sdslen(*diskkey), 
                          *diskval, sdslen(*diskval));
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                "write value of key(%s) to rocksdb failed", key);        
        return C_ERR;        
    }

    return C_OK;
}

/* 
** Returns C_ERR on fail
** Returns C_NONE on no need
** Returns C_OK on success
*/
int saveSetObjectOnDisk(redisDb *db, 
                        unsigned long long desno,
                        sds key, 
                        robj *val)
{
    int rc = C_OK;
    sds *diskkey = NULL;
    sds *diskval = NULL;

    if (server.set_use_disk_store == SET_DISK_STORAGE_NOT_USE) {
        return C_NONE;
    }

    diskkey = getClearedSharedKeySds();
    diskval = getClearedSharedValSds();

    if (rocksNeedExchangeKey(key)) {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%U", db->id, val->type, desno); 
    } else {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%S", db->id, val->type, key);  
    }

    rc = rocksGenSetObjectVal(val, diskval);
    if (rc == -1) {
        serverLog(LL_WARNING, 
                "generate rocksdb value for set obj(%s) failed", key);       
        return C_ERR;
    }

    rc = write_to_rocksdb(*diskkey, sdslen(*diskkey), 
                          *diskval, sdslen(*diskval));
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                "write value of key(%s) to rocksdb failed", key);         
        return C_ERR;        
    }    

    return C_OK;
}

/* 
** return C_ERR if failed
** return C_OK if success
*/
int saveHashObjectOnDisk(redisDb *db, 
                         unsigned long long desno, 
                         sds key, 
                         robj *val, 
                         int withlimit)
{
    int rc = C_OK;
    sds *diskkey = NULL;
    sds *diskval = NULL;
        
    if (val->encoding == OBJ_ENCODING_ZIPLIST) {
        diskkey = getClearedSharedKeySds();
        diskval = getClearedSharedValSds();
    
        rc = rocksSaveZiplistHashObject(db, desno, key, val, diskkey, diskval);
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                      "save OBJ_ENCODING_ZIPLIST hash object(%s) failed", key);
            return C_ERR;
        }
    } else if (val->encoding == OBJ_ENCODING_HT) {  
        if (withlimit) {
            rocksSaveRealHashObject(db, desno, key, val);
        } else {
            rocksSaveAllHashField(db, desno, key, val);
        }    
    } else {
        serverPanic("Unknown hash encoding");
        return C_ERR;
    }
          
    return C_OK;
}

/* 
** return C_ERR if failed
** return C_OK if success
*/
int saveWholeHashObjectOnDisk(redisDb *db, 
                              unsigned long long desno,
                              sds key, 
                              robj *val)
{
    int rc = C_OK;
    sds diskkey = sdsempty();
    sds diskval = sdsempty();

    if (rocksNeedExchangeKey(key)) {
        diskkey = sdscatfmt(diskkey, "%i_%u_%U", db->id, val->type, desno);
    } else {
        diskkey = sdscatfmt(diskkey, "%i_%u_%S", db->id, val->type, key);
    }

    rc = rocksGenHashObjectVal(val, &diskval);
    if (rc == -1) {
        serverLog(LL_WARNING, 
                "generate rocksdb value for hash obj(%s) failed", key);
        sdsfree(diskkey);
        sdsfree(diskval);        
        return C_ERR;
    }

    rc = write_to_rocksdb(diskkey, sdslen(diskkey), diskval, sdslen(diskval));
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                "write value of key(%s) to rocksdb failed", key); 
        sdsfree(diskkey);
        sdsfree(diskval);        
        return C_ERR;        
    }    

    sdsfree(diskkey);
    sdsfree(diskval);

    return C_OK;
}

/* 
** Returns C_ERR on fail
** Returns C_NONE on no need
** Returns C_OK on success
*/
int saveZsetObjectOnDisk(redisDb *db, 
                         unsigned long long desno,
                         sds key, 
                         robj *val)
{
    int rc = C_OK;
    sds *diskkey = NULL;
    sds *diskval = NULL;

    if (server.zset_use_disk_store == ZSET_DISK_STORAGE_NOT_USE) {
        return C_NONE;
    }

    diskkey = getClearedSharedKeySds();
    diskval = getClearedSharedValSds();

    if (rocksNeedExchangeKey(key)) {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%U", db->id, val->type, desno);
    } else {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%S", db->id, val->type, key);
    }
        
    rc = rocksGenZsetObjectVal(val, diskval);
    if (rc == -1) {
        serverLog(LL_WARNING, 
                "generate rocksdb value for string obj(%s) failed", 
                (char *)val->ptr);     
        return C_ERR;
    }

    rc = write_to_rocksdb(*diskkey, sdslen(*diskkey), 
                          *diskval, sdslen(*diskval));
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                "write value of key(%s) to rocksdb failed", key);         
        return C_ERR;        
    }        

    return C_OK;
}

/* 
** return C_ERR if failed
** return C_OK if success
*/
int saveObjectOnDiskLimit(redisDb *db, dictEntry *de, int limit)
{
    int rc = C_OK;
    sds thiskey = NULL;
    robj *thisval = NULL;
    unsigned char type = 0;

    if (de->v_ondisk == VAL_ON_DISK) {
        return C_OK;
    }

    thiskey = dictGetKey(de);
    thisval = dictGetVal(de);
    type = thisval->type;

    //serverLog(LL_WARNING, "save data for key(%s) onto disk", thiskey);    
    if (type == OBJ_STRING) {
        rc = saveStringObjectOnDisk(db, de->v_sno, thiskey, thisval);
        if (rc == C_OK) {
            setEntryValOnDisk(db->dict, de, type);
        }
    } else if (type == OBJ_LIST) {
        rc = saveListObjectOnDisk(db, de->v_sno, thiskey, thisval, limit);
        //if (rc == C_OK) {
        //    setEntryValOnDisk(db->dict, de, type);
        //}
    } else if (type == OBJ_SET) {
        rc = saveSetObjectOnDisk(db, de->v_sno, thiskey, thisval);
        if (rc == C_OK) {
            setEntryValOnDisk(db->dict, de, type);
        }
    } else if (type == OBJ_ZSET) {
        rc = saveZsetObjectOnDisk(db, de->v_sno, thiskey, thisval);
        if (rc == C_OK) {
            setEntryValOnDisk(db->dict, de, type);
        }
    } else if (type == OBJ_HASH) {
        rc = saveHashObjectOnDisk(db, de->v_sno, thiskey, thisval, limit);
        if (rc == C_OK && thisval->encoding == OBJ_ENCODING_ZIPLIST) {
            setEntryValOnDisk(db->dict, de, type);
        }
        //rc = saveWholeHashObjectOnDisk(db, thiskey, thisval);
    } else  {
        serverLog(LL_WARNING, "invalid value robj type:%d for key:%s", 
                  thisval->type, thiskey);
        rc = C_ERR;          
    }

    if (rc == C_ERR) {
        serverLog(LL_WARNING, "save key(%s) onto disk failed", thiskey); 
        return rc;
    } else {
        return C_OK;
    }
}

/* Return 1 if filter through, otherwise return 0 */
int dictFilterSelectedDe(dictEntry *de)
{
    robj *val = NULL;
    
    if ((!de) || dictIsEntryValOnDisk(de)) {
        return 0;
    }

    val = dictGetVal(de);
    if (val 
        && val->type == OBJ_SET 
        && server.set_use_disk_store == SET_DISK_STORAGE_NOT_USE) {
        return 0;
    }

    if (val 
        && val->type == OBJ_ZSET 
        && server.zset_use_disk_store == ZSET_DISK_STORAGE_NOT_USE) {
        return 0;
    }

    return 1;
}


/* 
** return 1 if timelimit touched
** return 0 if timelimit not touched
*/
int saveDbOnDiskWithTimelimit(redisDb *db, long long start, long long timelimit)
{

    dictEntry *de = NULL;
    int num = 0;
    long long elapsed = 0;
    int savenr = 0;
    static int iteration = 0;
    dict *dict = db->dict;

    /* Continue to expire if at the end of the cycle more than 25%
     * of the keys were expired. */
    do {  
        savenr = 0;
        
        /* If there is nothing to save on disk try next DB ASAP. */
        
        num = dictSize(db->dict);
        if (num <= 0) {
            break;
        }
               
        if (num > server.dstore_loop_key_nr) {
            num = server.dstore_loop_key_nr;
        }

        while (num--) {            
            if (server.dstore_policy == DISK_STORE_ALLKEYS_RANDOM) {
                de = dictGetRandomKey(dict);                
            } else if (server.dstore_policy == DISK_STORE_ALLKEYS_LRU) {
                de = getLruBestKey(db);
            }

            if (!dictFilterSelectedDe(de)) {
                continue;
            }
            
            if (saveObjectOnDiskLimit(db, de, 1) == C_OK) {
                savenr++;
            }

            if (!(savenr % server.dstore_loop_step_key_nr)) {
                elapsed = mstime() - start;
                if (elapsed > timelimit) {
                    return 1;
                }
                
            }
        }         

        /* We can't block forever here even if there are many keys to
         * expire. So after a given amount of milliseconds return to the
         * caller waiting for the other active expire cycle. */
        iteration++;
        if ((iteration & 0xf) == 0) { /* check once every 16 iterations. */
            elapsed = mstime() - start;
            if (elapsed > timelimit) {
                return 1;
            }
        }
        
        /* We don't repeat the cycle if there are less than 25% of keys
         * saved on disk in the current DB. */
    } while (0);

    return 0;
}

void saveDataOnDiskCycle(int flag)
{
    /* This function has some global state in order to continue the work
     * incrementally across calls. */
    int j = 0;
    static unsigned int curdb = 0;                    /* Last DB tested. */ 
    int dbs_per_call = CRON_DBS_PER_CALL;
    long long timelimit = 0;
    redisDb *db = NULL;
    long long start = mstime();

    /* return if not need to save data on disk */
    if (!needSaveObjectOnDisk(flag)) {
        serverLog(LL_DEBUG, "no need to save data on disk with model:%d", flag);
        return;
    }

    /* We usually should test CRON_DBS_PER_CALL per iteration, with
     * two exceptions:
     * 1) Don't test more DBs than we have.
     * 2) If last time we hit the time limit, we want to scan all DBs
     * in this iteration, as there is work to do in some DB and we don't want
     * keys to use memory for too much time. */
    if (dbs_per_call > server.dbnum) {
        dbs_per_call = server.dbnum;
    }
             
    if (flag == DISK_STORE_FAST) {
        timelimit = server.dstore_loop_timeout_ms; /* in microseconds. */
    } else {
        /* We can use at max ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC percentage of CPU time
     * per iteration. Since this function gets called with a frequency of
     * server.hz times per second, the following is the max amount of
     * microseconds we can spend in this function. */
        timelimit = 1000 * DISK_STORE_CYCLE_SLOW_TIME_PERC / server.hz / 100;
        timelimit = (timelimit <= 0) ? 1 : timelimit;
    }

    for (j = 0; j < dbs_per_call; j++) {
        db = server.db + (curdb % server.dbnum);

        /* Increment the DB now so we are sure if we run out of time
         * in the current DB we'll restart from the next. This allows to
         * distribute the time evenly across DBs. */
        curdb++;

        if (!dictSize(db->dict)) {
            continue;
        }
        
        if (saveDbOnDiskWithTimelimit(db, start, timelimit)) {
            return;
        }
    }
}

void freeValOnDisk(redisDb *db, 
                   unsigned long long desno, 
                   unsigned char type, 
                   sds key)
{
    int rc = C_OK;
    sds *diskkey = getClearedSharedKeySds();

    if (rocksNeedExchangeKey(key)) {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%U", db->id, type, desno); 
    } else {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%S", db->id, type, key); 
    }

    rc = del_from_rocksdb(*diskkey, sdslen(*diskkey));
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                "delete key(%s) from rocksdb failed", *diskkey);             
    }         
}

void freeEnrtyPub(redisDb *db, 
                  const void *key, 
                  dict *dt,
                  dictEntry *de,
                  unsigned char type)
{
    if (dictIsEntryValOnDisk(de)) {
        freeValOnDisk(db, de->v_sno, type, (sds)key);
    } else {
        dictFreeVal(dt, de);
    }

    dictFreeKey(dt, de);
    zfree(de);
}


void freeStringEnrty(redisDb *db, 
                     const void *key, 
                     dict *dt,
                     dictEntry *de,
                     unsigned char type)
{
    freeEnrtyPub(db, key, dt, de, type);
}

void freeListNodes(redisDb *db, 
                   unsigned long long desno,
                   sds key, 
                   robj *val)
{
    int rc = C_OK;
    quicklist *ql = NULL;
    quicklistNode *node = NULL;
    sds *diskkey = NULL;
    
    if (val->encoding != OBJ_ENCODING_QUICKLIST) {
        serverLog(LL_WARNING, "Unknown list encoding:%d", val->encoding);
        return;
    } 
       
    ql = val->ptr;    
    node = ql->head;
    
    while (node) {
        if (node->zl_ondisk != VAL_ON_DISK) {
            node = node->next;
            continue;
        }

        diskkey = getClearedSharedKeySds();
        if (rocksNeedExchangeKey(key)) {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%U", 
                             db->id, val->type, desno, node->sno);
        } else {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%U", 
                             db->id, val->type, key, node->sno);
        }                     
                             
        rc = del_from_rocksdb(*diskkey, sdslen(*diskkey));
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                    "delete key(%s) from rocksdb failed", *diskkey);             
        }

        node = node->next;
    } 
}


void freeListEnrty(redisDb *db, 
                   const void *key, 
                   dict *dt,
                   dictEntry *de,
                   unsigned char type)
{
    robj *val = NULL;
    
    if (dictIsEntryValOnDisk(de)) {
        freeValOnDisk(db, de->v_sno, type, (sds)key);
    } else {
        val = dictGetVal(de);
        freeListNodes(db, de->v_sno, (sds)key, val);
    }
   
    dictFreeKey(dt, de);
    zfree(de);
}

void freeSetEnrty(redisDb *db, 
                  const void *key, 
                  dict *dt,
                  dictEntry *de,
                  unsigned char type)
{
    freeEnrtyPub(db, key, dt, de, type);
}

void freeZsetEnrty(redisDb *db, 
                   const void *key, 
                   dict *dt,
                   dictEntry *de,
                   unsigned char type)
{
    freeEnrtyPub(db, key, dt, de, type);
}

/* free whole dentry content in certain dict.
 *  @db      server.db[xx]
 *  @key     KEY refers to dict 'd' in 'db'
 *  @type    type for dict 'd', which should be OBJ_HASH
 *  @d       dict refers to KEY 'key' in 'db'
 *  @de      certain dentry in dict 'd'
*/
void freeHashFieldVal(redisDb *db, 
                    unsigned long long desno,
                    sds key,
                    unsigned char type,
                    dict *d,                    
                    dictEntry *de)
{
    int rc = C_OK;
    robj *curkey = NULL;
    sds *diskkey = NULL;
    
    if (dictIsEntryValOnDisk(de)) {      
        diskkey = getClearedSharedKeySds();
        
        curkey = dictGetKey(de);
        curkey = getDecodedObject(curkey);         

        if (rocksNeedExchangeKey(key)) {
            if (rocksNeedExchangeKey((sds)curkey->ptr)) {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%U", 
                             db->id, type, desno, de->v_sno);
            } else {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%S", 
                             db->id, type, desno, (sds)curkey->ptr);
            }                 
        } else {
            if (rocksNeedExchangeKey((sds)curkey->ptr)) {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%U", 
                             db->id, type, key, de->v_sno); 
            } else {
                *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%S", 
                             db->id, type, key, (sds)curkey->ptr); 
            }                 
        }
        
        rc = del_from_rocksdb(*diskkey, sdslen(*diskkey));
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                    "delete key(%s) from rocksdb failed", *diskkey);             
        }
        
        decrRefCount(curkey);  
    } else {
        dictFreeVal(d, de);
    }

    dictFreeKey(d, de);

    zfree(de);         
}

void freeHashFields(redisDb *db, 
                    unsigned long long desno,
                    sds key, 
                    robj *val)
{
    dictIterator *di = NULL;
    dictEntry *de = NULL;
    dict *d = NULL;

    if (val->encoding == OBJ_ENCODING_ZIPLIST) {
        zfree(val->ptr);
        zfree(val);
        return;
    } 

    d = (dict *)val->ptr;
    di = dictGetIterator((dict *)val->ptr);
    while((de = dictNext(di)) != NULL) { 
        freeHashFieldVal(db, desno, key, val->type, (dict *)val->ptr, de);
    }
    dictReleaseIterator(di);

    zfree(d->ht[0].table);
    _dictReset(&d->ht[0]);
    zfree(d->ht[1].table);
    _dictReset(&d->ht[1]); 
    zfree(d);

    zfree(val);
}

/* @db     server.db[XX]
 * @key    KEY in 'db', sds
 * @dt     dict to find and delete 'key'
 * @de     dentry in 'dt' that points to 'key'
 * @type   type of KEY 'key' in 'dt'
*/
void freeHashEnrty(redisDb *db, 
                   const void *key, 
                   dict *dt,
                   dictEntry *de,
                   unsigned char type)
{
    robj *val = NULL;
    
    if (dictIsEntryValOnDisk(de)) {
        freeValOnDisk(db, de->v_sno, type, (sds)key);
    } else {
        val = dictGetVal(de);
        freeHashFields(db, de->v_sno, (sds)key, val);
    }
   
    dictFreeKey(dt, de);
    zfree(de);
}

/* db     server.db[XX]
 * key    KEY in 'db', sds
 * dt     dict to find and delete 'key'
 * de     dentry in 'dt' that points to 'key'
*/
void dictFreeEntry(void *db, 
                  const void *key, 
                  dict *dt,
                  dictEntry *de)
{   
    unsigned char type = 0;
    
    type = dictGetValType(de);
    
    if (type == OBJ_STRING) {
        freeStringEnrty((redisDb *)db, key, dt, de, type);
    } else if (type == OBJ_LIST) {
        freeListEnrty((redisDb *)db, key, dt, de, type);
    } else if (type == OBJ_SET) {
        freeSetEnrty((redisDb *)db, key, dt, de, type);
    } else if (type == OBJ_ZSET) {
        freeZsetEnrty((redisDb *)db, key, dt, de, type);
    } else if (type == OBJ_HASH) {
        freeHashEnrty((redisDb *)db, key, dt, de, type);
    } else {
        serverLog(LL_WARNING, "invalid type:%u", type);
    }
}

void init_value_desc(char *diskval, 
                     size_t diskvallen, 
                     accbuf_t *pbufacc)
{
    assert(pbufacc != NULL);
    assert(diskval != NULL);
    
    pbufacc->val_buf = diskval;
    pbufacc->val_pos = diskval;
    pbufacc->val_size = diskvallen;
    pbufacc->val_size_left = diskvallen;
}

/* 
** return 0 if buf not enough
** return 1 if read successfully
*/
int accbufRead(accbuf_t *pbufacc, void *buf, size_t len)
{
    if (pbufacc->val_size_left < len) {
        return 0;
    }

    memcpy(buf, pbufacc->val_pos, len);
    pbufacc->val_pos += len;
    pbufacc->val_size_left -= len;

    return 1;
}

/* 
** return -1 if failed
** return type if success
*/
int rocksLoadType(accbuf_t *pbufacc)
{
    unsigned char type = 0;

    if (accbufRead(pbufacc, &type, 1) == 0) {
        return -1;
    }

    return type;
}

/* 
** return RDB_LENERR if failed
** return len if success
*/
uint32_t rocksLoadLen(accbuf_t *pbufacc, int *isencoded)
{
    unsigned char buf[2];
    uint32_t len = 0;
    int type = 0;

    if (isencoded) {
        *isencoded = 0;
    }

    if (accbufRead(pbufacc, buf, 1) == 0) {
        return RDB_LENERR;
    }

    type = (buf[0] & 0xC0) >> 6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) {
            *isencoded = 1;
        }
        return buf[0] & 0x3F;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        return buf[0] & 0x3F;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (accbufRead(pbufacc, buf + 1, 1) == 0) {
            return RDB_LENERR;
        }
        return ((buf[0] & 0x3F) << 8) | buf[1];
    } else if (type == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        if (accbufRead(pbufacc, &len, 4) == 0) {
            return RDB_LENERR;
        }
        return ntohl(len);
    } else {
        serverLog(LL_WARNING, 
            "Unknown length encoding %d in rdbLoadLen()", type);
        return RDB_LENERR; /* Never reached. */
    }
}

/* 
** return C_ERR if failed
** return C_OK if success
*/
int rocksLoadDoubleValue(accbuf_t *paccbuf, double *val) {
    char buf[256];
    unsigned char len = 0;

    if (accbufRead(paccbuf, &len, 1) == 0) {
        return C_ERR;
    }
    
    switch(len) {
    case 255: 
        *val = R_NegInf; 
        break;
    case 254: 
        *val = R_PosInf; 
        break;
    case 253: 
        *val = R_Nan; 
        break;
    default:
        if (accbufRead(paccbuf, buf, len) == 0) {
            return C_ERR;
        }
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        break;
    }

    return C_OK;
}

/* Loads an integer-encoded object with the specified encoding type "enctype".
 * The returned value changes according to the flags, see
 * rocksGenerincLoadStringObject() for more info. 
 *
 * return NULL if failed
 * return object if success
*/
void *rocksLoadIntegerObject(accbuf_t *pbufacc, 
                             int enctype, 
                             int flags) {
    int plain = flags & RDB_LOAD_PLAIN;
    int encode = flags & RDB_LOAD_ENC;
    unsigned char enc[4];
    long long val = 0;
    char buf[LONG_STR_SIZE];
    char *p = NULL;
    int len = 0;

    if (enctype == RDB_ENC_INT8) {
        if (accbufRead(pbufacc, enc, 1) == 0) {
            return NULL;
        }
        val = (signed char)enc[0];
    } else if (enctype == RDB_ENC_INT16) {
        if (accbufRead(pbufacc, enc, 2) == 0) {
            return NULL;
        }
        val = enc[0] | (enc[1] << 8);
    } else if (enctype == RDB_ENC_INT32) {
        if (accbufRead(pbufacc, enc, 4) == 0) {
            return NULL;
        }
        val = enc[0] | (enc[1] << 8) | (enc[2] << 16) | (enc[3] << 24);
    } else {
        val = 0; /* anti-warning */
        serverLog(LL_WARNING, "Unknown RDB integer encoding type %d", enctype);
        return NULL;
    }
    
    if (plain) {
        len = ll2string(buf, sizeof(buf), val);
        p = zmalloc(len);
        memcpy(p, buf, len);
        return p;
    } else if (encode) {
        return createStringObjectFromLongLong(val);
    } else {
        return createObject(OBJ_STRING, sdsfromlonglong(val));
    }
}

/* Load an LZF compressed string in RDB format. The returned value
 * changes according to 'flags'. For more info check the
 * rdbGenericLoadStringObject() function. 
 *
 * return NULL if failed
 * return object if success
*/
void *rocksLoadLzfStringObject(accbuf_t *pbufacc, int flags) {
    int plain = flags & RDB_LOAD_PLAIN;
    unsigned int len = 0;
    unsigned int clen = 0;
    unsigned char *c = NULL;
    sds val = NULL;

    clen = rocksLoadLen(pbufacc, NULL);
    if (clen  == RDB_LENERR) {
        return NULL;
    }

    len = rocksLoadLen(pbufacc, NULL);
    if (len  == RDB_LENERR) {
        return NULL;
    }

    c = zmalloc(clen);
    if (c == NULL) {
        return NULL;
    }

    /* Allocate our target according to the uncompressed size. */
    if (plain) {
        val = zmalloc(len);
    } else {
        val = sdsnewlen(NULL, len);        
    }

    if (val == NULL) {
        zfree(c);
        return NULL;
    }

    /* Load the compressed representation and uncompress it to target. */
    if (accbufRead(pbufacc, c, clen) == 0) {
        zfree(c);
        if (plain) {
            zfree(val);
        } else {
            sdsfree(val);
        }
        return NULL;
    }
    
    if (lzf_decompress(c, clen, val, len) == 0) {
        zfree(c);
        if (plain) {
            zfree(val);
        } else {
            sdsfree(val);
        }
        return NULL;
    }
    
    zfree(c);

    if (plain) {
        return val;
    } else {
        return createObject(OBJ_STRING, val);
    }
}


/* Load a string object from an RDB file according to flags:
 *
 * RDB_LOAD_NONE (no flags): load an RDB object, unencoded.
 * RDB_LOAD_ENC: If the returned type is a Redis object, try to
 *               encode it in a special way to be more memory
 *               efficient. When this flag is passed the function
 *               no longer guarantees that obj->ptr is an SDS string.
 * RDB_LOAD_PLAIN: Return a plain string allocated with zmalloc()
 *                 instead of a Redis object with an sds in it.
 * RDB_LOAD_SDS: Return an SDS string instead of a Redis object.
 */
void *rocksGenericLoadStringObject(accbuf_t *paccbuf, int flags) {
    int encode = flags & RDB_LOAD_ENC;
    int plain = flags & RDB_LOAD_PLAIN;
    int isencoded = 0;
    uint32_t len = 0;
    robj *o = NULL;
    void *buf = NULL;

    len = rocksLoadLen(paccbuf, &isencoded);
    if (isencoded) {
        switch(len) {
        case RDB_ENC_INT8:
        case RDB_ENC_INT16:
        case RDB_ENC_INT32:
            return rocksLoadIntegerObject(paccbuf, len, flags);
        case RDB_ENC_LZF:
            return rocksLoadLzfStringObject(paccbuf, flags);
        default:
            serverLog(LL_WARNING, "Unknown RDB string encoding type %d",len);
            return NULL;
        }
    }

    if (len == RDB_LENERR) {
        return NULL;
    }
    
    if (!plain) {
        o = encode ? createStringObject(NULL, len) :
                           createRawStringObject(NULL, len);
        if (len && accbufRead(paccbuf, o->ptr, len) == 0) {
            decrRefCount(o);
            return NULL;
        }
        return o;
    } else {
        buf = zmalloc(len);
        if (len && accbufRead(paccbuf, buf, len) == 0) {
            zfree(buf);
            return NULL;
        }
        return buf;
    }
}

robj *rocksLoadStringObject(accbuf_t *paccbuf) {
    return rocksGenericLoadStringObject(paccbuf, RDB_LOAD_NONE);
}

robj *rocksLoadEncodedStringObject(accbuf_t *paccbuf) {
    robj *o = NULL;
    
    o = rocksGenericLoadStringObject(paccbuf, RDB_LOAD_ENC);
    if (o != NULL) {    
        o = tryObjectEncoding(o);
    }

    return o;
}

robj *rocksLoadListObject(accbuf_t *paccbuf)
{
    robj *o = NULL;
    robj *ele = NULL;
    robj *dec = NULL;
    size_t len = 0;
    
    /* Read list value */
    len = rocksLoadLen(paccbuf, NULL);
    if (len == RDB_LENERR) {
        return NULL;
    }

    o = createQuicklistObject();
    quicklistSetOptions(o->ptr, server.list_max_ziplist_size,
                                server.list_compress_depth);

    /* Load every single element of the list */
    while(len--) {
        ele = rocksLoadEncodedStringObject(paccbuf);
        if (ele == NULL) {
            return NULL;
        }
        
        dec = getDecodedObject(ele);
        quicklistPushTail(o->ptr, dec->ptr, sdslen(dec->ptr));
        decrRefCount(dec);
        decrRefCount(ele);
    }

    return o;
}

robj *rocksLoadListQuicklistObject(accbuf_t *paccbuf)
{
    robj *o = NULL;
    size_t len = 0;
    unsigned char *zl = NULL;

    len = rocksLoadLen(paccbuf, NULL);
    if (len == RDB_LENERR) {
        return NULL;
    }
    
    o = createQuicklistObject();
    quicklistSetOptions(o->ptr, server.list_max_ziplist_size,
                        server.list_compress_depth);

    while (len--) {
        zl = rocksGenericLoadStringObject(paccbuf, RDB_LOAD_PLAIN);
        if (zl == NULL) {
            return NULL;
        }
        quicklistAppendZiplist(o->ptr, zl);
    }

    return o;
}

robj *rocksLoadListZiplistObject(accbuf_t *paccbuf)
{
    int rc = C_OK;
    unsigned char *encoded = NULL;
    robj *o = NULL;

    encoded= rocksGenericLoadStringObject(paccbuf, RDB_LOAD_PLAIN);
    if (encoded == NULL) {
        return NULL;
    }
    
    o = createObject(OBJ_LIST, encoded); /* Obj type fixed below. */
    o->encoding = OBJ_ENCODING_ZIPLIST;
    rc = listTypeConvert(o, OBJ_ENCODING_QUICKLIST);
    if (rc == -1) {
        decrRefCount(o);
        return NULL;
    }

    return o;
}

robj *rocksLoadSetObject(accbuf_t *paccbuf)
{
    robj *o = NULL;
    robj *ele = NULL;
    size_t len = 0;
    unsigned int i = 0;
    long long llval = 0;
    
    /* Read list/set value */
    len = rocksLoadLen(paccbuf, NULL);
    if (len == RDB_LENERR) {
        return NULL;
    }

    /* Use a regular set when there are too many entries. */
    if (len > server.set_max_intset_entries) {
        o = createSetObject();
        /* It's faster to expand the dict to the right size asap in order
         * to avoid rehashing */
        if (len > DICT_HT_INITIAL_SIZE) {
            dictExpand(o->ptr,len);
        }
    } else {
        o = createIntsetObject();
    }

    /* Load every single element of the list/set */
    for (i = 0; i < len; i++) {  
        ele = rocksLoadEncodedStringObject(paccbuf);
        if (ele == NULL) {
            return NULL;
        }
        ele = tryObjectEncoding(ele);

        if (o->encoding == OBJ_ENCODING_INTSET) {
            /* Fetch integer value from element */
            if (isObjectRepresentableAsLongLong(ele, &llval) == C_OK) {
                o->ptr = intsetAdd(o->ptr, llval, NULL);
            } else {
                setTypeConvert(o, OBJ_ENCODING_HT);
                dictExpand(o->ptr, len);
            }
        }

        /* This will also be called when the set was just converted
         * to a regular hash table encoded set */
        if (o->encoding == OBJ_ENCODING_HT) {
            dictAdd((dict*)o->ptr, ele, NULL);
        } else {
            decrRefCount(ele);
        }
    }

    return o;
}

robj *rocksLoadSetIntsetObject(accbuf_t *paccbuf)
{
    unsigned char *encoded = NULL;
    robj *o = NULL;

    encoded= rocksGenericLoadStringObject(paccbuf, RDB_LOAD_PLAIN);
    if (encoded == NULL) {
        return NULL;
    }
    
    o = createObject(OBJ_SET, encoded); /* Obj type fixed below. */
    o->encoding = OBJ_ENCODING_INTSET;
    if (intsetLen(o->ptr) > server.set_max_intset_entries) {
        setTypeConvert(o,OBJ_ENCODING_HT);
    }

    return o;
}

robj *rocksLoadZsetObject(accbuf_t *paccbuf)
{
    robj *o = NULL;
    robj *ele = NULL;
    size_t zsetlen = 0;
    size_t maxelelen = 0;
    zset *zs = NULL;
    double score = 0.0;
    zskiplistNode *znode = NULL;

    zsetlen = rocksLoadLen(paccbuf, NULL);
    if (zsetlen == RDB_LENERR) {
        return NULL;
    }
    
    o = createZsetObject();
    zs = o->ptr;

    /* Load every single element of the list/set */
    while(zsetlen--) { 
        ele = rocksLoadEncodedStringObject(paccbuf);
        if (ele == NULL) {
            return NULL;
        }
        ele = tryObjectEncoding(ele);
        
        if (rocksLoadDoubleValue(paccbuf, &score) != C_OK) {
            return NULL;
        }

        /* Don't care about integer-encoded strings. */
        if (sdsEncodedObject(ele) && sdslen(ele->ptr) > maxelelen) {
            maxelelen = sdslen(ele->ptr);
        }

        znode = zslInsert(zs->zsl,score,ele);
        dictAdd(zs->dict,ele,&znode->score);
        incrRefCount(ele); /* added to skiplist */
    }

    /* Convert *after* loading, since sorted sets are not stored ordered. */
    if (zsetLength(o) <= server.zset_max_ziplist_entries 
        && maxelelen <= server.zset_max_ziplist_value) {
            zsetConvert(o,OBJ_ENCODING_ZIPLIST);
    }

    return o;
}

robj *rocksLoadZsetZiplistObject(accbuf_t *paccbuf)
{
    unsigned char *encoded = NULL;
    robj *o = NULL;

    encoded= rocksGenericLoadStringObject(paccbuf, RDB_LOAD_PLAIN);
    if (encoded == NULL) {
        return NULL;
    }
    
    o = createObject(OBJ_ZSET, encoded); /* Obj type fixed below. */
    o->encoding = OBJ_ENCODING_ZIPLIST;
    if (zsetLength(o) > server.zset_max_ziplist_entries) {
        zsetConvert(o,OBJ_ENCODING_SKIPLIST);
    }

    return o;
}

/* Returns NULL on error, robj on success */
robj *rocksLoadHashObject(redisDb *db, sds key, accbuf_t *paccbuf)
{
    int rc = C_OK;
    size_t len = 0;
    int ret = 0;
    robj *o = NULL;
    robj *field = NULL;
    robj *value = NULL;

    len = rocksLoadLen(paccbuf, NULL);
    if (len == RDB_LENERR) {
        return NULL;
    }

    o = createHashObject();

    /* Too many entries? Use a hash table. */
    if (len > server.hash_max_ziplist_entries) {
        rc = hashTypeConvert(db, key, o, OBJ_ENCODING_HT);
        if (rc == -1) {
            serverLog(LL_WARNING, "hashTypeConvert for hash(%s) failed", key);
            return NULL;
        }
    }

    /* Load every field and value into the ziplist */
    while (o->encoding == OBJ_ENCODING_ZIPLIST && len > 0) {
        len--;
        /* Load raw strings */
        field = rocksLoadStringObject(paccbuf);
        if (field == NULL) {
            return NULL;
        }
        serverAssert(sdsEncodedObject(field));
        
        value = rocksLoadStringObject(paccbuf);
        if (value == NULL) {
            return NULL;
        }
        serverAssert(sdsEncodedObject(value));

        /* Add pair to ziplist */
        o->ptr = ziplistPush(o->ptr, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
        o->ptr = ziplistPush(o->ptr, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
        /* Convert to hash table if size threshold is exceeded */
        if (sdslen(field->ptr) > server.hash_max_ziplist_value ||
            sdslen(value->ptr) > server.hash_max_ziplist_value)
        {
            decrRefCount(field);
            decrRefCount(value);

            rc = hashTypeConvert(db, key, o, OBJ_ENCODING_HT);
            if (rc == -1) {
                serverLog(LL_WARNING, "hashTypeConvert for hash(%s) failed", key);
                return NULL;
            }
            
            break;
        }
        decrRefCount(field);
        decrRefCount(value);
    }

    /* Load remaining fields and values into the hash table */
    while (o->encoding == OBJ_ENCODING_HT && len > 0) {
        len--;
        /* Load encoded strings */
        
        field = rocksLoadEncodedStringObject(paccbuf);
        if (field == NULL) {
            return NULL;
        }
        
        value = rocksLoadEncodedStringObject(paccbuf);
        if (value == NULL) {
            decrRefCount(field);
            return NULL;
        }

        field = tryObjectEncoding(field);
        value = tryObjectEncoding(value);

        /* Add pair to hash table */
        ret = dictAdd((dict*)o->ptr, field, value);
        if (ret == DICT_ERR) {
            serverPanic("Duplicate keys detected");
        }
    }

    /* All pairs should be read by now */
    serverAssert(len == 0);

    return o;
}

robj *rocksLoadHashZipmapObject(redisDb *db, sds key, accbuf_t *paccbuf)
{
    int rc = C_OK;
    unsigned char *encoded = NULL;
    robj *o = NULL;
    unsigned char *zl = NULL;
    unsigned char *zi = NULL;
    unsigned char *fstr = NULL;
    unsigned char *vstr = NULL;;
    unsigned int flen = 0;
    unsigned int vlen = 0;
    unsigned int maxlen = 0;

    encoded= rocksGenericLoadStringObject(paccbuf, RDB_LOAD_PLAIN);
    if (encoded == NULL) {
        return NULL;
    }
    
    o = createObject(OBJ_STRING, encoded); /* Obj type fixed below. */

    zl = ziplistNew();
    zi = zipmapRewind(o->ptr);

    do {
        zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen);
        if (zi == NULL) {
            break;
        }

        if (flen > maxlen) {
            maxlen = flen;
        }
        
        if (vlen > maxlen) {
            maxlen = vlen;
        }
        
        zl = ziplistPush(zl, fstr, flen, ZIPLIST_TAIL);
        zl = ziplistPush(zl, vstr, vlen, ZIPLIST_TAIL);
    } while(1);

    zfree(o->ptr);
    o->ptr = zl;
    o->type = OBJ_HASH;
    o->encoding = OBJ_ENCODING_ZIPLIST;

    if (hashTypeLength(o) > server.hash_max_ziplist_entries 
        || maxlen > server.hash_max_ziplist_value)
    {
        rc = hashTypeConvert(db, key, o, OBJ_ENCODING_HT);
        if (rc == -1) {
            decrRefCount(o);
            serverLog(LL_WARNING, "hashTypeConvert for key(%s) failed", key);
            return NULL;
        }
    }

    return o;
}

robj *rocksLoadHashZiplistObject(redisDb *db, sds key, accbuf_t *paccbuf)
{
    int rc = C_OK;
    unsigned char *encoded = NULL;
    robj *o = NULL;

    encoded= rocksGenericLoadStringObject(paccbuf, RDB_LOAD_PLAIN);
    if (encoded == NULL) {
        return NULL;
    }
    
    o = createObject(OBJ_HASH, encoded); /* Obj type fixed below. */
    o->encoding = OBJ_ENCODING_ZIPLIST;
    if (hashTypeLength(o) > server.hash_max_ziplist_entries){
        rc = hashTypeConvert(db, key, o, OBJ_ENCODING_HT);
        if (rc == -1) {
            serverLog(LL_WARNING, "hashTypeConvert for key(%s) failed", key);
            decrRefCount(o);
            return NULL;
        }
    }

    return o;
}

robj *rocksLoadObject(redisDb *db, sds key, int type, accbuf_t *paccbuf)
{
    robj *o = NULL;

    if (type == RDB_TYPE_STRING) {
        /* Read string value */
        o = rocksLoadEncodedStringObject(paccbuf);
    } else if (type == RDB_TYPE_LIST) {
        o = rocksLoadListObject(paccbuf);
    } else if (type == RDB_TYPE_SET) {
        o = rocksLoadSetObject(paccbuf);
    } else if (type == RDB_TYPE_ZSET) {
        o = rocksLoadZsetObject(paccbuf);
    } else if (type == RDB_TYPE_HASH) {
        o = rocksLoadHashObject(db, key, paccbuf);
    } else if (type == RDB_TYPE_LIST_QUICKLIST) {
        o = rocksLoadListQuicklistObject(paccbuf);
    } else if (type == RDB_TYPE_HASH_ZIPMAP) {
        o = rocksLoadHashZipmapObject(db, key, paccbuf);
    } else if (type == RDB_TYPE_LIST_ZIPLIST) {
        o = rocksLoadListZiplistObject(paccbuf);
    } else if (type == RDB_TYPE_SET_INTSET) {
        o = rocksLoadSetIntsetObject(paccbuf);
    } else if (type == RDB_TYPE_ZSET_ZIPLIST) {
        o = rocksLoadZsetZiplistObject(paccbuf);
    } else if (type == RDB_TYPE_HASH_ZIPLIST) {
        o = rocksLoadHashZiplistObject(db, key, paccbuf);
    } else {
        serverLog(LL_WARNING, "Unknown RDB encoding type %d", type);
        return NULL;
    }
    
    return o;
}   

/* Returns NULL on error, object on success */
robj *loadValObjectFromDiskWithSds(redisDb *db, 
                                   unsigned long long desno,
                                   sds key, 
                                   uint32_t type, 
                                   sds *diskkey)
{
    int rc = C_OK;
    char *diskval = NULL;
    size_t diskvallen = 0;
    accbuf_t valdesc;
    robj *val = NULL;
    int valtype = 0;

    sdssetlen(*diskkey, 0);  

    if (rocksNeedExchangeKey(key)) {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%U", db->id, type, desno);
    } else {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%S", db->id, type, key);
    }
    
    rc = get_from_rocksdb(*diskkey, sdslen(*diskkey), &diskval, &diskvallen);
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                  "get value of key(%s) from disk failed", *diskkey);
        return NULL;
    }

    init_value_desc(diskval, diskvallen, &valdesc);  

    valtype = rocksLoadType(&valdesc);
    if (valtype == -1) {
        serverLog(LL_WARNING, "load vtype for key(%s) from disk failed", key);
        rocksFree(diskval);
        return NULL;
    }
    
    //serverLog(LL_WARNING, "load value(type:%d) for key(%s) from disk", 
    //          type, thiskey);
    val = rocksLoadObject(db, key, valtype, &valdesc);      
    
    if (!val) {
        serverLog(LL_WARNING, "load object for key(%s) from disk failed", key);
        rocksFree(diskval);
        return NULL;
    }

    rocksFree(diskval);
    
    return val;
}


/* Returns NULL on error, object on success */
robj *loadValObjectFromDisk(redisDb *db, 
                            unsigned long long desno, 
                            sds key, 
                            uint32_t type)
{
    int rc = C_OK;
    sds *diskkey = getClearedSharedKeySds();
    char *diskval = NULL;
    size_t diskvallen = 0;
    accbuf_t valdesc;
    robj *val = NULL;
    int valtype = 0;

    if (rocksNeedExchangeKey(key)) {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%U", db->id, type, desno);
    } else {
        *diskkey = sdscatfmt(*diskkey, "%i_%u_%S", db->id, type, key);
    }
    
    rc = get_from_rocksdb(*diskkey, sdslen(*diskkey), &diskval, &diskvallen);
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                  "get value of key(%s) from disk failed", *diskkey);
        return NULL;
    }

    init_value_desc(diskval, diskvallen, &valdesc);  

    valtype = rocksLoadType(&valdesc);
    if (valtype == -1) {
        serverLog(LL_WARNING, "load vtype for key(%s) from disk failed", key);
        rocksFree(diskval);
        return NULL;
    }
    
    //serverLog(LL_WARNING, "load value(type:%d) for key(%s) from disk", 
    //          type, thiskey);
    val = rocksLoadObject(db, key, valtype, &valdesc);      
    
    if (!val) {
        serverLog(LL_WARNING, "load object for key(%s) from disk failed", key);
        rocksFree(diskval);
        return NULL;
    }

    rocksFree(diskval);
    
    return val;
}

int loadObjectFromDisk(redisDb *db, dictEntry *de)
{
    int rc = C_OK;
    robj *val = NULL;

    serverAssert((rc = dictIsEntryValOnDisk(de)) == 1);

    val = loadValObjectFromDisk(db, de->v_sno, dictGetKey(de), de->v_type);
    if (!val) {
        return C_ERR;
    }
    
    dictSetVal(db->dict, de, val);
    dictSetEntryValNotOnDisk(de);
    
    return C_OK;
}

int loadHashFieldValueFromDiskWithSds(redisDb *db,
                                      unsigned long long desno,
                                      sds hkey, 
                                      unsigned long long fdesno,
                                      robj *field, 
                                      robj **fval,
                                      sds *diskkey)
{
    int rc = C_OK;
    char *diskval = NULL;
    size_t diskvallen = 0;
    accbuf_t valdesc;
    robj *val = NULL;

    field = getDecodedObject(field);  
    sdssetlen(*diskkey, 0);

    if (rocksNeedExchangeKey(hkey)) {
        if (rocksNeedExchangeKey((sds)field->ptr)) {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%U", 
                    db->id, OBJ_HASH, desno, fdesno);
        } else {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%S", 
                    db->id, OBJ_HASH, desno, (sds)field->ptr);
        }            
    } else {
        if (rocksNeedExchangeKey((sds)field->ptr)) {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%U", 
                    db->id, OBJ_HASH, hkey, fdesno);
        } else {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%S", 
                    db->id, OBJ_HASH, hkey, (sds)field->ptr);
        }            
    }             
    decrRefCount(field);
    
    rc = get_from_rocksdb(*diskkey, sdslen(*diskkey), &diskval, &diskvallen);
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                  "get value of key(%s) from disk failed", *diskkey);
        return C_ERR;
    }

    init_value_desc(diskval, diskvallen, &valdesc);  

    val = rocksLoadStringObject(&valdesc);
    if (val == NULL) {
        rocksFree(diskval);
        serverLog(LL_WARNING, 
                  "analyze value of key(%s) from disk failed", *diskkey);
        return C_ERR;
    }
    
    serverAssert(sdsEncodedObject(val));
    *fval = val;

    rocksFree(diskval);
    
    return C_OK;
}

int loadHashFieldValueFromDisk(redisDb *db, 
                               unsigned long long desno,
                               sds hkey,
                               unsigned long long fdesno,
                               robj *field, 
                               robj **fval)
{
    int rc = C_OK;
    sds *diskkey = getClearedSharedKeySds();
    char *diskval = NULL;
    size_t diskvallen = 0;
    accbuf_t valdesc;
    robj *val = NULL;

    field = getDecodedObject(field);  
    if (rocksNeedExchangeKey(hkey)) {
        if (rocksNeedExchangeKey(hkey)) {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%U", 
                    db->id, OBJ_HASH, desno, fdesno);
        } else {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%S", 
                    db->id, OBJ_HASH, desno, (sds)field->ptr);
        }
    } else {
        if (rocksNeedExchangeKey(hkey)) {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%U", 
                    db->id, OBJ_HASH, hkey, fdesno);
        } else {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%S", 
                    db->id, OBJ_HASH, hkey, (sds)field->ptr);
        }           
    }
    decrRefCount(field);
    
    rc = get_from_rocksdb(*diskkey, sdslen(*diskkey), &diskval, &diskvallen);
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
                  "get value of key(%s) from disk failed", *diskkey);
        return C_ERR;
    }

    init_value_desc(diskval, diskvallen, &valdesc);  

    val = rocksLoadStringObject(&valdesc);
    if (val == NULL) {
        rocksFree(diskval);
        serverLog(LL_WARNING, 
                  "analyze value of key(%s) from disk failed", *diskkey);
        return C_ERR;
    }
    
    serverAssert(sdsEncodedObject(val));
    *fval = val;

    rocksFree(diskval);
    
    return C_OK;
}

unsigned char *loadQuicklistZl(quicklistNode *node)
{
    int rc = C_OK;
    char *diskval = NULL;
    size_t diskvallen = 0;
    accbuf_t valdesc;
    unsigned char *zl = NULL;

    if (node->zl_ondisk == VAL_NOT_ON_DISK) {
        return node->zl;
    }
    
    rc = get_from_rocksdb(node->zl_dstore_key, sdslen(node->zl_dstore_key), 
                          &diskval, &diskvallen);
    if (rc != C_OK) {
        serverLog(LL_WARNING, "get value of key(%s) from disk failed",
                  node->zl_dstore_key);
        //serverAssert(rc != C_OK);
        return NULL;
    }

    init_value_desc(diskval, diskvallen, &valdesc);  

    zl = rocksGenericLoadStringObject(&valdesc, RDB_LOAD_PLAIN);
    if (zl == NULL) {
        rocksFree(diskval);
        serverLog(LL_WARNING, "analyze value of key(%s) from disk failed", 
                  node->zl_dstore_key);
        return NULL;
    }

    rocksFree(diskval);
    
    return zl;
}

/* Returns C_OK on success, C_ERR on failed */
int loadListQuicklistNodeFromDisk(quicklistNode *node)
{
    unsigned char *zl = NULL;
    
    zl = loadQuicklistZl(node);
    if (!zl) {
        serverLog(LL_WARNING, "get zl of list(%s) from disk failed",
                  node->zl_dstore_key);
        return C_ERR;          
    }
    
    updQuicklistNodeVal(node, zl);
    
    return C_OK;
}

int quicklistTryLoadZiplist(quicklistNode *node)
{
    int rc = C_OK;
    
    if (node->zl_ondisk == VAL_ON_DISK) {
        rc = loadListQuicklistNodeFromDisk(node);
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                  "load quicklist(%s) from disk failed", node->zl_dstore_key);
            return C_ERR;
        }
    }

    return C_OK;
}

void rocksFree(void *ptr)
{
    if (ptr) {
        free(ptr);
    }
}

