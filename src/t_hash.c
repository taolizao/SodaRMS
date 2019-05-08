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
#include "dict.h"
#include "rocks.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. 
 * Returns 0 on success, -1 on error. */
int hashTypeTryConversion(redisDb *db, 
                          sds hkey, 
                          robj *o, 
                          robj **argv, 
                          int start, 
                          int end) {
    int i = 0;
    int rc = 0;

    if (o->encoding != OBJ_ENCODING_ZIPLIST) {
        return 0;
    }

    for (i = start; i <= end; i++) {
        if (sdsEncodedObject(argv[i]) &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            rc = hashTypeConvert(db, hkey, o, OBJ_ENCODING_HT);
            if (rc == -1) {
                serverLog(LL_WARNING, "hashTypeConvert for hash(%s) failed",
                          hkey);
                return -1;
            }
            
            break;
        }
    }

    return 0;
}

/* Encode given objects in-place when the hash uses a dict. */
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        if (o1) {
            *o1 = tryObjectEncoding(*o1);
        }
        
        if (o2) {
            *o2 = tryObjectEncoding(*o2);
        }
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeGetFromZiplist(robj *o, robj *field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    field = getDecodedObject(field);

    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            vptr = ziplistNext(zl, fptr);
            serverAssert(vptr != NULL);
        }
    }

    decrRefCount(field);

    if (vptr != NULL) {
        ret = ziplistGet(vptr, vstr, vlen, vll);
        serverAssert(ret);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeCheckHashFieldExists(robj *o, robj *field) {
    dictEntry *de = NULL;

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    de = dictFind(o->ptr, field);
    if (de == NULL) {
        return -1;
    }

    return 0;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeGetFromHashTable(redisDb *db,  
                             unsigned long long desno,
                             robj *hkey,
                             robj *o, 
                             robj *field, 
                             robj **value) 
{
    int rc = C_OK;
    dictEntry *de= NULL;
    robj *val = NULL;

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    de = dictFind((dict *)o->ptr, field);
    if (de == NULL) {
        return -1;
    }

    if (dictIsEntryValOnDisk(de)) {  
        rc = loadHashFieldValueFromDisk(db, desno, 
                              (sds)hkey->ptr, de->v_sno, field, &val);
        if (rc != C_OK) {
            serverLog(LL_WARNING, "load hash(%s) field failed");
            return -1;
        }

        dictSetVal((dict *)o->ptr, de, val);
        dictSetEntryValNotOnDisk(de);        
    }

    val = dictGetVal(de);
    /* Update the access time for the ageing algorithm.
         * Don't do it if we have a saving child, as this will trigger
         * a copy on write madness. */
    if (server.rdb_child_pid == -1 &&
        server.aof_child_pid == -1 )
    {
        val->lru = LRU_CLOCK();
    }
    
    *value = val;
    return 0;
}

/* Higher level function of hashTypeGet*() that always returns a Redis
 * object (either new or with refcount incremented), so that the caller
 * can retain a reference or call decrRefCount after the usage.
 *
 * The lower level function can prevent copy on write so it is
 * the preferred way of doing read operations. */
robj *hashTypeGetObject(redisDb *db, robj *key, robj *o, robj *field) {
    robj *value = NULL;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) {
            if (vstr) {
                value = createStringObject((char*)vstr, vlen);
            } else {
                value = createStringObjectFromLongLong(vll);
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        robj *aux;

        dictEntry *de = dictFind(db->dict, key->ptr);
        if (!de) {
            return NULL;
        }

        if (hashTypeGetFromHashTable(db, de->v_sno, key, o, field, &aux) == 0) {
            incrRefCount(aux);
            value = aux;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return value;
}

/* Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not
 * exist. */
size_t hashTypeGetValueLength(redisDb *db, 
                              unsigned long long desno,
                              robj *key, 
                              robj *o, 
                              robj *field) {
    size_t len = 0;
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0)
            len = vstr ? vlen : sdigits10(vll);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        robj *aux;
       
        if (hashTypeGetFromHashTable(db, desno, key, o, field, &aux) == 0)
            len = stringObjectLen(aux);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return len;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
int hashTypeExists(robj *o, robj *field) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) {
            return 1;
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (hashTypeCheckHashFieldExists(o, field) == 0) {
            return 1;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return 0;
}

/* Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update.
 * This function will take care of incrementing the reference count of the
 * retained fields and value objects.
 * Returns -1 on hashTypeConvert error, otherwise for updata flag(0/1) */
int hashTypeSet(redisDb *db, 
                sds hkey,
                robj *o, 
                robj *field, 
                robj *value) {
    int update = 0;
    int rc = C_OK;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        field = getDecodedObject(field);
        value = getDecodedObject(value);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = ziplistNext(zl, fptr);
                serverAssert(vptr != NULL);
                update = 1;

                /* Delete value */
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
                zl = ziplistInsert(zl, vptr, value->ptr, sdslen(value->ptr));
            }
        }

        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            zl = ziplistPush(zl, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
            zl = ziplistPush(zl, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
        }
        o->ptr = zl;
        decrRefCount(field);
        decrRefCount(value);

        /* Check if the ziplist needs to be converted to a hash table */
        if (hashTypeLength(o) > server.hash_max_ziplist_entries) {
            rc = hashTypeConvert(db, hkey, o, OBJ_ENCODING_HT);
            if (rc == -1) {
                serverLog(LL_WARNING, "hashTypeConvert for hash(%s) failed",
                          hkey);
                return -1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictReplace(o->ptr, field, value)) { /* Insert */
            incrRefCount(field);
        } else { /* Update */
            update = 1;
        }
        incrRefCount(value);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
int hashTypeDelete(robj *o, robj *field) {
    int deleted = 0;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        field = getDecodedObject(field);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                zl = ziplistDelete(zl,&fptr);
                zl = ziplistDelete(zl,&fptr);
                o->ptr = zl;
                deleted = 1;
            }
        }

        decrRefCount(field);

    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }

    return deleted;
}

/* Return the number of elements in a hash. */
unsigned long hashTypeLength(robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        length = dictSize((dict*)o->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }

    return length;
}

hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }

    return hi;
}

void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT) {
        dictReleaseIterator(hi->di);
    }

    zfree(hi);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 * could be found and C_ERR when the iterator reaches the end. */
int hashTypeNext(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            serverAssert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
        } else {
            /* Advance cursor */
            serverAssert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        if (fptr == NULL) return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        vptr = ziplistNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return C_ERR;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, 
                                int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    serverAssert(hi->encoding == OBJ_ENCODING_ZIPLIST);

    if (what & OBJ_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        serverAssert(ret);
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        serverAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromHashTable`. */
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);

    if (what & OBJ_HASH_KEY) {
        *dst = dictGetKey(hi->de);
    } else {    
        *dst = dictGetVal(hi->de);
    }
}

/* A non copy-on-write friendly but higher level version of hashTypeCurrent*()
 * that returns an object with incremented refcount (or a new object). It is up
 * to the caller to decrRefCount() the object if no reference is retained.
 * Returns NULL on error, robj on success */
robj *hashTypeCurrentObject(redisDb *db, 
                            unsigned long long desno,
                            sds hkey, 
                            hashTypeIterator *hi, 
                            int what) {
    int rc = 0;
    robj *dst = NULL;
    robj *key = NULL;

    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            dst = createStringObject((char*)vstr, vlen);
        } else {
            dst = createStringObjectFromLongLong(vll);
        }
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        key = dictGetKey(hi->de);
        
        if (what == OBJ_HASH_VALUE && dictIsEntryValOnDisk(hi->de)) {
            rc = loadHashFieldValueFromDisk(db, desno, hkey, 
                                hi->de->v_sno, key, &dst);
            if (rc != C_OK) {
                return NULL;
            }

            //hashTypeCurrentFromHashTable(hi, what, &dst);
        } else {
            hashTypeCurrentFromHashTable(hi, what, &dst);
            incrRefCount(dst);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return dst;
}

/* for OBJ_HASH type, the first level KV pair only exists in memory */
robj *hashTypeLookupWriteOrCreate(client *c, robj *key)
{
    robj *o = lookupKeyWrite(c->db, key);

    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db, key, o);
    } else if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return NULL;
    } else {
        if (o->type != OBJ_HASH) {
            addReply(c, shared.wrongtypeerr);
            return NULL;
        }
    }
    
    return o;
}

/* return 0 on success, -1 on error */
int hashTypeConvertZiplist(redisDb *db, 
                            sds hkey,
                            robj *o, 
                            int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    if (enc == OBJ_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;
        dictEntry *de = NULL;

        de = dictFind(db->dict, hkey);
        if (!de) {
            return -1;
        }

        hi = hashTypeInitIterator(o);
        dict = dictCreate(&hashDictType, NULL);

        while (hashTypeNext(hi) != C_ERR) {
            robj *field, *value;

            field = hashTypeCurrentObject(db, de->v_sno, 
                                          hkey, hi, OBJ_HASH_KEY);
            if (!field) {
                dictRelease(dict);
                return -1;
            }             
            field = tryObjectEncoding(field);
            
            value = hashTypeCurrentObject(db, de->v_sno, 
                                          hkey, hi, OBJ_HASH_VALUE);
            if (!value) {
                decrRefCount(field);
                dictRelease(dict);
                return -1;
            }
            
            value = tryObjectEncoding(value);
            ret = dictAdd(dict, field, value);
            if (ret != DICT_OK) {
                serverLogHexDump(LL_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                decrRefCount(field);
                decrRefCount(value);
                dictRelease(dict);
                //serverAssert(ret == DICT_OK);
                return -1;
            }
        }

        hashTypeReleaseIterator(hi);
        zfree(o->ptr);

        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
        return -1;
    }

    return 0;
}

/* Returns 0 on success, -1 on error */
int hashTypeConvert(redisDb *db, sds hkey, robj *o, int enc) {
    int rc = 0;
    
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        rc = hashTypeConvertZiplist(db, hkey, o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
        rc = -1;
    } else {
        serverPanic("Unknown hash encoding");
        rc = -1;
    }

    if (rc == -1) {
        serverLog(LL_WARNING, "hashTypeConvert for key(%s) failed", hkey);
    }

    return rc;
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/
int setHashKeyValDirectToDisk(redisDb *db, 
                              unsigned long long desno,
                              robj *hashname,
                              robj *hashobj, 
                              robj *field, 
                              robj *fval,
                              int *pupd) 
{
    int rc = C_OK;
    int n = 0;
    int hfield_exists = 1;
    dictEntry *de = NULL;
    dictEntry auxentry;
    robj *fname = NULL;
    sds *diskkey = getClearedSharedKeySds();
    sds *diskval = getClearedSharedValSds();

    if (dictAdd((dict *)hashobj->ptr, field, NULL) == DICT_OK) {
        // add success, that means 'field' not exists in 'hashobj' before
        incrRefCount(field);
        hfield_exists = 0;
    }

    fname = getDecodedObject(field); 
    de = dictFind((dict *)hashobj->ptr, field);
    if (!de) {
        serverLog(LL_WARNING, "hash(%s) not contains field(%s)", 
                  (sds)hashname->ptr, (sds)fname->ptr);
        goto l_err;         
    }
        
    if (rocksNeedExchangeKey(hashname->ptr)) {
        if (rocksNeedExchangeKey((sds)fname->ptr)) {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%U", db->id, OBJ_HASH, 
                        desno, de->v_sno);
        } else {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%U_%S", db->id, OBJ_HASH, 
                        desno, (sds)fname->ptr);
        }
    } else {
        if (rocksNeedExchangeKey((sds)fname->ptr)) {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%U", db->id, OBJ_HASH, 
                        (sds)hashname->ptr, de->v_sno);
        } else {
            *diskkey = sdscatfmt(*diskkey, "%i_%u_%S_%S", db->id, OBJ_HASH, 
                        (sds)hashname->ptr, (sds)fname->ptr);
        }               
    }                   
                        
    n = rocksGenStringObjectVal(fval, diskval, ROCKS_NOT_SAVE_STRING_TYPE);
    if (n == -1) {            
        serverLog(LL_WARNING, 
            "generate value sds for HASH(%s) field(%s) failed",
            (sds)hashname->ptr, (sds)fname->ptr);
        goto l_err;
    }

    rc = write_to_rocksdb(*diskkey, sdslen(*diskkey), 
                          *diskval, sdslen(*diskval));
    if (rc != C_OK) {
        serverLog(LL_WARNING, 
          "write KEY(%s) ==> VALUE(%s) to rocksdb failed", *diskkey, *diskval);         
        goto l_err;;      
    }

    if (hfield_exists) {
        *pupd = 1;
    }

    setEntryValOnDisk((dict *)hashobj->ptr, de, fval->type);
    dictSetVal((dict *)hashobj->ptr, de, NULL);

    decrRefCount(fname);

    return C_OK;

l_err:
    if (!hfield_exists) {
        dictDelete((dict *)hashobj->ptr, field); 
    }
    decrRefCount(fname);

    return C_ERR;
}

void hsetCommand(client *c) {
    int rc = C_OK;
    int update = 0;
    robj *o = NULL;    

    o = hashTypeLookupWriteOrCreate(c, c->argv[1]);
    if (o == NULL) {
        return;
    }

    // check if key length or val length overrun server.hash_max_ziplist_value
    // if true then call hashTypeConvert to convert it to real hash
    rc = hashTypeTryConversion(c->db, c->argv[1]->ptr, o, c->argv, 2, 3);
    if (rc == -1) {
        addReply(c, shared.dstoreerr);
        return;
    }

    // if o->encoding == OBJ_ENCODING_HT, then try to encode key and value
    hashTypeTryObjectEncoding(o, &c->argv[2], &c->argv[3]);
    
    if (o->encoding == OBJ_ENCODING_HT
        && useDiskStore() 
        && needWriteToDiskDirect()) {
        dictEntry *de = dictFind(c->db->dict, c->argv[1]->ptr);
        if (!de) {
            addReply(c, shared.czero);
            return;
        }
    
        rc = setHashKeyValDirectToDisk(c->db, de->v_sno, c->argv[1], o, 
                                       c->argv[2], c->argv[3], &update);
        if (rc != C_OK) {
            addReply(c, shared.czero);
            return;
        }
    } else {
        update = hashTypeSet(c->db, c->argv[1]->ptr, o, c->argv[2], c->argv[3]);
    }

    addReply(c, update ? shared.czero : shared.cone);
    signalModifiedKey(c->db, c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH, "hset", c->argv[1], c->db->id);
    server.dirty++;
}

void hsetnxCommand(client *c) {
    robj *o;
    int rc = C_OK;
    int update = 0;

    o = hashTypeLookupWriteOrCreate(c,c->argv[1]);
    if (o == NULL) {
        return;
    }
    //hashTypeTryConversion(o,c->argv,2,3);
    rc = hashTypeTryConversion(c->db, c->argv[1]->ptr, o, c->argv, 2, 3);
    if (rc == -1) {
        addReply(c, shared.dstoreerr);
        return;
    }

    if (hashTypeExists(o, c->argv[2])) {
        addReply(c, shared.czero);
    } else {
        hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);

        if (o->encoding == OBJ_ENCODING_HT
            && useDiskStore() 
            && needWriteToDiskDirect()) {
            dictEntry *de = dictFind(c->db->dict, c->argv[1]->ptr);
            if (!de) {
                addReply(c, shared.czero);
                return;
            }
            
            rc = setHashKeyValDirectToDisk(c->db, de->v_sno, c->argv[1], o,
                                c->argv[2], c->argv[3], &update);
            if (rc != C_OK) {
                addReply(c, shared.czero);
                return;
            }
        } else {
            //update = hashTypeSet(o, c->argv[2], c->argv[3]);
            update = hashTypeSet(c->db, c->argv[1]->ptr, 
                                 o, c->argv[2], c->argv[3]);                     
        }
        
        //hashTypeSet(o,c->argv[2],c->argv[3]);
        
        addReply(c, shared.cone);
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        server.dirty++;
    }
}

void hmsetCommand(client *c) {
    int i = 0;
    int count = 0;
    robj *o = NULL;
    int rc = C_OK;
    int update = 0;

    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    //hashTypeTryConversion(o,c->argv,2,c->argc-1);
    rc = hashTypeTryConversion(c->db, c->argv[1]->ptr, 
                               o, c->argv, 2, c->argc - 1);
    if (rc == -1) {
        addReply(c, shared.dstoreerr);
        return;
    }

    dictEntry *de = dictFind(c->db->dict, c->argv[1]->ptr);
    if (!de) {
        addReply(c, shared.czero);
        return;
    }
    
    for (i = 2; i < c->argc; i += 2) {
        hashTypeTryObjectEncoding(o,&c->argv[i], &c->argv[i+1]);
        
        if (o->encoding == OBJ_ENCODING_HT
            && useDiskStore() 
            && needWriteToDiskDirect()) {
            rc = setHashKeyValDirectToDisk(c->db, de->v_sno, c->argv[1], o,
                                c->argv[i], c->argv[i+1], &update);
            if (rc != C_OK) {
                continue;
            } else {
                count++;
            }
        } else {
            update = hashTypeSet(c->db, c->argv[1]->ptr, 
                                 o, c->argv[i], c->argv[i+1]);                     
            // assume operation should be success, even hashTypeConvert
            // failed, but hash KV already set in memory
            count++; 
        }
        //hashTypeSet(o,c->argv[i],c->argv[i+1]);
    }

    if (count) {
        addReply(c, shared.ok);        
        signalModifiedKey(c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH, "hset", c->argv[1], c->db->id);
        server.dirty++;
    } else {
        addReply(c, shared.err);
    }
}

void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o, *current, *new;

    if (getLongLongFromObjectOrReply(c, c->argv[3], &incr, NULL) != C_OK) {
        return;
    }

    o = hashTypeLookupWriteOrCreate(c, c->argv[1]);
    if (o == NULL) {
        return;
    }

    current = hashTypeGetObject(c->db, c->argv[1], o, c->argv[2]);
    if (current != NULL) {
        if (getLongLongFromObjectOrReply(c, current, &value,
                            "hash value is not an integer") != C_OK) {
            decrRefCount(current);
            return;
        }
        decrRefCount(current);
    } else {
        value = 0;
    }

    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN - oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX - oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr;
    new = createStringObjectFromLongLong(value);
    hashTypeTryObjectEncoding(o, &c->argv[2], NULL);
    hashTypeSet(c->db, c->argv[1]->ptr, o, c->argv[2], new);
    
    decrRefCount(new);
    addReplyLongLong(c,value);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    server.dirty++;
}

void hincrbyfloatCommand(client *c) {
    double long value, incr;
    robj *o, *current, *new, *aux;

    if (getLongDoubleFromObjectOrReply(c, c->argv[3], &incr, NULL) != C_OK) {
        return;
    }

    o = hashTypeLookupWriteOrCreate(c, c->argv[1]);
    if (o == NULL) {
        return;
    }

    current = hashTypeGetObject(c->db, c->argv[1], o, c->argv[2]);
    if (current != NULL) {
        if (getLongDoubleFromObjectOrReply(c, current, &value,
                        "hash value is not a valid float") != C_OK) {
            decrRefCount(current);
            return;
        }
        decrRefCount(current);
    } else {
        value = 0;
    }

    value += incr;
    new = createStringObjectFromLongDouble(value,1);
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);
    //hashTypeSet(o,c->argv[2],new);
    hashTypeSet(c->db, c->argv[1]->ptr, o, c->argv[2], new);
    
    addReplyBulk(c,new);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    aux = createStringObject("HSET",4);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,3,new);
    decrRefCount(new);
}

static void addHashFieldToReply(client *c, 
                                unsigned long long desno,
                                robj *key, 
                                robj *o, 
                                robj *field) {
    int ret;

    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    } else if (o->encoding == OBJ_ENCODING_HT) {
        robj *value;

        ret = hashTypeGetFromHashTable(c->db, desno, key, o, field, &value);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            addReplyBulk(c, value);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }
}

void hgetCommand(client *c) {
    robj *o = NULL;
    dictEntry *de = NULL;

    o = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk);
    if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return;
    }
    
    if (o == NULL || checkType(c, o, OBJ_HASH)) {
        return;
    }

    de = dictFind(c->db->dict, c->argv[1]->ptr);
    if (!de) {
        addReply(c, shared.nullbulk);
        return;
    }
    
    addHashFieldToReply(c, de->v_sno, c->argv[1], o, c->argv[2]);
}

void hmgetCommand(client *c) {
    robj *o;
    int i;
    dictEntry *de = NULL;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    o = lookupKeyRead(c->db, c->argv[1]);
    if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return;
    }
            
    if (o != NULL && o->type != OBJ_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    de = dictFind(c->db->dict, c->argv[1]->ptr);
    if (!de) {
        addReply(c, shared.nullbulk);
        return;
    }

    addReplyMultiBulkLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, de->v_sno, c->argv[1], o, c->argv[i]);
    }
}

/* Search and remove an element from hash.
 *  @db      server.db[xx]
 *  @key     KEY refers to dict object 'o' in 'db'
 *  @type    type for dict object 'd', which should be OBJ_HASH
 *  @d       dict refers to KEY 'key' in 'db'
 *  @field   field object in dict 'd'
 * Return 1 on deleted and 0 on not found. */
int hashSearchDelKey(redisDb *db, 
                     unsigned long long desno,
                     robj *key, 
                     unsigned char type,
                     dict *d, 
                     robj *field)
{
    unsigned int h = 0;
    unsigned int idx = 0;
    dictEntry *he = NULL;
    dictEntry *prevHe = NULL;
    int table = 0;

    if (d->ht[0].size == 0) {
        return DICT_ERR; /* d->ht[0].table is NULL */
    }
    
    if (dictIsRehashing(d)) {
        _dictRehashStep(d);
    }
    
    h = dictHashKey(d, field);

    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        prevHe = NULL;
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                /* Unlink the element from the list */
                if (prevHe) {
                    prevHe->next = he->next;
                } else {
                    d->ht[table].table[idx] = he->next;
                }

                freeHashFieldVal(db, desno, (sds)key->ptr, type, d, he);
                        
                d->ht[table].used--;
                return DICT_OK;
            }
            prevHe = he;
            he = he->next;
        }
        
        if (!dictIsRehashing(d)) {
            break;
        }
    }
    
    return DICT_ERR; /* not found */
}


/* Delete an element from a hash.
 *  @db      server.db[xx]
 *  @key     KEY refers to dict object 'o' in 'db'
 *  @o       object refers to KEY 'key' in 'db'
 *  @field   field object in dict 'o'
 * Return 1 on deleted and 0 on not found. */
int hashDeleteKey(redisDb *db, 
                  unsigned long long desno,
                  robj *key, 
                  robj *o, 
                  robj *field) {
    int deleted = 0;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = NULL;
        unsigned char *fptr = NULL;

        field = getDecodedObject(field);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                zl = ziplistDelete(zl,&fptr);
                zl = ziplistDelete(zl,&fptr);
                o->ptr = zl;
                deleted = 1;
            }
        }

        decrRefCount(field);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (hashSearchDelKey(db, desno, key, o->type, 
                            (dict*)o->ptr, field) == C_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            if (htNeedsResize(o->ptr)) {
                dictResize(o->ptr);
            }
        }

    } else {
        serverPanic("Unknown hash encoding");
    }

    return deleted;
}


void hdelCommand(client *c) {
    robj *o = NULL;
    dictEntry *de = NULL;
    int j = 0;
    int deleted = 0;
    int keyremoved = 0;

    o = lookupKeyWriteOrReply(c, c->argv[1], shared.czero);
    if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return;
    }
            
    if (o == NULL || checkType(c, o, OBJ_HASH)) {
        return;
    }

    de = dictFind(c->db->dict, c->argv[1]->ptr);
    if (!de) {
        addReplyLongLong(c,deleted);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        if (hashDeleteKey(c->db, de->v_sno, c->argv[1], o, c->argv[j])) { 
        //if (hashTypeDelete(o,c->argv[j])) {
            deleted++;
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db, c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

void hlenCommand(client *c) {
    robj *o;

    o = lookupKeyReadOrReply(c, c->argv[1], shared.czero);
    if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return;
    }
    
    if (o == NULL || checkType(c, o, OBJ_HASH)) {
        return;
    }

    addReplyLongLong(c, hashTypeLength(o));
}

void hstrlenCommand(client *c) {
    robj *o = NULL;

    o = lookupKeyReadOrReply(c, c->argv[1], shared.czero);
    if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return;
    }
    
    if (o == NULL || checkType(c, o, OBJ_HASH)) {
        return;
    }

    dictEntry *de = dictFind(c->db->dict, c->argv[1]->ptr);
    if (!de) {
        addReply(c, shared.czero);
        return;
    }
        
    addReplyLongLong(c, 
          hashTypeGetValueLength(c->db, de->v_sno, c->argv[1], o, c->argv[2]));
}

/* Returns C_OK on sucess, C_ERR on error */
int addHashIteratorCursorToReply(client *c, 
                                 unsigned long long desno,
                                 robj *hkey, 
                                 hashTypeIterator *hi, 
                                 int what) {
    int rc = C_OK;
    robj *value = NULL;
    robj *key = NULL;
    int needfree = 0;
    
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }

    } else if (hi->encoding == OBJ_ENCODING_HT) {        
        key = dictGetKey(hi->de);

        if (what == OBJ_HASH_VALUE && dictIsEntryValOnDisk(hi->de)) {
            rc = loadHashFieldValueFromDisk(c->db, desno, 
                              hkey->ptr, hi->de->v_sno, key, &value);
            if (rc != C_OK) {
                serverLog(LL_WARNING, "loadHashFieldValueFromDisk for "
                    "hash(%s) field(%s) failed", hkey->ptr, key->ptr);
                return C_ERR;
            }
            needfree = 1;
        } else {
            hashTypeCurrentFromHashTable(hi, what, &value);
        }
        
        addReplyBulk(c, value);
        decrRefCountByFlag(needfree, value);
    } else {
        serverPanic("Unknown hash encoding");
        return C_ERR;
    }

    return C_OK;
}

void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0;
    int rc = C_OK;
    dictEntry *de = NULL;

    o = lookupKeyReadOrReply(c, c->argv[1], shared.emptymultibulk);
    if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return;
    }
    
    if (o == NULL || checkType(c, o, OBJ_HASH)) {
        return;
    }

    de = dictFind(c->db->dict, c->argv[1]->ptr);
    if (!de) {
        addReply(c, shared.emptymultibulk);
        return;
    }

    if (flags & OBJ_HASH_KEY) {
        multiplier++;
    }
    
    if (flags & OBJ_HASH_VALUE) {
        multiplier++;
    }

    length = hashTypeLength(o) * multiplier;
    addReplyMultiBulkLen(c, length);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR) {
        if (flags & OBJ_HASH_KEY) {
            rc = addHashIteratorCursorToReply(c, de->v_sno,
                                              c->argv[1], hi, OBJ_HASH_KEY);
            if (rc != C_OK) {
                addReply(c, shared.nullbulk);
            }
            count++;
        }
        
        if (flags & OBJ_HASH_VALUE) {
            rc = addHashIteratorCursorToReply(c, de->v_sno,
                                              c->argv[1], hi, OBJ_HASH_VALUE);
            if (rc != C_OK) {
                addReply(c, shared.nullbulk);
            }
            count++;
        }
    }

    hashTypeReleaseIterator(hi);
    serverAssert(count == length);
}

void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

void hgetallCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

void hexistsCommand(client *c) {
    robj *o;

    o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return;
    }
    
    if (o == NULL || checkType(c,o,OBJ_HASH)) {
        return;
    }

    addReply(c, hashTypeExists(o,c->argv[2]) ? shared.cone : shared.czero);
}

void hscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c, c->argv[2], &cursor) == C_ERR) {
        return;
    }

    o = lookupKeyReadOrReply(c, c->argv[1], shared.emptyscan);
    if (o == shared.dstoreerr) {
        addReply(c, shared.dstoreerr);
        return;
    }
    
    if (o == NULL || checkType(c, o, OBJ_HASH)) {
        return;
    }
    
    scanGenericCommand(c, o, cursor);
}
