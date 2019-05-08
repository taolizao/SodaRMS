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
#include "lzf.h"    /* LZF compression library */
#include "zipmap.h"
#include "endianconv.h"
#include "rdb.h"
#include "rocks.h"
#include "const.h"

#include <time.h>
#include <signal.h>
#include <errno.h>
#include <ctype.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/utsname.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <atomic.h>
#include <limits.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <locale.h>

#include "multitask.h"
#include "list.h"

#define rdbExitReportCorruptRDB(...) rdbCheckThenExit(__LINE__,__VA_ARGS__)

extern int rdbCheckMode;
void rdbCheckError(const char *fmt, ...);
void rdbCheckSetError(const char *fmt, ...);

void rdbCheckThenExit(int linenum, char *reason, ...) {
    va_list ap;
    char msg[1024];
    int len;

    len = snprintf(msg,sizeof(msg),
        "Internal error in RDB reading function at rdb.c:%d -> ", linenum);
    va_start(ap,reason);
    vsnprintf(msg+len,sizeof(msg)-len,reason,ap);
    va_end(ap);

    if (!rdbCheckMode) {
        serverLog(LL_WARNING, "%s", msg);
        char *argv[2] = {"",server.rdb_filename};
        redis_check_rdb_main(2,argv);
    } else {
        rdbCheckError("%s",msg);
    }
    exit(1);
}

static int rdbWriteRawToSds(sds *savebuf, void *p, size_t len) {
    return dumpWriteRawToSds(savebuf, p, len);
}

static int rdbWriteRaw(rio *rdb, void *p, size_t len) {
    return dumpWriteRaw(rdb, p, len);
}

void rdbSaveTypeToSds(sds *savebuf, unsigned char type) {
    *savebuf = sdscatlen(*savebuf, &type, 1);
    //return rdbWriteRaw(rdb,&type,1);
}

/* Returns -1 if failed, otherwise returns length to write */
int rdbSaveType(rio *rdb, unsigned char type) {
    return rdbWriteRaw(rdb,&type,1);
}

/* Load a "type" in RDB format, that is a one byte unsigned integer.
 * This function is not only used to load object types, but also special
 * "types" like the end-of-file type, the EXPIRE type, and so forth. */
int rdbLoadType(rio *rdb) {
    unsigned char type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}

time_t rdbLoadTime(rio *rdb) {
    int32_t t32;
    if (rioRead(rdb,&t32,4) == 0) return -1;
    return (time_t)t32;
}

void rdbSaveMillisecondTimeToSds(sds *savebuf, long long t) {
    *savebuf = sdscatlen(*savebuf, (int64_t *)&t, 8);
    //int64_t t64 = (int64_t) t;
    //return rdbWriteRaw(rdb,&t64,8);
}

int rdbSaveMillisecondTime(rio *rdb, long long t) {
    int64_t t64 = (int64_t) t;
    return rdbWriteRaw(rdb,&t64,8);
}

long long rdbLoadMillisecondTime(rio *rdb) {
    int64_t t64;
    if (rioRead(rdb,&t64,8) == 0) return -1;
    return (long long)t64;
}

/* Saves an encoded length. The first two bits in the first byte are used to
 * hold the encoding type. See the RDB_* definitions for more information
 * on the types of encoding. */
int rdbSaveLenToSds(sds *savebuf, uint32_t len) {
    unsigned char buf[2];
    size_t nwritten = 0;

    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        *savebuf = sdscatlen(*savebuf, buf, 1);
        //if (rdbWriteRaw(rdb,buf,1) == -1) return -1;        
        nwritten = 1;
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF)|(RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        *savebuf = sdscatlen(*savebuf, buf, 2);
        //if (rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;
    } else {
        /* Save a 32 bit len */
        buf[0] = (RDB_32BITLEN << 6);
        *savebuf = sdscatlen(*savebuf, buf, 1);
        //if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonl(len);
        *savebuf = sdscatlen(*savebuf, &len, 4);
        //if (rdbWriteRaw(rdb,&len,4) == -1) return -1;
        nwritten = 1 + 4;
    }
    return nwritten;
}


/* Saves an encoded length. The first two bits in the first byte are used to
 * hold the encoding type. See the RDB_* definitions for more information
 * on the types of encoding. */
int rdbSaveLen(rio *rdb, uint32_t len) {
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1<<6)) {
        /* Save a 6 bit len */
        buf[0] = (len&0xFF)|(RDB_6BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1<<14)) {
        /* Save a 14 bit len */
        buf[0] = ((len>>8)&0xFF)|(RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;
    } else {
        /* Save a 32 bit len */
        buf[0] = (RDB_32BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonl(len);
        if (rdbWriteRaw(rdb,&len,4) == -1) return -1;
        nwritten = 1+4;
    }
    return nwritten;
}

/* Load an encoded length. The "isencoded" argument is set to 1 if the length
 * is not actually a length but an "encoding type". See the RDB_ENC_*
 * definitions in rdb.h for more information. */
uint32_t rdbLoadLen(rio *rdb, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;
    if (rioRead(rdb,buf,1) == 0) return RDB_LENERR;
    type = (buf[0]&0xC0)>>6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) *isencoded = 1;
        return buf[0]&0x3F;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        return buf[0]&0x3F;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (rioRead(rdb,buf+1,1) == 0) return RDB_LENERR;
        return ((buf[0]&0x3F)<<8)|buf[1];
    } else if (type == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        if (rioRead(rdb,&len,4) == 0) return RDB_LENERR;
        return ntohl(len);
    } else {
        rdbExitReportCorruptRDB(
            "Unknown length encoding %d in rdbLoadLen()",type);
        return -1; /* Never reached. */
    }
}

/* Encodes the "value" argument as integer when it fits in the supported ranges
 * for encoded types. If the function successfully encodes the integer, the
 * representation is stored in the buffer pointer to by "enc" and the string
 * length is returned. Otherwise 0 is returned. */
int rdbEncodeInteger(long long value, unsigned char *enc) {
    if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

/* Loads an integer-encoded object with the specified encoding type "enctype".
 * The returned value changes according to the flags, see
 * rdbGenerincLoadStringObject() for more info. */
void *rdbLoadIntegerObject(rio *rdb, int enctype, int flags) {
    int plain = flags & RDB_LOAD_PLAIN;
    int encode = flags & RDB_LOAD_ENC;
    unsigned char enc[4];
    long long val;

    if (enctype == RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        rdbExitReportCorruptRDB("Unknown RDB integer encoding type %d",enctype);
    }
    if (plain) {
        char buf[LONG_STR_SIZE], *p;
        int len = ll2string(buf,sizeof(buf),val);
        p = zmalloc(len);
        memcpy(p,buf,len);
        return p;
    } else if (encode) {
        return createStringObjectFromLongLong(val);
    } else {
        return createObject(OBJ_STRING,sdsfromlonglong(val));
    }
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space */
int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc) {
    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    value = strtoll(s, &endptr, 10);
    if (endptr[0] != '\0') return 0;
    ll2string(buf,32,value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    if (strlen(buf) != len || memcmp(buf,s,len)) return 0;

    return rdbEncodeInteger(value,enc);
}

ssize_t rdbSaveLzfBlobToSds(sds *savebuf, 
                            void *data, 
                            size_t compress_len,
                            size_t original_len) {
    unsigned char byte = 0;
    ssize_t n = 0;
    ssize_t nwritten = 0;

    /* Data compressed! Let's save it on disk */
    byte = (RDB_ENCVAL << 6) | RDB_ENC_LZF;
    n = rdbWriteRawToSds(savebuf, &byte, 1);    
    //if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) goto writeerr;
    nwritten += n;

    n = rdbSaveLenToSds(savebuf, compress_len);  
    //if ((n = rdbSaveLen(rdb,compress_len)) == -1) goto writeerr;
    nwritten += n;

    n = rdbSaveLenToSds(savebuf, original_len);
    //if ((n = rdbSaveLen(rdb,original_len)) == -1) goto writeerr;
    nwritten += n;

    n = rdbWriteRawToSds(savebuf, data, compress_len);  
    //if ((n = rdbWriteRaw(rdb,data,compress_len)) == -1) goto writeerr;
    nwritten += n;

    return nwritten;

//writeerr:
    return -1;
}

ssize_t rdbSaveLzfBlob(rio *rdb, void *data, size_t compress_len,
                       size_t original_len) {
    unsigned char byte;
    ssize_t n, nwritten = 0;

    /* Data compressed! Let's save it on disk */
    byte = (RDB_ENCVAL<<6)|RDB_ENC_LZF;
    if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,compress_len)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,original_len)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbWriteRaw(rdb,data,compress_len)) == -1) goto writeerr;
    nwritten += n;

    return nwritten;

writeerr:
    return -1;
}

ssize_t rdbSaveLzfStringObjectToSds(sds *savebuf, 
                                    unsigned char *s, 
                                    size_t len) {
    size_t comprlen = 0;
    size_t outlen = 0;
    void *out = NULL;
    ssize_t nwritten = 0;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= LZF_COMPRESS_SRC_MIN_LENGTH) {
        return 0;
    }
    
    outlen = len + LZF_COMPRESS_DST_APPEND_LENGTH;
    out = zmalloc(outlen + 1);
    if (out == NULL) {
        return 0;
    }
    
    comprlen = lzf_compress(s, len, out, outlen);
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }

    nwritten = rdbSaveLzfBlobToSds(savebuf, out, comprlen, len);
    zfree(out);
    
    return nwritten;
}


ssize_t rdbSaveLzfStringObject(rio *rdb, unsigned char *s, size_t len) {
    size_t comprlen = 0;
    size_t outlen = 0;
    void *out = NULL;
    ssize_t nwritten = 0;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= LZF_COMPRESS_SRC_MIN_LENGTH) {
        return 0;
    }
    
    outlen = len + LZF_COMPRESS_DST_APPEND_LENGTH;
    out = zmalloc(outlen + 1);
    if (out == NULL) {
        return 0;
    }
    
    comprlen = lzf_compress(s, len, out, outlen);
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }

    nwritten = rdbSaveLzfBlob(rdb, out, comprlen, len);
    zfree(out);
    
    return nwritten;
}

/* Load an LZF compressed string in RDB format. The returned value
 * changes according to 'flags'. For more info check the
 * rdbGenericLoadStringObject() function. */
void *rdbLoadLzfStringObject(rio *rdb, int flags) {
    int plain = flags & RDB_LOAD_PLAIN;
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    if ((clen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if ((c = zmalloc(clen)) == NULL) goto err;

    /* Allocate our target according to the uncompressed size. */
    if (plain) {
        val = zmalloc(len);
    } else {
        if ((val = sdsnewlen(NULL,len)) == NULL) goto err;
    }

    /* Load the compressed representation and uncompress it to target. */
    if (rioRead(rdb,c,clen) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) == 0) {
        if (rdbCheckMode) rdbCheckSetError("Invalid LZF compressed string");
        goto err;
    }
    zfree(c);

    if (plain)
        return val;
    else
        return createObject(OBJ_STRING,val);
err:
    zfree(c);
    if (plain)
        zfree(val);
    else
        sdsfree(val);
    return NULL;
}

/* Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form */
ssize_t rdbSaveRawStringToSds(sds *savebuf, unsigned char *s, size_t len) {
    int enclen = 0;
    ssize_t n = 0;
    ssize_t nwritten = 0;

    /* Try integer encoding */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s, len, buf)) > 0) {
            //if (rdbWriteRaw(rdb,buf,enclen) == -1) return -1;
            rdbWriteRawToSds(savebuf, buf, enclen);
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    if (server.rdb_compression && len > 20) {
        n = rdbSaveLzfStringObjectToSds(savebuf, s, len);
        if (n == -1) {
            return -1;
        }
        
        if (n > 0) {
            return n;
        }
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
    n = rdbSaveLenToSds(savebuf, len);
    //if ((n = rdbSaveLen(rdb,len)) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        rdbWriteRawToSds(savebuf, s, len);
        //if (rdbWriteRaw(rdb,s,len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}


/* Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form */
ssize_t rdbSaveRawString(rio *rdb, unsigned char *s, size_t len) {
    int enclen;
    ssize_t n, nwritten = 0;

    /* Try integer encoding */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
            if (rdbWriteRaw(rdb,buf,enclen) == -1) return -1;
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    if (server.rdb_compression && len > 20) {
        n = rdbSaveLzfStringObject(rdb,s,len);
        if (n == -1) return -1;
        if (n > 0) return n;
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
    if ((n = rdbSaveLen(rdb,len)) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        if (rdbWriteRaw(rdb,s,len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* Save a long long value as either an encoded string or a string. */
ssize_t rdbSaveLongLongAsStringObjectToSds(sds *savebuf, long long value) {
    unsigned char buf[32];
    ssize_t n = 0;
    ssize_t nwritten = 0;
    int enclen = rdbEncodeInteger(value, buf);
    if (enclen > 0) {
        *savebuf = sdscatlen(*savebuf, buf, enclen);
        return enclen;
        //return rdbWriteRaw(rdb,buf,enclen);
    } else {
        /* Encode as string */
        enclen = ll2string((char*)buf, 32, value);
        serverAssert(enclen < 32);
        n = rdbSaveLenToSds(savebuf, enclen);
        //if ((n = rdbSaveLen(rdb,enclen)) == -1) return -1;
        nwritten += n;
        
        n = rdbWriteRawToSds(savebuf, buf, enclen);
        //if ((n = rdbWriteRaw(rdb,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

/* Save a long long value as either an encoded string or a string. */
ssize_t rdbSaveLongLongAsStringObject(rio *rdb, long long value) {
    unsigned char buf[32];
    ssize_t n, nwritten = 0;
    int enclen = rdbEncodeInteger(value,buf);
    if (enclen > 0) {
        return rdbWriteRaw(rdb,buf,enclen);
    } else {
        /* Encode as string */
        enclen = ll2string((char*)buf,32,value);
        serverAssert(enclen < 32);
        if ((n = rdbSaveLen(rdb,enclen)) == -1) return -1;
        nwritten += n;
        if ((n = rdbWriteRaw(rdb,buf,enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

/* Like rdbSaveStringObjectRaw() but handle encoded objects */
int rdbSaveStringObjectToSds(sds *savebuf, robj *obj) {
    /* Avoid to decode the object, then encode it again, if the
     * object is already integer encoded. */
    if (obj->encoding == OBJ_ENCODING_INT) {
        return rdbSaveLongLongAsStringObjectToSds(savebuf, (long)obj->ptr);
    } else {
        serverAssertWithInfo(NULL, obj, sdsEncodedObject(obj));
        return rdbSaveRawStringToSds(savebuf, obj->ptr, sdslen(obj->ptr));
    }
}

/* Like rdbSaveStringObjectRaw() but handle encoded objects */
int rdbSaveStringObject(rio *rdb, robj *obj) {
    /* Avoid to decode the object, then encode it again, if the
     * object is already integer encoded. */
    if (obj->encoding == OBJ_ENCODING_INT) {
        return rdbSaveLongLongAsStringObject(rdb,(long)obj->ptr);
    } else {
        serverAssertWithInfo(NULL,obj,sdsEncodedObject(obj));
        return rdbSaveRawString(rdb,obj->ptr,sdslen(obj->ptr));
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
void *rdbGenericLoadStringObject(rio *rdb, int flags) {
    int encode = flags & RDB_LOAD_ENC;
    int plain = flags & RDB_LOAD_PLAIN;
    int isencoded;
    uint32_t len;

    len = rdbLoadLen(rdb,&isencoded);
    if (isencoded) {
        switch(len) {
        case RDB_ENC_INT8:
        case RDB_ENC_INT16:
        case RDB_ENC_INT32:
            return rdbLoadIntegerObject(rdb,len,flags);
        case RDB_ENC_LZF:
            return rdbLoadLzfStringObject(rdb,flags);
        default:
            rdbExitReportCorruptRDB("Unknown RDB string encoding type %d",len);
        }
    }

    if (len == RDB_LENERR) return NULL;
    if (!plain) {
        robj *o = encode ? createStringObject(NULL,len) :
                           createRawStringObject(NULL,len);
        if (len && rioRead(rdb,o->ptr,len) == 0) {
            decrRefCount(o);
            return NULL;
        }
        return o;
    } else {
        void *buf = zmalloc(len);
        if (len && rioRead(rdb,buf,len) == 0) {
            zfree(buf);
            return NULL;
        }
        return buf;
    }
}

robj *rdbLoadStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,RDB_LOAD_NONE);
}

robj *rdbLoadEncodedStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,RDB_LOAD_ENC);
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
int rdbSaveDoubleValueToSds(sds *savebuf, double val) {
    unsigned char buf[128];
    int len;

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
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf)-1,(long long)val);
        else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }

    *savebuf = sdscatlen(*savebuf, buf, len);
    return len;
    //return rdbWriteRaw(rdb,buf,len);
}


/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
int rdbSaveDoubleValue(rio *rdb, double val) {
    unsigned char buf[128];
    int len;

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
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf)-1,(long long)val);
        else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    return rdbWriteRaw(rdb,buf,len);
}

/* For information about double serialization check rdbSaveDoubleValue() */
int rdbLoadDoubleValue(rio *rdb, double *val) {
    char buf[256];
    unsigned char len;

    if (rioRead(rdb,&len,1) == 0) return -1;
    switch(len) {
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;
    default:
        if (rioRead(rdb,buf,len) == 0) return -1;
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        return 0;
    }
}

/* Save the object type of object "o". 
 * Returns -1 if failed, otherwise returns length saved */
int rdbSaveObjectTypeToSds(sds *savebuf, robj *o) {
    switch (o->type) {
    case OBJ_STRING:
        rdbSaveTypeToSds(savebuf, RDB_TYPE_STRING);
        return 1;
    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            rdbSaveTypeToSds(savebuf, RDB_TYPE_LIST_QUICKLIST);
            return 1;
        } else {
            serverPanic("Unknown list encoding");
        }
    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET) {
            rdbSaveTypeToSds(savebuf, RDB_TYPE_SET_INTSET);
            return 1;
        } else if (o->encoding == OBJ_ENCODING_HT) {
            rdbSaveTypeToSds(savebuf, RDB_TYPE_SET);
            return 1;
        } else {
            serverPanic("Unknown set encoding");
        }
    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            rdbSaveTypeToSds(savebuf, RDB_TYPE_ZSET_ZIPLIST);
            return 1;
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            rdbSaveTypeToSds(savebuf, RDB_TYPE_ZSET);
            return 1;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            rdbSaveTypeToSds(savebuf, RDB_TYPE_HASH_ZIPLIST);
            return 1;
        } else if (o->encoding == OBJ_ENCODING_HT) {
            rdbSaveTypeToSds(savebuf, RDB_TYPE_HASH);
            return 1;
        } else {
            serverPanic("Unknown hash encoding");
        }
    default:
        serverPanic("Unknown object type");
        return -1;
    }
    
    return -1; /* avoid warning */
}


/* Save the object type of object "o". 
 * Returns -1 if failed, otherwise returns length saved */
int rdbSaveObjectType(rio *rdb, robj *o) {
    switch (o->type) {
    case OBJ_STRING:
        return rdbSaveType(rdb,RDB_TYPE_STRING);
    case OBJ_LIST:
        if (o->encoding == OBJ_ENCODING_QUICKLIST)
            return rdbSaveType(rdb,RDB_TYPE_LIST_QUICKLIST);
        else
            serverPanic("Unknown list encoding");
    case OBJ_SET:
        if (o->encoding == OBJ_ENCODING_INTSET)
            return rdbSaveType(rdb,RDB_TYPE_SET_INTSET);
        else if (o->encoding == OBJ_ENCODING_HT)
            return rdbSaveType(rdb,RDB_TYPE_SET);
        else
            serverPanic("Unknown set encoding");
    case OBJ_ZSET:
        if (o->encoding == OBJ_ENCODING_ZIPLIST)
            return rdbSaveType(rdb,RDB_TYPE_ZSET_ZIPLIST);
        else if (o->encoding == OBJ_ENCODING_SKIPLIST)
            return rdbSaveType(rdb,RDB_TYPE_ZSET);
        else
            serverPanic("Unknown sorted set encoding");
    case OBJ_HASH:
        if (o->encoding == OBJ_ENCODING_ZIPLIST)
            return rdbSaveType(rdb,RDB_TYPE_HASH_ZIPLIST);
        else if (o->encoding == OBJ_ENCODING_HT)
            return rdbSaveType(rdb,RDB_TYPE_HASH);
        else
            serverPanic("Unknown hash encoding");
    default:
        serverPanic("Unknown object type");
        return -1;
    }
    
    return -1; /* avoid warning */
}

/* Use rdbLoadType() to load a TYPE in RDB format, but returns -1 if the
 * type is not specifically a valid Object Type. */
int rdbLoadObjectType(rio *rdb) {
    int type;
    if ((type = rdbLoadType(rdb)) == -1) return -1;
    if (!rdbIsObjectType(type)) return -1;
    return type;
}

ssize_t rdbSaveListObjectToSds(dump_task_priv_t *tpriv,
                               robj *o)
{
    int rc = C_OK;
    ssize_t n = 0;
    long compress_len = 0;
    ssize_t nwritten = 0;
    int needfree = 0;
    quicklist *ql = NULL;
    quicklistNode *node = NULL;
    void *data = NULL;
    
    /* Save a list value */
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        ql = o->ptr;
        node = ql->head;

        n = rdbSaveLenToSds(&tpriv->thd_wrbuf, ql->len);
        if (n == -1) {
            return -1;
        }
        nwritten += n;

        do {
            needfree = 0;
            if (node->zl_ondisk == VAL_ON_DISK) {
                rc = loadListQuicklistNodeFromDisk(node);
                if (rc != C_OK) {
                    return -1;
                }
                needfree = 1;
            } 
            
            if (quicklistNodeIsCompressed(node)) {
                compress_len = quicklistGetLzf(node, &data);
                if (compress_len == -1) {
                    return -1;
                }

                n = rdbSaveLzfBlobToSds(&tpriv->thd_wrbuf, data, 
                                        compress_len, node->sz);
                if (n == -1) {
                    return -1;
                }
                nwritten += n;
            } else {
                n = rdbSaveRawStringToSds(&tpriv->thd_wrbuf, 
                                          node->zl, node->sz);
                if (n == -1) {
                    return -1;
                }
                nwritten += n;
            }

            if (needfree) {
                zfree(node->zl);
                node->zl_ondisk = VAL_ON_DISK;
            }

            //rc = rdbSaveThdWriteData(ptaskpoolpriv, tpriv, 1);
            //if (rc != C_OK) {
            //    serverLog(LL_WARNING, "rdbSaveThdWriteData for list failed");
            //    return -1;
            //}
        } while ((node = node->next));
    } else {
        serverPanic("Unknown list encoding");
    }

    return nwritten;
}

ssize_t rdbSaveSetObjectToSds(dump_task_priv_t *tpriv, robj *o)
{
    ssize_t n = 0;
    ssize_t nwritten = 0;
    dict *set = NULL;
    dictIterator *di = NULL;
    dictEntry *de = NULL;
    robj *eleobj = NULL;
    size_t l = 0;

    /* Save a set value */
    if (o->encoding == OBJ_ENCODING_HT) {
        set = o->ptr;
        di = dictGetIterator(set);

        n = rdbSaveLenToSds(&tpriv->thd_wrbuf, dictSize(set));
        if (n == -1) {
            return -1;
        }
        nwritten += n;

        while((de = dictNext(di)) != NULL) {
            eleobj = dictGetKey(de);
            n = rdbSaveStringObjectToSds(&tpriv->thd_wrbuf, eleobj);
            if (n == -1) {
                return -1;
            }
            nwritten += n;
        }
        dictReleaseIterator(di);
    } else if (o->encoding == OBJ_ENCODING_INTSET) {
        l = intsetBlobLen((intset*)o->ptr);

        n = rdbSaveRawStringToSds(&tpriv->thd_wrbuf,o->ptr,l);
        if (n == -1) {
            return -1;
        }
        nwritten += n;
    } else {
        serverPanic("Unknown set encoding");
    }

    return nwritten;
}

ssize_t rdbSaveZsetObjectToSds(dump_task_priv_t *tpriv,
                               robj *o)
{
    ssize_t n = 0;
    ssize_t nwritten = 0;
    size_t l = 0;
    zset *zs = NULL;
    dictIterator *di = NULL;
    dictEntry *de = NULL;
    robj *eleobj = NULL;
    double *score = NULL;

    /* Save a sorted set value */
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        l = ziplistBlobLen((unsigned char*)o->ptr);

        n = rdbSaveRawStringToSds(&tpriv->thd_wrbuf, o->ptr, l);
        if (n == -1) {
            return -1;
        }
        nwritten += n;
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zs = o->ptr;
        di = dictGetIterator(zs->dict);

        n = rdbSaveLenToSds(&tpriv->thd_wrbuf, dictSize(zs->dict));
        if (n == -1) {
            return -1;
        }
        nwritten += n;

        while((de = dictNext(di)) != NULL) {
            eleobj = dictGetKey(de);
            score = dictGetVal(de);

            n = rdbSaveStringObjectToSds(&tpriv->thd_wrbuf, eleobj);
            if (n == -1) {
                return -1;
            }
            nwritten += n;

            n = rdbSaveDoubleValueToSds(&tpriv->thd_wrbuf, *score);
            if (n == -1) {
                return -1;
            }
            nwritten += n;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    return nwritten;
}

ssize_t rdbSaveHashObjectToSds(dump_taskpool_priv_t *ptaskpoolpriv,
                               dump_task_priv_t *tpriv,
                               unsigned long long desno,
                               robj *ko,
                               robj *o)
{
    int rc = C_OK;
    ssize_t n = 0;
    ssize_t nwritten = 0;
    int needfree = 0;
    size_t l = 0;
    robj *val = NULL;
    robj *key = NULL;
    dictIterator *di = NULL;
    dictEntry *de = NULL;

    /* Save a hash value */
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        l = ziplistBlobLen((unsigned char*)o->ptr);

        n = rdbSaveRawStringToSds(&tpriv->thd_wrbuf, o->ptr, l);
        if (n == -1) {
            return -1;
        }
        nwritten += n;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        di = dictGetIterator(o->ptr);

        n = rdbSaveLenToSds(&tpriv->thd_wrbuf, dictSize((dict*)o->ptr));
        if (n == -1) {
            return -1;
        }
        nwritten += n;

        while((de = dictNext(di)) != NULL) {
            needfree = 0;
            key = dictGetKey(de);

            if (dictIsEntryValOnDisk(de)) {  
                rc = loadHashFieldValueFromDiskWithSds(
                                ptaskpoolpriv->task_ext.db, desno, 
                                (sds)(ko->ptr), de->v_sno, key, 
                                &val, &tpriv->thd_rbuf);
                if (rc != C_OK) {
                    serverLog(LL_WARNING, "load hash(%s) field failed",
                              (sds)(ko->ptr));
                    return -1;
                }  
                needfree = 1;
            } else {
                val = dictGetVal(de);
            }

            n = rdbSaveStringObjectToSds(&tpriv->thd_wrbuf, key);
            if (n == -1) {
                decrRefCountByFlag(needfree, val);
                return -1;
            }
            nwritten += n;

            n = rdbSaveStringObjectToSds(&tpriv->thd_wrbuf, val);
            if (n == -1) {
                decrRefCountByFlag(needfree, val);
                return -1;
            }
            nwritten += n;

            decrRefCountByFlag(needfree, val);
        }
        dictReleaseIterator(di);

    } else {
        serverPanic("Unknown hash encoding");
    }

    return nwritten;
}

/* Save a Redis object. Returns -1 on error, number of bytes written on success. */
ssize_t rdbSaveObjectToSds(dump_taskpool_priv_t *ptaskpoolpriv,
                           dump_task_priv_t *tpriv,
                           unsigned long long desno,
                           robj *ko, 
                           robj *o) 
{
    ssize_t n = 0;

    if (o->type == OBJ_STRING) {
        /* Save a string value */
        n = rdbSaveStringObjectToSds(&tpriv->thd_wrbuf, o);
    } else if (o->type == OBJ_LIST) {
        n = rdbSaveListObjectToSds(tpriv, o);
    } else if (o->type == OBJ_SET) {
        n = rdbSaveSetObjectToSds(tpriv, o);
    } else if (o->type == OBJ_ZSET) {
        n = rdbSaveZsetObjectToSds(tpriv, o);        
    } else if (o->type == OBJ_HASH) {
        n = rdbSaveHashObjectToSds(ptaskpoolpriv, tpriv, desno, ko, o);
    } else {
        serverPanic("Unknown object type");
    }
    
    return n;
}

/* Save a Redis object. Returns -1 on error, number of bytes written on success. */
ssize_t rdbSaveObject(redisDb *db, 
                      unsigned long long desno, 
                      robj *ko, 
                      rio *rdb, 
                      robj *o) {
    int rc = C_OK;
    robj *val = NULL;
    robj *key = NULL;
    ssize_t n = 0;
    ssize_t nwritten = 0;
    int needfree = 0;

    if (o->type == OBJ_STRING) {
        /* Save a string value */
        if ((n = rdbSaveStringObject(rdb,o)) == -1) return -1;
        nwritten += n;
    } else if (o->type == OBJ_LIST) {
        /* Save a list value */
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;

            if ((n = rdbSaveLen(rdb,ql->len)) == -1) return -1;
            nwritten += n;

            do {
                needfree = 0;
                if (node->zl_ondisk == VAL_ON_DISK) {
                    rc = loadListQuicklistNodeFromDisk(node);
                    if (rc != C_OK) {
                        return -1;
                    }
                    needfree = 1;
                } 
                
                if (quicklistNodeIsCompressed(node)) {
                    void *data;
                    long compress_len = quicklistGetLzf(node, &data);
                    if (compress_len == -1) {
                        return -1;
                    }

                    n = rdbSaveLzfBlob(rdb, data, compress_len, node->sz);
                    if (n == -1) {
                        return -1;
                    }
                    nwritten += n;
                } else {
                    n = rdbSaveRawString(rdb,node->zl,node->sz);
                    if (n == -1) {
                        return -1;
                    }
                    nwritten += n;
                }

                if (needfree) {
                    zfree(node->zl);
                    node->zl_ondisk = VAL_ON_DISK;
                }
            } while ((node = node->next));
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        /* Save a set value */
        if (o->encoding == OBJ_ENCODING_HT) {
            dict *set = o->ptr;
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize(set))) == -1) return -1;
            nwritten += n;

            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                if ((n = rdbSaveStringObject(rdb,eleobj)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset*)o->ptr);

            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        /* Save a sorted set value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize(zs->dict))) == -1) return -1;
            nwritten += n;

            while((de = dictNext(di)) != NULL) {
                robj *eleobj = dictGetKey(de);
                double *score = dictGetVal(de);

                if ((n = rdbSaveStringObject(rdb,eleobj)) == -1) return -1;
                nwritten += n;
                if ((n = rdbSaveDoubleValue(rdb,*score)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        /* Save a hash value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;

        } else if (o->encoding == OBJ_ENCODING_HT) {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize((dict*)o->ptr))) == -1) return -1;
            nwritten += n;

            while((de = dictNext(di)) != NULL) {
                needfree = 0;
                key = dictGetKey(de);

                if (dictIsEntryValOnDisk(de)) {  
                    rc = loadHashFieldValueFromDisk(db, desno, (sds)(ko->ptr), 
                                                    de->v_sno, key, &val);
                    if (rc != C_OK) {
                        serverLog(LL_WARNING, "load hash(%s) field failed");
                        return -1;
                    }  
                    needfree = 1;
                } else {
                    val = dictGetVal(de);
                }

                if ((n = rdbSaveStringObject(rdb, key)) == -1) {
                    decrRefCountByFlag(needfree, val);
                    return -1;
                }
                nwritten += n;
                if ((n = rdbSaveStringObject(rdb, val)) == -1) {
                    decrRefCountByFlag(needfree, val);
                    return -1;
                }
                
                nwritten += n;
                decrRefCountByFlag(needfree, val);
            }
            dictReleaseIterator(di);

        } else {
            serverPanic("Unknown hash encoding");
        }

    } else {
        serverPanic("Unknown object type");
    }
    return nwritten;
}

/* Return the length the object will have on disk if saved with
 * the rdbSaveObject() function. Currently we use a trick to get
 * this length with very little changes to the code. In the future
 * we could switch to a faster solution. */
size_t rdbSavedObjectLen(redisDb *db, 
                         unsigned long long desno,
                         robj *ko, 
                         robj *o) 
{
    ssize_t len = rdbSaveObject(db, desno, ko, NULL, o);
    serverAssertWithInfo(NULL, o, len != -1);
    return len;
}

/* Save a key-value pair, with expire time, type, key, value.
 * On error -1 is returned.
 * On success if the key was actually saved 1 is returned, otherwise 0
 * is returned (the key was already expired). */
int rdbSaveKeyValuePair(redisDb *db, rio *rdb, robj *key, dictEntry *de,
                        expireExtDesc *expiretime, long long now)
{
    int rc = 0;
    robj *val = NULL;
    int needfreeval = 0;
    
    /* Save the expire time */
    if (expiretime) {
        /* If this key is already expired skip it */
        if (expiretime->expire_time < now) {
            return 0;
        }

        if (expiretime->needdel_realtime) {
            rc = rdbSaveType(rdb, RDB_OPCODE_REALTIME_EXPIRETIME_MS);
        } else {
            rc = rdbSaveType(rdb, RDB_OPCODE_EXPIRETIME_MS);
        }
        if (rc == -1) {
            return -1;
        }

        rc = rdbSaveMillisecondTime(rdb, expiretime->expire_time);
        if (rc == -1) {
            return -1;
        }
    }

    if (dictIsEntryValOnDisk(de)) {
        //size_t t_start = mstime();
               
        val = loadValObjectFromDisk(db, de->v_sno, key->ptr, de->v_type);
        if (!val) {
            return -1;
        }
        needfreeval = 1;

        //size_t t_end = mstime();
        //serverLog(LL_WARNING, "loadValObjectFromDisk load key(%s) cost "
        //              "%ld ms", key->ptr, t_end - t_start);
    } else {
        val = dictGetVal(de);
    }

    /* Save type, key, value */
    if (rdbSaveObjectType(rdb, val) == -1) {
        decrRefCountByFlag(needfreeval, val);        
        return -1;
    }
    
    if (rdbSaveStringObject(rdb, key) == -1) {
        decrRefCountByFlag(needfreeval, val);
        return -1;
    }
    
    if (rdbSaveObject(db, de->v_sno, key, rdb, val) == -1) {
        decrRefCountByFlag(needfreeval, val);
        return -1;
    }

    decrRefCountByFlag(needfreeval, val);
    
    return 1;
}

/* Save an AUX field. */
int rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen) {
    if (rdbSaveType(rdb,RDB_OPCODE_AUX) == -1) return -1;
    if (rdbSaveRawString(rdb,key,keylen) == -1) return -1;
    if (rdbSaveRawString(rdb,val,vallen) == -1) return -1;
    return 1;
}

/* Wrapper for rdbSaveAuxField() used when key/val length can be obtained
 * with strlen(). */
int rdbSaveAuxFieldStrStr(rio *rdb, char *key, char *val) {
    return rdbSaveAuxField(rdb,key,strlen(key),val,strlen(val));
}

/* Wrapper for strlen(key) + integer type (up to long long range). */
int rdbSaveAuxFieldStrInt(rio *rdb, char *key, long long val) {
    char buf[LONG_STR_SIZE];
    int vlen = ll2string(buf,sizeof(buf),val);
    return rdbSaveAuxField(rdb,key,strlen(key),buf,vlen);
}

/* Save a few default AUX fields with information about the RDB generated. */
int rdbSaveInfoAuxFields(rio *rdb) {
    int redis_bits = (sizeof(void*) == 8) ? 64 : 32;

    /* Add a few fields about the state when the RDB was created. */
    if (rdbSaveAuxFieldStrStr(rdb,"redis-ver",REDIS_VERSION) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"redis-bits",redis_bits) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"ctime",time(NULL)) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"used-mem",zmalloc_used_memory()) == -1) return -1;
    return 1;
}

void rdbSaveTimeMark(long long *start, int thdid, char *msg)
{
    long long dur = 0;
    long long end = mstime();

    dur = end - (*start);
    serverLog(LL_WARNING, 
             "Thread %d %s start at %lld and end at %lld, cost %lld ms",
             thdid, msg, *start, end, dur);
    *start = end; 
}

int rdbThdSaveKeyValPair(dump_taskpool_priv_t *ptaskpoolpriv,
                         dump_task_priv_t *tpriv,
                         dump_denode_t *pdenode)
{
    int rc = C_OK;
    sds keystr = NULL;
    robj key;    
    robj *val = NULL;
    int needfreeval = 0;
    expireExtDesc *expiredesc = NULL;
    task_ext_t *ptaskext = &ptaskpoolpriv->task_ext;
    
    keystr = dictGetKey(pdenode->de);
    initStaticStringObject(key, keystr);
    getExpireDesc(ptaskext->db, &key, &expiredesc);

    /* Save the expire time */
    if (expiredesc) {
        /* If this key is already expired skip it */
        if (expiredesc->expire_time < ptaskext->now) {
            return C_OK;
        }

        if (expiredesc->needdel_realtime) {
            rdbSaveTypeToSds(&tpriv->thd_wrbuf, 
                             RDB_OPCODE_REALTIME_EXPIRETIME_MS);
        } else {
            rdbSaveTypeToSds(&tpriv->thd_wrbuf, RDB_OPCODE_EXPIRETIME_MS);
        }

        rdbSaveMillisecondTimeToSds(&tpriv->thd_wrbuf, expiredesc->expire_time);
    }
    
    if (dictIsEntryValOnDisk(pdenode->de)) {
        //size_t t_start = mstime();             
        val = loadValObjectFromDiskWithSds(ptaskext->db, 
                                pdenode->de->v_sno, keystr, 
                                pdenode->de->v_type, &tpriv->thd_rbuf);
        if (!val) {
            serverLog(LL_WARNING, "loadValObjectFromDisk for key(%s) failed",
                      keystr);
            return C_ERR;
        }
        needfreeval = 1;
        //size_t t_end = mstime();
        //serverLog(LL_WARNING, "loadValObjectFromDisk load key(%s) cost "
        //              "%ld ms", keystr, t_end - t_start);
    } else {
        val = dictGetVal(pdenode->de);
    }

    //serverLog(LL_WARNING, "Thread %d rdb save key(%s type:%d)",
    //          taskid, keystr, val->type);

    /* Save type, key, value */
    rc = rdbSaveObjectTypeToSds(&tpriv->thd_wrbuf, val);
    if (rc == -1) {
        decrRefCountByFlag(needfreeval, val);        
        return C_ERR;
    }

    rc = rdbSaveStringObjectToSds(&tpriv->thd_wrbuf, &key);
    if (rc == -1) {
        decrRefCountByFlag(needfreeval, val);
        return C_ERR;
    }

    rc = rdbSaveObjectToSds(ptaskpoolpriv, tpriv, 
                            pdenode->de->v_sno, &key, val);
    if (rc == -1) {
        decrRefCountByFlag(needfreeval, val);
        return C_ERR;
    }

    decrRefCountByFlag(needfreeval, val);
    
    return C_OK;   
}

int rdbSaveThdProc(void *priv)
{
    int rc = C_OK;
    task_desc_t *ptask = (task_desc_t *)priv;
    task_pool_t *ptaskpool = NULL;
    dump_denode_t *pdenode = NULL;
    dump_taskpool_priv_t *tpoolpriv = NULL;
    dump_task_priv_t *tpriv = NULL;

    if (!ptask) {
        return C_OK;
    }

    ptaskpool = ptask->task_ppool;
    if (!ptaskpool) {
        return C_OK;
    }

    tpoolpriv = (dump_taskpool_priv_t *)ptaskpool->taskpool_taskprivdata;
    if (!tpoolpriv) {
        return C_OK;
    }

    tpriv = &tpoolpriv->task_privs[ptask->task_id];    
    tpriv->thd_wrbuf = sdsCheckAndReset(&tpriv->thd_wrbuf, 
                                        server.dump_thdbuf_size);
    
    while (1) {
        pdenode = NULL;
        
        statistic_getobj((void **)&pdenode, &tpriv->thd_stat);
        if (!pdenode) {
            if (tpoolpriv->task_can_stop && tpriv->thd_stat.stat_count == 0) {
                break;
            }
            
            usleep(5);
            continue;
        }

        rc = rdbThdSaveKeyValPair(tpoolpriv, tpriv, pdenode);
        if (rc != C_OK) {
            serverLog(LL_WARNING, "rdbThdSaveKeyValPair failed");
            ptaskpool->taskpool_hasfailed++;
            break;
        }

        rc = dumpSaveThdWriteData(tpoolpriv, tpriv, 1);
        if (rc != C_OK) {
            serverLog(LL_WARNING, "rdbSaveThdWriteData failed");
            ptaskpool->taskpool_hasfailed++;
            break;
        }
    }

    if (rc != C_OK) {
        return C_ERR;
    }

    rc = dumpSaveThdWriteData(tpoolpriv, tpriv, 0);
    if (rc != C_OK) {
        serverLog(LL_WARNING, "rdbSaveThdWriteData failed");
        ptaskpool->taskpool_hasfailed++;
        return C_ERR;
    }

    serverLog(LL_WARNING, "Thread %d save %d keys to rdb, "
              "which started at %ld and duration %ld ms",
              ptask->task_id, tpriv->thd_opnr, 
              tpriv->thd_opstart, mstime() - tpriv->thd_opstart);

    return C_OK;
}

int rdbSaveSingleDbCont(redisDb *db, rio *rdb, long long now)
{
    int rc = C_OK;
    dictIterator *di = NULL;    
    dict *d = db->dict;
    dictEntry *de = NULL;
    expireExtDesc *expiredesc = NULL;    
    sds keystr = NULL;
    robj key;
    task_pool_t taskpool;
    size_t tend = 0;
    size_t tstart = mstime();
           
    di = dictGetSafeIterator(d);
    if (!di) {
        return C_ERR;
    }      

    if (server.dump_concurrency == DUMP_CONCURRENCY) {
        rc = dumpSaveTaskpoolInitAndStart(&taskpool, db, rdb, 
                                now, "DUMPRDB", rdbSaveThdProc);
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                      "dumpSaveTaskpoolInitAndStart to dump rdb failed");
            goto werr;          
        }
    }

    /* Iterate this DB writing every entry */
    while((de = dictNext(di)) != NULL) {
        if (server.dump_concurrency == DUMP_CONCURRENCY) {
            rc = dumpSaveSingleDentry(&taskpool, de);
            if (rc != C_OK) {
                serverLog(LL_WARNING, "dumpSaveSingleDentry for rdb failed");
                dumpSaveSetTaskHasFailed (&taskpool);        
                dumpSaveSetTaskCanStop(&taskpool); 
                rc = -1;
                goto taskcheck;
            }                       
        } else {
            keystr = dictGetKey(de);                        
            initStaticStringObject(key, keystr);
            getExpireDesc(db, &key, &expiredesc);
            rc = rdbSaveKeyValuePair(db, rdb, &key, de, expiredesc, now);
            if (rc == -1) {
                serverLog(LL_WARNING, "rdbSaveKeyValuePair key(%s) failed",
                          keystr);
                goto werr;
            }
         }
    }
    
    dictReleaseIterator(di);
    di = NULL; /* So that we don't release it again on error. */

taskcheck:
    if (server.dump_concurrency == DUMP_CONCURRENCY) {
        rc = dumpSaveTaskpoolWaitFinal(&taskpool);
        if (rc != C_OK) {
            serverLog(LL_WARNING, 
                      "dumpSaceTaskpoolWaitFinal for dump rdb failed");
            rc = -1;
        }
    }       

werr:
    if (di) {
        dictReleaseIterator(di);
    }

    tend = mstime();
    serverLog(LL_WARNING, "rdbSaveSingleDbCont save DB(%d) start at %ld and "
             "end at %ld, totally cost %ld ms with res:%d", 
             db->id, tstart, tend, tend - tstart, rc);
    
    return (rc >= 0) ? C_OK : C_ERR;
}

/* Produces a dump of the database in RDB format sending it to the specified
 * Redis I/O channel. On success C_OK is returned, otherwise C_ERR
 * is returned and part of the output, or all the output, can be
 * missing because of I/O errors.
 *
 * When the function returns C_ERR and if 'error' is not NULL, the
 * integer pointed by 'error' is set to the value of errno just after the I/O
 * error. */
int rdbSaveRio(rio *rdb, int *error) {
    int rc = 0;
    char magic[10];
    int j = 0;
    redisDb *db = NULL;
    long long now = mstime();
    uint64_t cksum = 0;
    uint32_t db_size = 0;
    uint32_t expires_size = 0;

    if (server.rdb_checksum) {
        rdb->update_cksum = rioGenericUpdateChecksum;
    }
    
    snprintf(magic,sizeof(magic),"REDIS%04d",RDB_VERSION);
    
    if (rdbWriteRaw(rdb, magic, 9) == -1) {
        goto werr;
    }
    
    if (rdbSaveInfoAuxFields(rdb) == -1) {
        goto werr;
    }

    for (j = 0; j < server.dbnum; j++) {
        db = server.db + j;

        if (dictSize(db->dict) == 0) {
            continue;
        }

        /* Write the SELECT DB opcode */
        if (rdbSaveType(rdb, RDB_OPCODE_SELECTDB) == -1) {
            goto werr;
        }
        
        if (rdbSaveLen(rdb, j) == -1) {
            goto werr;
        }

        /* Write the RESIZE DB opcode. We trim the size to UINT32_MAX, which
         * is currently the largest type we are able to represent in RDB sizes.
         * However this does not limit the actual size of the DB to load since
         * these sizes are just hints to resize the hash tables. */
        
        db_size = (dictSize(db->dict) <= UINT32_MAX) ?
                                dictSize(db->dict) : UINT32_MAX;
        expires_size = (dictSize(db->expires) <= UINT32_MAX) ?
                                dictSize(db->expires) : UINT32_MAX;
        if (rdbSaveType(rdb, RDB_OPCODE_RESIZEDB) == -1) {
            goto werr;
        }
        
        if (rdbSaveLen(rdb, db_size) == -1) {
            goto werr;
        }
        
        if (rdbSaveLen(rdb, expires_size) == -1) {
            goto werr;
        }
    
        rc = rdbSaveSingleDbCont(db, rdb, now);
        if (rc != C_OK) {
            serverLog(LL_WARNING, "rdbSaveSingleDb %d failed", db->id);
            goto werr;
        }
    }

    /* EOF opcode */
    if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1) {
        goto werr;
    }

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. */
    cksum = rdb->cksum;
    memrev64ifbe(&cksum);
    if (rioWrite(rdb, &cksum, 8) == 0) {
        goto werr;
    }
    
    return C_OK;

werr:
    if (error) {
        *error = errno;
    }
    
    return C_ERR;
}

/* This is just a wrapper to rdbSaveRio() that additionally adds a prefix
 * and a suffix to the generated RDB dump. The prefix is:
 *
 * $EOF:<40 bytes unguessable hex string>\r\n
 *
 * While the suffix is the 40 bytes hex string we announced in the prefix.
 * This way processes receiving the payload can understand when it ends
 * without doing any processing of the content. */
int rdbSaveRioWithEOFMark(rio *rdb, int *error) {
    char eofmark[RDB_EOF_MARK_SIZE];

    getRandomHexChars(eofmark,RDB_EOF_MARK_SIZE);
    if (error) *error = 0;
    if (rioWrite(rdb,"$EOF:",5) == 0) goto werr;
    if (rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) goto werr;
    if (rioWrite(rdb,"\r\n",2) == 0) goto werr;
    if (rdbSaveRio(rdb,error) == C_ERR) goto werr;
    if (rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) goto werr;
    return C_OK;

werr: /* Write error. */
    /* Set 'error' only if not already set by rdbSaveRio() call. */
    if (error && *error == 0) *error = errno;
    return C_ERR;
}

/* Save the DB on disk. Return C_ERR on error, C_OK on success. */
int rdbSave(char *filename) {
    char tmpfile[256];
    char cwd[MAXPATHLEN]; /* Current working dir path for error messages. */
    FILE *fp;
    rio rdb;
    int error = 0;

    snprintf(tmpfile,256,"temp-%d.rdb", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        char *cwdp = getcwd(cwd,MAXPATHLEN);
        serverLog(LL_WARNING,
            "Failed opening the RDB file %s (in server root dir %s) "
            "for saving: %s",
            filename,
            cwdp ? cwdp : "unknown",
            strerror(errno));
        return C_ERR;
    }

    rioInitWithFile(&rdb,fp);

    if (rdbSaveRio(&rdb,&error) == C_ERR) {
        errno = error;
        goto werr;
    }

    /* Make sure data will not remain on the OS's output buffers */
    if (fflush(fp) == EOF) goto werr;    
    if (fsync(fileno(fp)) == -1) goto werr;    
    if (fclose(fp) == EOF) goto werr;    

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    if (rename(tmpfile,filename) == -1) {
        char *cwdp = getcwd(cwd,MAXPATHLEN);
        serverLog(LL_WARNING,
            "Error moving temp DB file %s on the final "
            "destination %s (in server root dir %s): %s",
            tmpfile,
            filename,
            cwdp ? cwdp : "unknown",
            strerror(errno));
        unlink(tmpfile);
        return C_ERR;
    }

    serverLog(LL_NOTICE,"DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    server.lastbgsave_status = C_OK;
    return C_OK;

werr:
    serverLog(LL_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return C_ERR;
}

int rdbSaveBackground(char *filename) {
    int rc = C_OK;
    pid_t childpid = -1;
    long long start = 0;

    if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) {
        return C_ERR;
    }  

    rc = create_rocksdb_snapshot();
    if (rc != C_OK) {
        serverLog(LL_WARNING, "create_rocksdb_snapshot failed");
        return C_ERR;
    }

    server.dirty_before_bgsave = server.dirty;
    server.lastbgsave_try = time(NULL);

    start = ustime();
    if ((childpid = fork()) == 0) {
        int retval;  

        /* Child */
        closeListeningSockets(0);
        redisSetProcTitle("redis-rdb-bgsave");
        retval = rdbSave(filename);
        if (retval == C_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                serverLog(LL_NOTICE,
                    "RDB: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }
        }

        // release snapshot in child procedure
        // release_rocksdb_snapshot(REAL_RELEASE_SNAP);
        
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent */
        /* parent procedure still use original rdb */
        release_rocksdb_snapshot(FAKE_RELEASE_SNAP);
        
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        if (childpid == -1) {
            server.lastbgsave_status = C_ERR;
            real_release_rocksdb_snapshot();
            serverLog(LL_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return C_ERR;
        }
        serverLog(LL_NOTICE,"Background saving started by pid %d",childpid);
        
        server.rdb_save_time_start = time(NULL);
        server.rdb_child_pid = childpid;
        server.rdb_child_type = RDB_CHILD_TYPE_DISK;
        updateDictResizePolicy();
        return C_OK;
    }
    return C_OK; /* unreached */
}

void rdbRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,sizeof(tmpfile),"temp-%d.rdb", (int) childpid);
    unlink(tmpfile);
}

/* Load a Redis object of the specified type from the specified file.
 * On success a newly allocated object is returned, otherwise NULL. */
robj *rdbLoadObject(redisDb *db, sds key, int rdbtype, rio *rdb) {
    int rc = C_OK;
    robj *o = NULL, *ele, *dec;
    size_t len;
    unsigned int i;

    if (rdbtype == RDB_TYPE_STRING) {
        /* Read string value */
        if ((o = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        o = tryObjectEncoding(o);
    } else if (rdbtype == RDB_TYPE_LIST) {
        /* Read list value */
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;

        o = createQuicklistObject();
        quicklistSetOptions(o->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);

        /* Load every single element of the list */
        while(len--) {
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            dec = getDecodedObject(ele);
            size_t len = sdslen(dec->ptr);
            rc = quicklistPushTail(o->ptr, dec->ptr, len);
            decrRefCount(dec);
            decrRefCount(ele);
            if (rc == -1) {
                return NULL;
            }
        }
    } else if (rdbtype == RDB_TYPE_SET) {
        /* Read list/set value */
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;

        /* Use a regular set when there are too many entries. */
        if (len > server.set_max_intset_entries) {
            o = createSetObject();
            /* It's faster to expand the dict to the right size asap in order
             * to avoid rehashing */
            if (len > DICT_HT_INITIAL_SIZE)
                dictExpand(o->ptr,len);
        } else {
            o = createIntsetObject();
        }

        /* Load every single element of the list/set */
        for (i = 0; i < len; i++) {
            long long llval;
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            ele = tryObjectEncoding(ele);

            if (o->encoding == OBJ_ENCODING_INTSET) {
                /* Fetch integer value from element */
                if (isObjectRepresentableAsLongLong(ele,&llval) == C_OK) {
                    o->ptr = intsetAdd(o->ptr,llval,NULL);
                } else {
                    setTypeConvert(o,OBJ_ENCODING_HT);
                    dictExpand(o->ptr,len);
                }
            }

            /* This will also be called when the set was just converted
             * to a regular hash table encoded set */
            if (o->encoding == OBJ_ENCODING_HT) {
                dictAdd((dict*)o->ptr,ele,NULL);
            } else {
                decrRefCount(ele);
            }
        }
    } else if (rdbtype == RDB_TYPE_ZSET) {
        /* Read list/set value */
        size_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;

        if ((zsetlen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
        o = createZsetObject();
        zs = o->ptr;

        /* Load every single element of the list/set */
        while(zsetlen--) {
            robj *ele;
            double score;
            zskiplistNode *znode;

            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            ele = tryObjectEncoding(ele);
            if (rdbLoadDoubleValue(rdb,&score) == -1) return NULL;

            /* Don't care about integer-encoded strings. */
            if (sdsEncodedObject(ele) && sdslen(ele->ptr) > maxelelen)
                maxelelen = sdslen(ele->ptr);

            znode = zslInsert(zs->zsl,score,ele);
            dictAdd(zs->dict,ele,&znode->score);
            incrRefCount(ele); /* added to skiplist */
        }

        /* Convert *after* loading, since sorted sets are not stored ordered. */
        if (zsetLength(o) <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
                zsetConvert(o,OBJ_ENCODING_ZIPLIST);
    } else if (rdbtype == RDB_TYPE_HASH) {
        size_t len;
        int ret;

        len = rdbLoadLen(rdb, NULL);
        if (len == RDB_LENERR) return NULL;

        o = createHashObject();

        /* Too many entries? Use a hash table. */
        if (len > server.hash_max_ziplist_entries) {
            if (hashTypeConvert(db, key, o, OBJ_ENCODING_HT) == -1) {
                return NULL;
            }
        }

        /* Load every field and value into the ziplist */
        while (o->encoding == OBJ_ENCODING_ZIPLIST && len > 0) {
            robj *field, *value;

            len--;
            /* Load raw strings */
            field = rdbLoadStringObject(rdb);
            if (field == NULL) {
                return NULL;
            }
            serverAssert(sdsEncodedObject(field));
            value = rdbLoadStringObject(rdb);
            if (value == NULL) {
                return NULL;
            }
            serverAssert(sdsEncodedObject(value));

            /* Add pair to ziplist */
            o->ptr = ziplistPush(o->ptr, field->ptr, 
                                 sdslen(field->ptr), ZIPLIST_TAIL);
            o->ptr = ziplistPush(o->ptr, value->ptr, 
                                 sdslen(value->ptr), ZIPLIST_TAIL);
            /* Convert to hash table if size threshold is exceeded */
            if (sdslen(field->ptr) > server.hash_max_ziplist_value ||
                sdslen(value->ptr) > server.hash_max_ziplist_value)
            {
                decrRefCount(field);
                decrRefCount(value);
                if (hashTypeConvert(db, key, o, OBJ_ENCODING_HT) == -1) {
                    return NULL;
                }
                break;
            }
            decrRefCount(field);
            decrRefCount(value);
        }

        /* Load remaining fields and values into the hash table */
        while (o->encoding == OBJ_ENCODING_HT && len > 0) {
            robj *field, *value;

            len--;
            /* Load encoded strings */
            field = rdbLoadEncodedStringObject(rdb);
            if (field == NULL) return NULL;
            value = rdbLoadEncodedStringObject(rdb);
            if (value == NULL) return NULL;

            field = tryObjectEncoding(field);
            value = tryObjectEncoding(value);

            /* Add pair to hash table */
            ret = dictAdd((dict*)o->ptr, field, value);
            if (ret == DICT_ERR) {
                rdbExitReportCorruptRDB("Duplicate keys detected");
            }
        }

        /* All pairs should be read by now */
        serverAssert(len == 0);
    } else if (rdbtype == RDB_TYPE_LIST_QUICKLIST) {
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
        o = createQuicklistObject();
        quicklistSetOptions(o->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);

        while (len--) {
            unsigned char *zl = rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN);
            if (zl == NULL) return NULL;
            quicklistAppendZiplist(o->ptr, zl);
        }
    } else if (rdbtype == RDB_TYPE_HASH_ZIPMAP  ||
               rdbtype == RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == RDB_TYPE_SET_INTSET   ||
               rdbtype == RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == RDB_TYPE_HASH_ZIPLIST)
    {
        unsigned char *encoded = rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN);
        if (encoded == NULL) return NULL;
        o = createObject(OBJ_STRING,encoded); /* Obj type fixed below. */

        /* Fix the object encoding, and make sure to convert the encoded
         * data type into the base type if accordingly to the current
         * configuration there are too many elements in the encoded data
         * type. Note that we only check the length and not max element
         * size as this is an O(N) scan. Eventually everything will get
         * converted. */
        switch(rdbtype) {
            case RDB_TYPE_HASH_ZIPMAP:
                /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
                {
                    unsigned char *zl = ziplistNew();
                    unsigned char *zi = zipmapRewind(o->ptr);
                    unsigned char *fstr, *vstr;
                    unsigned int flen, vlen;
                    unsigned int maxlen = 0;

                    while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                        if (flen > maxlen) maxlen = flen;
                        if (vlen > maxlen) maxlen = vlen;
                        zl = ziplistPush(zl, fstr, flen, ZIPLIST_TAIL);
                        zl = ziplistPush(zl, vstr, vlen, ZIPLIST_TAIL);
                    }

                    zfree(o->ptr);
                    o->ptr = zl;
                    o->type = OBJ_HASH;
                    o->encoding = OBJ_ENCODING_ZIPLIST;

                    if (hashTypeLength(o) > server.hash_max_ziplist_entries ||
                        maxlen > server.hash_max_ziplist_value)
                    {
                        rc = hashTypeConvert(db, key, o, OBJ_ENCODING_HT);
                        if (rc == -1) {
                            rdbExitReportCorruptRDB("hashTypeConvert failed");
                        }
                    }
                }
                break;
            case RDB_TYPE_LIST_ZIPLIST:
                o->type = OBJ_LIST;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                rc = listTypeConvert(o, OBJ_ENCODING_QUICKLIST);
                if (rc == -1) {
                    rdbExitReportCorruptRDB("listTypeConvert failed");
                }
                break;
            case RDB_TYPE_SET_INTSET:
                o->type = OBJ_SET;
                o->encoding = OBJ_ENCODING_INTSET;
                if (intsetLen(o->ptr) > server.set_max_intset_entries)
                    setTypeConvert(o,OBJ_ENCODING_HT);
                break;
            case RDB_TYPE_ZSET_ZIPLIST:
                o->type = OBJ_ZSET;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                if (zsetLength(o) > server.zset_max_ziplist_entries)
                    zsetConvert(o,OBJ_ENCODING_SKIPLIST);
                break;
            case RDB_TYPE_HASH_ZIPLIST:
                o->type = OBJ_HASH;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                if (hashTypeLength(o) > server.hash_max_ziplist_entries) {
                    rc = hashTypeConvert(db, key, o, OBJ_ENCODING_HT);
                    if (rc == -1) {
                        rdbExitReportCorruptRDB("hashTypeConvert failed");
                    }
                }
                break;
            default:
                rdbExitReportCorruptRDB("Unknown RDB encoding type %d",rdbtype);
                break;
        }
    } else {
        rdbExitReportCorruptRDB("Unknown RDB encoding type %d",rdbtype);
    }
    return o;
}

/* Mark that we are loading in the global state and setup the fields
 * needed to provide loading stats. */
void startLoading(FILE *fp) {
    struct stat sb;

    /* Load the DB */
    server.loading = 1;
    server.loading_start_time = time(NULL);
    server.loading_loaded_bytes = 0;
    if (fstat(fileno(fp), &sb) == -1) {
        server.loading_total_bytes = 0;
    } else {
        server.loading_total_bytes = sb.st_size;
    }
}

/* Refresh the loading progress info */
void loadingProgress(off_t pos) {
    server.loading_loaded_bytes = pos;
    if (server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

/* Loading finished */
void stopLoading(void) {
    server.loading = 0;
}

/* Track loading progress in order to serve client's from time to time
   and if needed calculate rdb checksum  */
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len) {
    if (server.rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    if (server.loading_process_events_interval_bytes &&
        (r->processed_bytes + len)/server.loading_process_events_interval_bytes > r->processed_bytes/server.loading_process_events_interval_bytes)
    {
        /* The DB can take some non trivial amount of time to load. Update
         * our cached time since it is used to create and update the last
         * interaction time with clients and for other important things. */
        updateCachedTime();
        if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER)
            replicationSendNewlineToMaster();
        loadingProgress(r->processed_bytes);
        processEventsWhileBlocked();
    }
}

int rdbLoad(char *filename) {
    int rc = C_OK;
    uint32_t dbid;
    int type, rdbver;
    redisDb *db = server.db+0;
    char buf[1024];
    int needdel_realtime = 0;
    long long expiretime, now = mstime();
    FILE *fp;
    rio rdb;
    dictEntry *de = NULL;
    sds addkey = NULL;

    if ((fp = fopen(filename,"r")) == NULL) return C_ERR;

    rioInitWithFile(&rdb,fp);
    rdb.update_cksum = rdbLoadProgressCallback;
    rdb.max_processing_chunk = server.loading_process_events_interval_bytes;
    if (rioRead(&rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        serverLog(LL_WARNING,"Wrong signature trying to load DB from file");
        errno = EINVAL;
        return C_ERR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        fclose(fp);
        serverLog(LL_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return C_ERR;
    }

    startLoading(fp);
    while(1) {
        robj *key, *val;
        expiretime = -1;
        needdel_realtime = 0;

        /* Read type. */
        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;

        /* Handle special types. */
        if (type == RDB_OPCODE_EXPIRETIME) {
            /* EXPIRETIME: load an expire associated with the next key
             * to load. Note that after loading an expire we need to
             * load the actual type, and continue. */
            if ((expiretime = rdbLoadTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliseconds. */
            expiretime *= 1000;
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            /* EXPIRETIME_MS: milliseconds precision expire times introduced
             * with RDB v3. Like EXPIRETIME but no with more precision. */
            if ((expiretime = rdbLoadMillisecondTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        } else if(type == RDB_OPCODE_REALTIME_EXPIRETIME) {
            /* EXPIRETIME: load an expire associated with the next key
             * to load. Note that after loading an expire we need to
             * load the actual type, and continue. */
            if ((expiretime = rdbLoadTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliseconds. */
            expiretime *= 1000;
            needdel_realtime = 1;
        } else if (type == RDB_OPCODE_REALTIME_EXPIRETIME_MS) {
            /* EXPIRETIME_MS: milliseconds precision expire times introduced
             * with RDB v3. Like EXPIRETIME but no with more precision. */
            if ((expiretime = rdbLoadMillisecondTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            needdel_realtime = 1;
        } else if (type == RDB_OPCODE_EOF) {
            /* EOF: End of file, exit the main loop. */
            break;
        } else if (type == RDB_OPCODE_SELECTDB) {
            /* SELECTDB: Select the specified database. */
            if ((dbid = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if (dbid >= (unsigned)server.dbnum) {
                serverLog(LL_WARNING,
                    "FATAL: Data file was created with a Redis "
                    "server configured to handle more than %d "
                    "databases. Exiting\n", server.dbnum);
                exit(1);
            }
            db = server.db+dbid;
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_RESIZEDB) {
            /* RESIZEDB: Hint about the size of the keys in the currently
             * selected data base, in order to avoid useless rehashing. */
            uint32_t db_size, expires_size;
            if ((db_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            dictExpand(db->dict,db_size);
            dictExpand(db->expires,expires_size);
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_AUX) {
            /* AUX: generic string-string fields. Use to add state to RDB
             * which is backward compatible. Implementations of RDB loading
             * are requierd to skip AUX fields they don't understand.
             *
             * An AUX field is composed of two strings: key and value. */
            robj *auxkey, *auxval;
            if ((auxkey = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
            if ((auxval = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;

            if (((char*)auxkey->ptr)[0] == '%') {
                /* All the fields with a name staring with '%' are considered
                 * information fields and are logged at startup with a log
                 * level of NOTICE. */
                serverLog(LL_NOTICE,"RDB '%s': %s",
                    (char*)auxkey->ptr,
                    (char*)auxval->ptr);
            } else {
                /* We ignore fields we don't understand, as by AUX field
                 * contract. */
                serverLog(LL_DEBUG,"Unrecognized RDB AUX field: '%s'",
                    (char*)auxkey->ptr);
            }

            decrRefCount(auxkey);
            decrRefCount(auxval);
            continue; /* Read type again. */
        }

        /* Read key */
        if ((key = rdbLoadStringObject(&rdb)) == NULL) {
            goto eoferr;
        }

        addkey = sdsdup(key->ptr);
        de = dictAddRaw(db->dict, addkey);
        if (!de) {
            sdsfree(addkey);
            decrRefCount(key);
            goto eoferr;
        }
        
        /* Read value */
        if ((val = rdbLoadObject(db, key->ptr, type, &rdb)) == NULL) {             
            dictDelete(db->dict, key->ptr);
            decrRefCount(key);
            goto eoferr;
        }
        /* Check if the key already expired. This function is used when loading
         * an RDB file from disk, either at startup, or when an RDB was
         * received from the master. In the latter case, the master is
         * responsible for key expiry. If we would expire keys here, the
         * snapshot taken by the master may not be reflected on the slave. */
        if (server.masterhost == NULL && expiretime != -1 && expiretime < now) {
            dictDelete(db->dict, key->ptr);
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }

        dictSetVal(db->dict, de, val);
        dictSetEntryValType(de, val->type);
        dictSetEntryValNotOnDisk(de); 
   
        if (val && val->type == OBJ_LIST) {
            signalListAsReady(db, key);
        }
        
        if (server.cluster_enabled) {
            slotToKeyAdd(key);
        }   
              
        if (useDiskStore() 
            && needSaveObjectOnDisk(DISK_STORE_FAST)
            && (!dictIsEntryValOnDisk(de))) {                  
            rc = saveObjectOnDiskLimit(db, de, 0);
            if (rc != C_OK) {
                serverLog(LL_WARNING, 
                          "load rdb call saveObjectOnDiskLimit failed");
                goto eoferr;
            }
        }

        /* Set the expire time if needed */
        if (expiretime != -1) {
            if (needdel_realtime != 0) {
                setRealtimeExpireFlag(key);
            }
            
            setExpire(db, key, expiretime);
        }

        decrRefCount(key);
    }
    /* Verify the checksum if RDB version is >= 5 */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;

        if (rioRead(&rdb,&cksum,8) == 0) goto eoferr;
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            serverLog(LL_WARNING,"RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            serverLog(LL_WARNING,"Wrong RDB checksum. Aborting now.");
            rdbExitReportCorruptRDB("RDB CRC error");
        }
    }

    fclose(fp);
    stopLoading();
    return C_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    serverLog(LL_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
    rdbExitReportCorruptRDB("Unexpected EOF reading RDB file");
    return C_ERR; /* Just to avoid warning */
}

/* A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of actual BGSAVEs. */
void backgroundSaveDoneHandlerDisk(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.lastbgsave_status = C_OK;
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background saving error");
        server.lastbgsave_status = C_ERR;
    } else {
        mstime_t latency;

        serverLog(LL_WARNING,
            "Background saving terminated by signal %d", bysignal);
        latencyStartMonitor(latency);
        rdbRemoveTempFile(server.rdb_child_pid);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("rdb-unlink-temp-file",latency);
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * tirggering an error conditon. */
        if (bysignal != SIGUSR1)
            server.lastbgsave_status = C_ERR;
    }
    server.rdb_child_pid = -1;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_last = time(NULL)-server.rdb_save_time_start;
    server.rdb_save_time_start = -1;
    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, RDB_CHILD_TYPE_DISK);
}

/* A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of RDB -> Salves socket transfers for
 * diskless replication. */
void backgroundSaveDoneHandlerSocket(int exitcode, int bysignal) {
    uint64_t *ok_slaves;

    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background RDB transfer terminated with success");
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background transfer error");
    } else {
        serverLog(LL_WARNING,
            "Background transfer terminated by signal %d", bysignal);
    }
    server.rdb_child_pid = -1;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_start = -1;

    /* If the child returns an OK exit code, read the set of slave client
     * IDs and the associated status code. We'll terminate all the slaves
     * in error state.
     *
     * If the process returned an error, consider the list of slaves that
     * can continue to be emtpy, so that it's just a special case of the
     * normal code path. */
    ok_slaves = zmalloc(sizeof(uint64_t)); /* Make space for the count. */
    ok_slaves[0] = 0;
    if (!bysignal && exitcode == 0) {
        int readlen = sizeof(uint64_t);

        if (read(server.rdb_pipe_read_result_from_child, ok_slaves, readlen) ==
                 readlen)
        {
            readlen = ok_slaves[0]*sizeof(uint64_t)*2;

            /* Make space for enough elements as specified by the first
             * uint64_t element in the array. */
            ok_slaves = zrealloc(ok_slaves,sizeof(uint64_t)+readlen);
            if (readlen &&
                read(server.rdb_pipe_read_result_from_child, ok_slaves+1,
                     readlen) != readlen)
            {
                ok_slaves[0] = 0;
            }
        }
    }

    close(server.rdb_pipe_read_result_from_child);
    close(server.rdb_pipe_write_result_to_parent);

    /* We can continue the replication process with all the slaves that
     * correctly received the full payload. Others are terminated. */
    listNode *ln;
    listIter li;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            uint64_t j;
            int errorcode = 0;

            /* Search for the slave ID in the reply. In order for a slave to
             * continue the replication process, we need to find it in the list,
             * and it must have an error code set to 0 (which means success). */
            for (j = 0; j < ok_slaves[0]; j++) {
                if (slave->id == ok_slaves[2*j+1]) {
                    errorcode = ok_slaves[2*j+2];
                    break; /* Found in slaves list. */
                }
            }
            if (j == ok_slaves[0] || errorcode != 0) {
                serverLog(LL_WARNING,
                "Closing slave %s: child->slave RDB transfer failed: %s",
                    replicationGetSlaveName(slave),
                    (errorcode == 0) ? "RDB transfer child aborted"
                                     : strerror(errorcode));
                freeClient(slave);
            } else {
                serverLog(LL_WARNING,
                "Slave %s correctly received the streamed RDB file.",
                    replicationGetSlaveName(slave));
                /* Restore the socket as non-blocking. */
                anetNonBlock(NULL,slave->fd);
                anetSendTimeout(NULL,slave->fd,0);
            }
        }
    }
    zfree(ok_slaves);

    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, RDB_CHILD_TYPE_SOCKET);
}

/* When a background RDB saving/transfer terminates, call the right handler. */
void backgroundSaveDoneHandler(int exitcode, int bysignal) {
    switch(server.rdb_child_type) {
    case RDB_CHILD_TYPE_DISK:
        backgroundSaveDoneHandlerDisk(exitcode,bysignal);
        break;
    case RDB_CHILD_TYPE_SOCKET:
        backgroundSaveDoneHandlerSocket(exitcode,bysignal);
        break;
    default:
        serverPanic("Unknown RDB child type.");
        break;
    }
}

/* Spawn an RDB child that writes the RDB to the sockets of the slaves
 * that are currently in SLAVE_STATE_WAIT_BGSAVE_START state. */
int rdbSaveToSlavesSockets(void) {
    int *fds;
    uint64_t *clientids;
    int numfds;
    listNode *ln;
    listIter li;
    pid_t childpid;
    long long start;
    int pipefds[2];

    if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) return C_ERR;

    /* Before to fork, create a pipe that will be used in order to
     * send back to the parent the IDs of the slaves that successfully
     * received all the writes. */
    if (pipe(pipefds) == -1) return C_ERR;
    server.rdb_pipe_read_result_from_child = pipefds[0];
    server.rdb_pipe_write_result_to_parent = pipefds[1];

    /* Collect the file descriptors of the slaves we want to transfer
     * the RDB to, which are i WAIT_BGSAVE_START state. */
    fds = zmalloc(sizeof(int)*listLength(server.slaves));
    /* We also allocate an array of corresponding client IDs. This will
     * be useful for the child process in order to build the report
     * (sent via unix pipe) that will be sent to the parent. */
    clientids = zmalloc(sizeof(uint64_t)*listLength(server.slaves));
    numfds = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
            clientids[numfds] = slave->id;
            fds[numfds++] = slave->fd;
            replicationSetupSlaveForFullResync(slave,getPsyncInitialOffset());
            /* Put the socket in blocking mode to simplify RDB transfer.
             * We'll restore it when the children returns (since duped socket
             * will share the O_NONBLOCK attribute with the parent). */
            anetBlock(NULL,slave->fd);
            anetSendTimeout(NULL,slave->fd,server.repl_timeout*1000);
        }
    }

    /* Create the child process. */
    start = ustime();
    if ((childpid = fork()) == 0) {
        /* Child */
        int retval;
        rio slave_sockets;

        rioInitWithFdset(&slave_sockets,fds,numfds);
        zfree(fds);

        closeListeningSockets(0);
        redisSetProcTitle("redis-rdb-to-slaves");

        retval = rdbSaveRioWithEOFMark(&slave_sockets,NULL);
        if (retval == C_OK && rioFlush(&slave_sockets) == 0)
            retval = C_ERR;

        if (retval == C_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                serverLog(LL_NOTICE,
                    "RDB: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }

            /* If we are returning OK, at least one slave was served
             * with the RDB file as expected, so we need to send a report
             * to the parent via the pipe. The format of the message is:
             *
             * <len> <slave[0].id> <slave[0].error> ...
             *
             * len, slave IDs, and slave errors, are all uint64_t integers,
             * so basically the reply is composed of 64 bits for the len field
             * plus 2 additional 64 bit integers for each entry, for a total
             * of 'len' entries.
             *
             * The 'id' represents the slave's client ID, so that the master
             * can match the report with a specific slave, and 'error' is
             * set to 0 if the replication process terminated with a success
             * or the error code if an error occurred. */
            void *msg = zmalloc(sizeof(uint64_t)*(1+2*numfds));
            uint64_t *len = msg;
            uint64_t *ids = len+1;
            int j, msglen;

            *len = numfds;
            for (j = 0; j < numfds; j++) {
                *ids++ = clientids[j];
                *ids++ = slave_sockets.io.fdset.state[j];
            }

            /* Write the message to the parent. If we have no good slaves or
             * we are unable to transfer the message to the parent, we exit
             * with an error so that the parent will abort the replication
             * process with all the childre that were waiting. */
            msglen = sizeof(uint64_t)*(1+2*numfds);
            if (*len == 0 ||
                write(server.rdb_pipe_write_result_to_parent,msg,msglen)
                != msglen)
            {
                retval = C_ERR;
            }
            zfree(msg);
        }
        zfree(clientids);
        rioFreeFdset(&slave_sockets);
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent */
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        if (childpid == -1) {
            serverLog(LL_WARNING,"Can't save in background: fork: %s",
                strerror(errno));

            /* Undo the state change. The caller will perform cleanup on
             * all the slaves in BGSAVE_START state, but an early call to
             * replicationSetupSlaveForFullResync() turned it into BGSAVE_END */
            listRewind(server.slaves,&li);
            while((ln = listNext(&li))) {
                client *slave = ln->value;
                int j;

                for (j = 0; j < numfds; j++) {
                    if (slave->id == clientids[j]) {
                        slave->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
                        break;
                    }
                }
            }
            close(pipefds[0]);
            close(pipefds[1]);
        } else {
            serverLog(LL_NOTICE,"Background RDB transfer started by pid %d",
                childpid);
            server.rdb_save_time_start = time(NULL);
            server.rdb_child_pid = childpid;
            server.rdb_child_type = RDB_CHILD_TYPE_SOCKET;
            updateDictResizePolicy();
        }
        zfree(clientids);
        zfree(fds);
        return (childpid == -1) ? C_ERR : C_OK;
    }
    return C_OK; /* Unreached. */
}

void saveCommand(client *c) {
    if (server.rdb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");
        return;
    }
    if (rdbSave(server.rdb_filename) == C_OK) {
        addReply(c,shared.ok);
    } else {
        addReply(c,shared.err);
    }
}

/* BGSAVE [SCHEDULE] */
void bgsaveCommand(client *c) {
    int schedule = 0;

    /* The SCHEDULE option changes the behavior of BGSAVE when an AOF rewrite
     * is in progress. Instead of returning an error a BGSAVE gets scheduled. */
    if (c->argc > 1) {
        if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"schedule")) {
            schedule = 1;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    if (server.rdb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");
    } else if (server.aof_child_pid != -1) {
        if (schedule) {
            server.rdb_bgsave_scheduled = 1;
            addReplyStatus(c,"Background saving scheduled");
        } else {
            addReplyError(c,
                "An AOF log rewriting in progress: can't BGSAVE right now. "
                "Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenver "
                "possible.");
        }
    } else if (rdbSaveBackground(server.rdb_filename) == C_OK) {
        addReplyStatus(c,"Background saving started");
    } else {
        addReply(c,shared.err);
    }
}
