/* ROCKSDBLib 2.0 -- A C rocksdb library
 *
 * Copyright (c) 2006-2015, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2015, Oran Agra
 * Copyright (c) 2015, Redis Labs, Inc
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

#ifndef BDRP_SODARMS_ROCKS_H
#define BDRP_SODARMS_ROCKS_H

#include "rocksdb/c.h"
#include "server.h"
#include "quicklist.h"

enum {
    ROCKS_SAVE_STRING_TYPE = 1,
    ROCKS_NOT_SAVE_STRING_TYPE = 0,
};

typedef struct rocksdb_context {
    rocksdb_t *db;                          /* rocksdb handle */
    rocksdb_snapshot_t *snapshot;           /* snapshot of db */
    rocksdb_backup_engine_t *backupengine;  /* rocksdb backup engine handle */
    rocksdb_cache_t *cache;
    rocksdb_options_t *options;             /* rocksdb normal options */
    rocksdb_readoptions_t *readoptions;     /* rocksdb read options */
    rocksdb_writeoptions_t *writeoptions;   /* rocksdb write options*/
    rocksdb_restore_options_t *restore_options; /* rocksdb restore options */
    rocksdb_block_based_table_options_t *block_options; /* recksdb block options */
} rocksdb_context_t;

// structure for buffer accessing
typedef struct accbuf {
    char *val_buf;         // buffer to storage value content
    char *val_pos;         // access position of 'val_buf'
    size_t val_size;       // size of 'val_buf'
    size_t val_size_left;  // size from 'val_pos' to end
} accbuf_t;

rocksdb_context_t *get_rocksdb_context(void);
int init_rocksdb_context(char *dbpath, 
                         char *backuppath, 
                         rocksdbStoreOptions *dboptions);
int32_t write_to_rocksdb(char *key, size_t keylen, char *value, size_t vallen);
int get_from_rocksdb(char *key, size_t keylen, char **value, size_t *pvallen);
int del_from_rocksdb(char *key, size_t keylen);
int backup_rocksdb(void);
int restore_rocksdb(char *dbpath);
void release_rocksdb_context(void);

void saveDataOnDiskCycle(int flag);
robj *loadValObjectFromDisk(redisDb *db, 
                            unsigned long long desno, 
                            sds key, 
                            uint32_t type);
robj *loadValObjectFromDiskWithSds(redisDb *db, 
                                   unsigned long long desno,
                                   sds key, 
                                   uint32_t type, 
                                   sds *diskkey);
int loadObjectFromDisk(redisDb *db, dictEntry *de);
int loadHashFieldValueFromDisk(redisDb *db, 
                               unsigned long long desno,
                               sds hkey, 
                               unsigned long long fdesno,
                               robj *field, 
                               robj **fval);
int loadHashFieldValueFromDiskWithSds(redisDb *db,
                                      unsigned long long desno,
                                      sds hkey, 
                                      unsigned long long fdesno,
                                      robj *field, 
                                      robj **fval,
                                      sds *diskkey);                               

size_t rocksMemBlockCacheUsage(void);
size_t rocksMemIteratorPinUsage(void);
char *rocksMemMemtableUsage(void);
char *rocksMemIndexFilterUsage(void);

int getValTypeByEntry(dictEntry *de);
int dictValNeedLoadIntoMemory(dictEntry *de);
int rocksRemoveKey(redisDb *db, 
                   unsigned long long desno, 
                   sds key, 
                   unsigned type);

int saveStringObjectOnDisk(redisDb *db, 
                           unsigned long long desno, 
                           sds key, 
                           robj *val);
unsigned char *loadQuicklistZl(quicklistNode *node);
int loadListQuicklistNodeFromDisk(quicklistNode *node);
int quicklistTryLoadZiplist(quicklistNode *node);
int saveListObjectOnDisk(redisDb *db, 
                         unsigned long long desno,
                         sds key, 
                         robj *val, 
                         int withlimit);

int dictFilterSelectedDe(dictEntry *de);
void updQuicklistNodeVal(quicklistNode *node, unsigned char *zl);
int saveZsetObjectOnDisk(redisDb *db, 
                         unsigned long long desno,
                         sds key, 
                         robj *val);
int saveSetObjectOnDisk(redisDb *db, 
                        unsigned long long desno,
                        sds key, 
                        robj *val);
int saveHashObjectOnDisk(redisDb *db, 
                         unsigned long long desno, 
                         sds key, 
                         robj *val, 
                         int withlimit);
int rocksGenStringObjectVal(robj *val, sds *psaveval, int savetype);

void freeObjectOnDisk(redisDb *db, dictEntry *de);
int create_rocksdb_snapshot(void);
void real_release_rocksdb_snapshot(void);
void release_rocksdb_snapshot(int fakerelease);

void dictFreeEntry(void *db, 
                  const void *key, 
                  dict *dt,
                  dictEntry *de); 
void freeHashFieldVal(redisDb *db, 
                    unsigned long long desno,
                    sds key,
                    unsigned char type,
                    dict *d,                    
                    dictEntry *de);                 

void rocksFree(void *ptr);
int saveObjectOnDiskLimit(redisDb *db, dictEntry *de, int limit);
int rocksNeedExchangeKey(sds key);

#endif   /*end of BDRP_SODARMS_ROCKS_H*/

