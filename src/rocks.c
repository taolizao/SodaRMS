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

rocksdb_context_t g_rocksdb_context;

rocksdb_context_t *get_rocksdb_context(void) {
    return &g_rocksdb_context;
}

#if 0
static void rocksdbCmpDestroy(void* arg) { }

static int rocksdbCmpCompare(void* arg, const char* a, size_t alen,
                      const char* b, size_t blen) {
  size_t n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  if (r == 0) {
    if (alen < blen) r = -1;
    else if (alen > blen) r = +1;
  }
  return r;
}

static const char* rocksdbCmpName(void* arg) {
  return "foo";
}
#endif

void displayRocksdbStoreOptions(rocksdbStoreOptions *dboptions)
{
    serverLog(LL_WARNING, 
            "# Server\r\n"
            "num-levels:%d\r\n"
            "write-buffer-size:%ld\r\n"
            "write-buffer-number:%d\r\n"
            "min-write-buffer-number-to-merge:%d\r\n"
            "target-file-size-base:%ld\r\n"
            "target-file-size-multiplier:%d\r\n"
            "level0-file-number-compaction-trigger:%d\r\n"
            "level0-slowdown-write-trigger:%d\r\n"
            "level0-stop-write-trigger:%d\r\n"
            "max-bytes-for-level-base:%ld\r\n"
            "max-bytes-for-level-multiplier:%lld\r\n"
            "max-open-files:%u\r\n"
            "max-background-jobs:%u\r\n"
            "max-manifest-file-size:%lu\r\n"
            "max-compaction-readahead-size:%lu\r\n"
            "max-block-size:%lu\r\n"
            "max-block-cache-size:%lu\r\n"
            "optimize-filters-for-hits:%d\r\n"
            "cache-index-and-filter-blocks:%d\r\n"
            "pin-10-filter-and-index-blocks-in-cache:%d\r\n",
            dboptions->db_num_levels,
            dboptions->db_write_buffer_size,
            dboptions->db_max_write_buffer_nr,
            dboptions->db_min_write_buffer_nr_to_merge,
            dboptions->db_target_file_size_base,
            dboptions->db_target_file_size_multiplier,
            dboptions->db_level0_filenr_compaction_trigger,
            dboptions->db_level0_slowdown_write_trigger,
            dboptions->db_level0_stop_write_trigger,
            dboptions->db_maxbyte_for_level_base,
            dboptions->db_maxbyte_for_level_multiplier,
            dboptions->db_max_open_files,
            dboptions->db_max_background_jobs,
            dboptions->db_max_manifest_file_size,
            dboptions->db_compaction_readahead_size,
            dboptions->db_block_size,
            dboptions->db_block_cache_size,
            dboptions->db_optimize_filters_for_hits,
            dboptions->db_cache_index_and_filter_blocks,
            dboptions->db_pin_10_filter_and_index_blocks_in_cache);
}

int init_rocksdb_context(char *dbpath, 
                         char *backuppath, 
                         rocksdbStoreOptions *dboptions)
{
    int rc = C_OK;
    long cpus = 0;
    char *err = NULL;   
    //rocksdb_comparator_t *cmp = NULL;
    rocksdb_env_t *env = NULL;
    rocksdb_filterpolicy_t *pfilterpolicy = NULL;
    rocksdb_ratelimiter_t *pratelimiter = NULL;
    rocksdb_context_t *procksdbctx = get_rocksdb_context();
    int compression_levels[7] = {rocksdb_no_compression, rocksdb_no_compression,
                                rocksdb_no_compression, rocksdb_no_compression,
                                rocksdb_no_compression, rocksdb_no_compression,
                                rocksdb_no_compression};
    //int compression_levels[7] = {rocksdb_no_compression, rocksdb_no_compression,
    //                        rocksdb_snappy_compression, rocksdb_snappy_compression,
    //                        rocksdb_snappy_compression, rocksdb_snappy_compression,
    //                        rocksdb_snappy_compression};                            

    displayRocksdbStoreOptions(dboptions);
    
    memset(procksdbctx, 0, sizeof(*procksdbctx));
    procksdbctx->options = rocksdb_options_create();

//    cmp = rocksdb_comparator_create(NULL, rocksdbCmpDestroy, 
//                                  rocksdbCmpCompare, rocksdbCmpName);
//    rocksdb_options_set_comparator(procksdbctx->options, cmp);                              
    env = rocksdb_create_default_env();
    rocksdb_options_set_env(procksdbctx->options, env);
    
    rocksdb_options_set_error_if_exists(procksdbctx->options, 1);    
    rocksdb_options_set_paranoid_checks(procksdbctx->options, 1);
    // create the DB if it's not already present
    rocksdb_options_set_create_if_missing(procksdbctx->options, 1);
    

    // get # of online cores
    cpus = sysconf(_SC_NPROCESSORS_ONLN);      
    rocksdb_options_increase_parallelism(procksdbctx->options, (int)(cpus));

    // comactions
    rocksdb_options_set_base_background_compactions(procksdbctx->options, 1);
    rocksdb_options_optimize_level_style_compaction(procksdbctx->options, 0);

    // block options
    procksdbctx->block_options = rocksdb_block_based_options_create(); 

    rocksdb_block_based_options_set_block_size(procksdbctx->block_options, 
                                    dboptions->db_block_size);
    rocksdb_block_based_options_set_index_type(procksdbctx->block_options,
                   rocksdb_block_based_table_index_type_two_level_index_search);
    rocksdb_block_based_options_set_partition_filters(
                                    procksdbctx->block_options, 1);
    rocksdb_block_based_options_set_metadata_block_size(
              procksdbctx->block_options, ROCKSDB_METADATA_BLOCK_SIZE_DEF);                                
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(
              procksdbctx->block_options, 
              dboptions->db_cache_index_and_filter_blocks);
    rocksdb_block_based_options_set_cache_index_and_filter_blocks_hign_pri(
              procksdbctx->block_options, 1);
    rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(
              procksdbctx->block_options, 
              dboptions->db_pin_10_filter_and_index_blocks_in_cache);                                            

    // filter policy
    pfilterpolicy = rocksdb_filterpolicy_create_bloom_full(
                                                ROCKSDB_BLOOM_BITS_PER_KEY);
    if (!pfilterpolicy) {
        serverLog(LL_WARNING, "rocksdb open backuppath(%s) failed:%s\n", 
                  backuppath, err);
        rocksdb_options_destroy(procksdbctx->options);        
        return C_ERR;
    }
    
    rocksdb_block_based_options_set_filter_policy(procksdbctx->block_options, 
                                    pfilterpolicy);                          
    
    
    procksdbctx->cache = rocksdb_cache_create_lru(
                                    dboptions->db_block_cache_size);  
    rocksdb_block_based_options_set_block_cache(
                    procksdbctx->block_options, procksdbctx->cache); 
    rocksdb_options_set_block_based_table_factory(procksdbctx->options, 
                                    procksdbctx->block_options);

    // memory usage
    // not use bloom filter at the last level
    rocksdb_options_set_optimize_filters_for_hits(procksdbctx->options, 0);   
    
    // compression
    rocksdb_options_set_compression(procksdbctx->options, 
                                    rocksdb_no_compression);
    rocksdb_options_set_compression_options(procksdbctx->options, 
                                    -14, -1, 0, 0);
    rocksdb_options_set_compression_per_level(procksdbctx->options, 
                                    compression_levels, 7);

    // cf configuration
    rocksdb_options_set_num_levels(procksdbctx->options, 
                                    dboptions->db_num_levels);
    rocksdb_options_set_write_buffer_size(procksdbctx->options, 
                                    dboptions->db_write_buffer_size); 
    rocksdb_options_set_max_write_buffer_number(procksdbctx->options, 
                                    dboptions->db_max_write_buffer_nr);   
    rocksdb_options_set_min_write_buffer_number_to_merge(procksdbctx->options, 
                                    dboptions->db_min_write_buffer_nr_to_merge);
    rocksdb_options_set_target_file_size_base(procksdbctx->options, 
                                    dboptions->db_target_file_size_base);
    rocksdb_options_set_target_file_size_multiplier(procksdbctx->options, 
                                    dboptions->db_target_file_size_multiplier);
    rocksdb_options_set_level0_file_num_compaction_trigger(procksdbctx->options, 
                               dboptions->db_level0_filenr_compaction_trigger); 
    rocksdb_options_set_level0_slowdown_writes_trigger(procksdbctx->options, 
                               dboptions->db_level0_slowdown_write_trigger);
    rocksdb_options_set_level0_stop_writes_trigger(procksdbctx->options, 
                               dboptions->db_level0_stop_write_trigger);
    rocksdb_options_set_max_bytes_for_level_base(procksdbctx->options, 
                               dboptions->db_maxbyte_for_level_base);  
    rocksdb_options_set_max_bytes_for_level_multiplier(procksdbctx->options, 
                               dboptions->db_maxbyte_for_level_multiplier); 
    rocksdb_options_set_max_open_files(procksdbctx->options, 
                               dboptions->db_max_open_files);
    rocksdb_options_set_max_background_compactions(procksdbctx->options, 
                               dboptions->db_max_background_jobs);
    rocksdb_options_set_base_background_compactions(procksdbctx->options, 1);
    rocksdb_options_set_max_background_flushes(procksdbctx->options, 
                               dboptions->db_max_background_jobs);
    rocksdb_options_set_max_manifest_file_size(procksdbctx->options, 
                               dboptions->db_max_manifest_file_size);  
    rocksdb_options_compaction_readahead_size(procksdbctx->options, 
                               dboptions->db_compaction_readahead_size); 
                            
    // IO control
    pratelimiter = rocksdb_ratelimiter_create(1000 * 1024 * 1024, 
                                              100 * 1000, 10);                           
    rocksdb_options_set_ratelimiter(procksdbctx->options, pratelimiter);       
    rocksdb_options_set_level_compaction_dynamic_level_bytes(
                               procksdbctx->options, 1);    
    
    // clear rocks db data
    rc = delete_dir(dbpath);
    if (rc < 0) {
        serverLog(LL_WARNING, "delete dir(%s) failed", dbpath);
    }
    rc = delete_dir(backuppath);
    if (rc < 0) {
        serverLog(LL_WARNING, "delete dir(%s) failed", backuppath);
    }

    rc = create_multilevel_dir(dbpath);
    if (rc < 0) {
        serverLog(LL_WARNING, "create dir(%s) failed", dbpath);
        rocksdb_ratelimiter_destroy(pratelimiter);          
        rocksdb_cache_destroy(procksdbctx->cache);
        rocksdb_block_based_options_destroy(procksdbctx->block_options);          
        rocksdb_options_destroy(procksdbctx->options);
        return C_ERR;
    }
    rc = create_multilevel_dir(backuppath);
    if (rc < 0) {
        serverLog(LL_WARNING, "create dir(%s) failed", backuppath);
        rocksdb_ratelimiter_destroy(pratelimiter);          
        rocksdb_cache_destroy(procksdbctx->cache);
        rocksdb_block_based_options_destroy(procksdbctx->block_options);          
        rocksdb_options_destroy(procksdbctx->options);
        return C_ERR;
    }
    
    // open DB    
    procksdbctx->db = rocksdb_open(procksdbctx->options, dbpath, &err);
    if ((!procksdbctx->db) || err) {
        serverLog(LL_WARNING, "rocksdb open datapath(%s) failed:%s\n", 
                  dbpath, err);
        rocksdb_ratelimiter_destroy(pratelimiter);          
        rocksdb_cache_destroy(procksdbctx->cache);
        rocksdb_block_based_options_destroy(procksdbctx->block_options);          
        rocksdb_options_destroy(procksdbctx->options);
        return C_ERR;
    }

    // open Backup Engine that we will use for backing up our database
    procksdbctx->backupengine = rocksdb_backup_engine_open(procksdbctx->options, 
                                                           backuppath, &err);
    if ((!procksdbctx->backupengine) || err) {
        serverLog(LL_WARNING, "rocksdb open backuppath(%s) failed:%s\n", 
                  backuppath, err);
        rocksdb_close(procksdbctx->db);
        rocksdb_ratelimiter_destroy(pratelimiter);     
        rocksdb_cache_destroy(procksdbctx->cache);
        rocksdb_block_based_options_destroy(procksdbctx->block_options);   
        rocksdb_options_destroy(procksdbctx->options);        
        return C_ERR;
    }    

    procksdbctx->writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(procksdbctx->writeoptions, 1);  
    rocksdb_writeoptions_set_sync(procksdbctx->writeoptions, 0);
    
    procksdbctx->readoptions = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(procksdbctx->readoptions, 1);
    rocksdb_readoptions_set_fill_cache(procksdbctx->readoptions, 1);
    
    procksdbctx->restore_options = rocksdb_restore_options_create();
       
    return C_OK;
}

int32_t write_to_rocksdb(char *key,  size_t keylen, char *value,  size_t vallen)
{
    char *err = NULL;
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    //serverLog(LL_WARNING, "write Key(%s) to rocksdb", key);
    rocksdb_put(procksdbctx->db, procksdbctx->writeoptions, 
                       key, keylen, value, vallen, &err);
    if (err) {
        serverLog(LL_WARNING, "rocksdb write Key(%s) -> Val(%s)  failed:%s\n",
                        key, value, err);
        return C_ERR;
    }

    return C_OK;
}

/* when call this function, remember to free returned 'value' after used */
int get_from_rocksdb(char *key, size_t keylen, char **value, size_t *pvallen)
{
    char *err = NULL;
    char *returned_value = NULL;
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    //serverLog(LL_WARNING, "get Key(%s) from rocksdb", key);
    
    returned_value = rocksdb_get(procksdbctx->db, procksdbctx->readoptions, 
                                               key, keylen, pvallen, &err);
    if (err || (!returned_value) || (!pvallen)) {
        serverLog(LL_WARNING, "rocksdb read Key(%s) failed for err=%d or "
                  "returnval:%s value or value_length:%d", 
                  key, err, returned_value ? returned_value : "nil", 
                  pvallen ? *pvallen : 0);
        return C_ERR;
    }

    *value = returned_value;
    
    return C_OK;
}

int del_from_rocksdb(char *key, size_t keylen)
{
    char *err = NULL;
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    //serverLog(LL_WARNING, "del Key(%s) from rocksdb", key);
    
    rocksdb_delete(procksdbctx->db, procksdbctx->writeoptions, key, keylen,  &err);
    if (err) {
        serverLog(LL_WARNING, "rocksdb delete Key(%s) failed:%s\n", key, err);
        return C_ERR;
    }
    
    return C_OK;
}

void real_release_rocksdb_snapshot(void)
{
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    if (procksdbctx->snapshot) {
        rocksdb_release_snapshot(procksdbctx->db, procksdbctx->snapshot); 
        procksdbctx->snapshot = NULL;
    }
}

int create_rocksdb_snapshot(void)
{
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    real_release_rocksdb_snapshot();

    procksdbctx->snapshot = 
                (rocksdb_snapshot_t *)rocksdb_create_snapshot(procksdbctx->db);
    if (!procksdbctx->snapshot) {
        serverLog(LL_WARNING, "create rocksdb snapshot failed");
        return C_ERR;
    }
    
    rocksdb_readoptions_set_snapshot(procksdbctx->readoptions, 
                                     procksdbctx->snapshot);

    return C_OK;                                 
}

void release_rocksdb_snapshot(int fakerelease)
{
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    rocksdb_readoptions_set_snapshot(procksdbctx->readoptions, NULL);
    if (!fakerelease) {
        real_release_rocksdb_snapshot();
    }                                     
}

int backup_rocksdb(void)
{
    char *err = NULL;
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    rocksdb_backup_engine_create_new_backup(procksdbctx->backupengine, 
                                    procksdbctx->db, &err);
    if (err) {
        serverLog(LL_WARNING, "rocksdb backup failed:%s\n", err);
        return C_ERR;
    }

    return C_OK;
}

int restore_rocksdb(char *dbpath)
{
    char *err = NULL;
    rocksdb_context_t *procksdbctx = get_rocksdb_context();
    
    rocksdb_close(procksdbctx->db);    
    
    rocksdb_backup_engine_restore_db_from_latest_backup(procksdbctx->backupengine,
                    dbpath, dbpath, procksdbctx->restore_options, &err);
    if (err) {
        serverLog(LL_WARNING, "rocksdb restore failed:%s\n", err);
        return C_ERR;
    }

    procksdbctx->db = rocksdb_open(procksdbctx->options, dbpath, &err);
    if ((!procksdbctx->db) || err) {
        serverLog(LL_WARNING, "rocksdb reopen datapath(%s) failed:%s\n", dbpath, err);
        return C_ERR;
    }

    return C_OK;
}

void release_rocksdb_context(void)
{
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    rocksdb_restore_options_destroy(procksdbctx->restore_options);
    rocksdb_writeoptions_destroy(procksdbctx->writeoptions);
    rocksdb_readoptions_destroy(procksdbctx->readoptions);    
    rocksdb_backup_engine_close(procksdbctx->backupengine);
    rocksdb_close(procksdbctx->db);
    rocksdb_options_destroy(procksdbctx->options);
}

size_t rocksMemBlockCacheUsage(void) 
{
    rocksdb_context_t *procksdbctx = get_rocksdb_context();
    
    return rocksdb_cache_get_usage(procksdbctx->cache);
}

size_t rocksMemIteratorPinUsage(void)
{
    rocksdb_context_t *procksdbctx = get_rocksdb_context();

    return rocksdb_cache_get_pinned_usage(procksdbctx->cache);
}

char *rocksMemMemtableUsage(void)
{
    rocksdb_context_t *procksdbctx = get_rocksdb_context();
    
    return rocksdb_property_value(procksdbctx->db, 
                                  "rocksdb.cur-size-all-mem-tables");
}

char *rocksMemIndexFilterUsage(void)
{
    rocksdb_context_t *procksdbctx = get_rocksdb_context();
    
    return rocksdb_property_value(procksdbctx->db, 
                                  "rocksdb.estimate-table-readers-mem");
}

