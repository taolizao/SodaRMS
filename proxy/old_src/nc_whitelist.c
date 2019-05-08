/********************
@create date: 2013-12-16 00:47
@modify date: 2014-03-17 20:11
@author: taolizao01@gmail.com
@brief: Whitelist function->just 4 protection && the anthorization of  read/write permission
********************/

#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <libgen.h>
#include <sys/time.h>

#include "nc_core.h"
#include "nc_whitelist.h"

/* This tag will decide that which whitelist 
 * buffer should the pointer point to
 * 0: point to the buffer of whitelist
 * 1: point to the buffer of whitelist_switch */
proxy_atomic_t wl_num_tag = ATOMIC_INIT( (1 << (sizeof(uint32_t) * 8 - WL_TAG_BITS)) | 0 );

time_t ip_last_modification = 0;       /* the last change of timestamp of ip whitelist*/
time_t bns_last_modification = 0;      /* the last change of timestamp of bns whitelist*/
struct proxyWhitelist proxy_w_list;    /* connect whitelist */
struct timeval old,new;                /* the absolute value of "new - old" is larger than BNS_PERIOD or not */
int force_to_read_whitelist = 0;       /* force to read ip whitelist */
int force_to_read_bns = 0;             /* force to read bns whitelist */

static rstatus_t 
compare(const void *a,const void *b)
{
    uint32_t av = *(uint32_t *)a;
    uint32_t bv = *(uint32_t *)b;
    if(av > bv) {
        return -1;
    } else if(av < bv) {
        return 1;
    } else {
        return 0;
    }
}

/* @brief: convert a ip to its digital number and 
 *         verify the given ip is in the whitelist or not
 * @param: [in]  fd: file-descriptor
 * @param: [out] type: the correspondent r/w type of valid ip
 * @return rstatus_t */
rstatus_t 
nc_ip_verify(int fd, int *type)
{
    err_t ret = 0;
    struct sockaddr sa;
    socklen_t addr_len = sizeof(struct sockaddr_in);

    ret = getpeername(fd,&sa,&addr_len);
    if (ret != 0) {
        log_warn("Failed to invoke getpeername function");
        return NC_ERROR;
    }
    uint32_t ip_to_num = inet_network(inet_ntoa(((struct sockaddr_in *)&sa)->sin_addr));
    
    return nc_bsearch(ip_to_num, type);
}

/* @brief: just check out the digital ip is in the whitelist or not 
 *         and return its r/w type
 * @param: [in]  num: the digital number of valid ip
 * @param: [out] type: the correspondent r/w type of valid ip 
 * @return: rstatus_t */
rstatus_t 
nc_bsearch(uint32_t num, int *type)
{
    int whitelist_tag = -1;
    int whitelist_element_num = -1;
    perm_t* w_list = NULL;

    uint32_t wl_num_tag_2_int = proxy_atomic_read(&wl_num_tag);
    whitelist_tag = wl_num_tag_2_int >> (sizeof(uint32_t) * 8 - WL_TAG_BITS);
    whitelist_element_num = (wl_num_tag_2_int << WL_TAG_BITS) >> WL_TAG_BITS;

    if(0 == whitelist_element_num) {
        *type = (unsigned)(PERM_R | PERM_W);
        return NC_OK;
    }
    if (whitelist_tag == 0) {
        w_list = proxy_w_list.whitelist;
    } else if (whitelist_tag == 1) {
        w_list = proxy_w_list.whitelist_switch;
    }

    void* hit = bsearch((const void*)&num, (const void*)w_list, (size_t)whitelist_element_num, sizeof(perm_t), compare);
    if (hit) {
        perm_t* permt = (perm_t*)hit;
        *type = permt->perm;
        return NC_OK;
    }
    else {
        return NC_ERROR;
    }
}

/* @brief: create a detached thread in order to poll the whitelist 
 * @param: [int] nci: a pointer to the structure of nci 
 * @return: void */
void 
nc_get_whitelist(struct instance *nci)
{
    if (nci->whitelist == 0 && nci->graylist == NULL) {
        loga("whitelist & graylist : off, whitelist & graylist thread exit now");
    } else {
        pthread_t thread;
        if(pthread_create(&thread,NULL,intervalGetWhitelist,(void *)nci) != 0)
        {
            log_warn("Fatal:Can't initialize the whitelist thread.");
            exit(1);
        }
        return;
    }
    return;
}

/* @brief: get full file name from the given filename,for example,the field of
 *         whitelist_prefix is "whitelist", the field of whitelist_switch is 
 *         "bns", so the field of filename is "whitelist.bns" */
/* @param: [in]  whitelist_prefix: the prefix of whitelist (ip whitelist or bns whitelist) 
 * @param: [in]  whitelist_switch: the suffix of whitelist (ip whitelist or bns whitelist)
 * @param: [out] filename: get the joint filename according the prefix and suffix of whitelist  
 * @return static rstatus_t */
static rstatus_t
nc_get_full_filename(char *whitelist_prefix, char *filename, char *whitelist_suffix)
{
    int length = strlen(whitelist_suffix) + strlen(whitelist_prefix) + 1;
    
    if (length < MAXLEN) {
        strncpy(filename,whitelist_prefix,strlen(whitelist_prefix)); 
    } else {
        log_warn("FATAL!The filename is too long");
        return NC_ERROR;
    }

    strcat(filename, ".");
    strcat(filename, whitelist_suffix);  

    return NC_OK;
}

/* @brief: check out the given file is existed or not 
 * @param: [in] arg: the file name(ip whitelist or bns whitelist) 
 * @return: static rstatus_t */
static rstatus_t 
file_exist(char *file_name)
{
    if(access(file_name,R_OK|F_OK) == -1) {
        log_debug(LOG_VERB, "The file[%s] doesn't exist or cannot be readed!", file_name); 
        return NC_ERROR;
    }
    return NC_OK;
}
int parse_graylist(struct instance *nci, char *buf)
{
#define BUF_MAX_SIZE (128*20)
    char *parse;
    char *s, *ss, *e;
    int idx = (nci->gray_idx + 1) % 2;
    int mode = 0, i, num = 0;
    char dbuf[BUF_MAX_SIZE] = {0};

    s = strchr(buf, '\"');
    e = strrchr(buf, '\"');
    if(s == NULL || e == NULL || s >= e) {
        return -1;
    }
    ss = strchr(s+1, '\"');
    if(ss == NULL || ss != e) {
        return -1;
    }
    parse = strtok(buf, " \t");
    if(NULL == parse || s <= parse || (1 != strlen(parse))) {
        return -1;
    }
    if(*parse == 'b') {
        mode = REDIS_CMDS_BLACK;
        for(i = 0; i < MSG_REQ_REDIS_NUM; i++) {
            nci->cmd_gray[idx][i] = REDIS_CMDS_WHITE;
        }
    } else if (*parse == 'w') {
        mode = REDIS_CMDS_WHITE;
        for(i = 0; i < MSG_REQ_REDIS_NUM; i++) {
            if(i == (MSG_REQ_REDIS_PING & 0XFF) || 
                    i == (MSG_REQ_REDIS_AUTH & 0xFF) ||
                    i == (MSG_REQ_REDIS_QUIT & 0XFF)) {
                nci->cmd_gray[idx][i] = REDIS_CMDS_WHITE;
            } else {
                nci->cmd_gray[idx][i] = REDIS_CMDS_BLACK;
            }
        }
    } else {
        return -1;
    }

    while(NULL != (parse = strtok(NULL, " ,\t\r\n\""))) {
        num++;
        if(1 == num && s >= parse) {
            return -1;
        }
        for(i = 0; i < MSG_REQ_REDIS_NUM; i++) {
            if(!strcmp(parse, msg_name[i])) {
                strcat(dbuf, parse);
                strcat(dbuf, ", ");
                nci->cmd_gray[idx][i] = mode;
                break;
            }
        }
    }
    nci->gray_idx = idx;
    if(mode == REDIS_CMDS_BLACK) {
        log_debug(LOG_WARN, "parse cmd blacklist: %s", dbuf);
    } else {
        log_debug(LOG_WARN, "parse cmd whitelist: %s", dbuf);
    }
    return 0;
}

static rstatus_t
_intervalUpdateGraylist(struct instance *nci)
{
#define BUF_MAX_SIZE (128*20)
    static time_t grayfile_last_modif;
    char *filename = nci->graylist;
    struct stat statbuff;
    char buf[BUF_MAX_SIZE];    
    FILE *f;

    if(NULL == filename || NC_OK != file_exist(filename)) {
        return NC_OK;
    }
    if(stat(filename, &statbuff) < 0) {
        log_warn("Failed to get the stat of graylist file [%s]!", filename);
        return NC_ERROR;
    }
    if(grayfile_last_modif == statbuff.st_mtime) {
        return NC_OK;
    }
    f = fopen(filename, "r");
    if(NULL == f) {
        log_warn("Failed to open graylist file [%s]!", filename);
        return NC_ERROR;
    }
    if(NULL == fgets(buf, BUF_MAX_SIZE, f)) {
        log_warn("Failed to read graylist file [%s]!", filename);
        fclose(f);
        return NC_ERROR;
    }

    if(-1 == parse_graylist(nci, buf)) {
        log_warn("Failed to parse graylist file [%s]!", filename);
    }

    fclose(f);
    grayfile_last_modif = statbuff.st_mtime;
    return NC_OK;
}

/*@brief: The whitelist thread,which will poll the existed ip whitelist 
 *        or bns whitelist at the interval of 2 seconds at default 
 *@param: [in] arg: thread parameter */
/*@return void* */
void* 
intervalGetWhitelist(void *arg)
{
    /* Detach itself to avoid the possibility of memory leakage */
    pthread_detach(pthread_self());

    struct instance *nci;
    nci = (struct instance *)arg;
    err_t err = 0;
    char ip[MAXLEN] = "0";
    char bns[MAXLEN] = "0";

    if(nci->whitelist) {
        gettimeofday(&old,NULL);
        err = nc_get_full_filename(nci->whitelist_prefix, bns, "bns"); 
        if (err == NC_ERROR) {
            return NULL;
        } else {
            loga("Success to get the name of bns whitelist:[%s]", bns);
        }

        err = nc_get_full_filename(nci->whitelist_prefix, ip, "ip");
        if (err == NC_ERROR) {
            return NULL;
        } else {
            loga("Success to get the name of ip whitelist:[%s]", ip);
        }
    }

    while (1) {
        if (nci->whitelist == 1) {
            err = _intervalGetWhitelist(arg, CONN_WL_UPDATE, bns, ip);
            if (err == -1) {
                log_warn("Ip/bns whitelist update failed!"); 
            }
        }
        err = _intervalUpdateGraylist(nci);
        sleep(2);
    } 
}

/* @brief: get the corresponding permission:r/w/rw
 * @param: [in] arg: thread parameter 
 * @return: static uint32_t */
static uint32_t 
nc_get_perm(char *buf)
{
    int buf_len = strlen(buf);
    int i = 0;
    uint32_t permint = PERM_NONE;   

    for (i = 0; i < buf_len; i++) {
        if (buf[i] == 'r' || buf[i] == 'R') {
            permint |= PERM_R;
        } else if (buf[i] == 'w' || buf[i] == 'W') {
            permint |= PERM_W;
        } else if (buf[i] == 'x' || buf[i] == 'X') {
            permint |= PERM_X;
        } else {
            log_warn("invalid perm argument '%c'", buf[i]);
            return PERM_NONE;
        }
    }

    return permint;
}

/* @brief: get the timestamp of ip whitelist 
 * @param: [in] arg: the name of ip whitelist 
 * @return: static uint32_t */
static rstatus_t 
nc_check_ipwhitelist(char *file_name)
{
    if(access(file_name,R_OK|F_OK) == -1) {
        log_warn("The whitelist(%s) doesn't exist or cannot be readed!The file of whitelist should be included!", file_name); 
        return NC_ERROR;
    } else {
        struct stat statbuff;
        if(stat(file_name,&statbuff) < 0) {
            log_warn("Failed to get the stat of whitelist file [%s]!", file_name);
            return NC_ERROR;
        } else {
            if(ip_last_modification != statbuff.st_mtime) {
                ip_last_modification = statbuff.st_mtime;
                return NC_OK;
            } else {
                return NC_ERROR;
            }
        }
    }
}

/* @brief: get the timestamp of bns whitelist 
 * @param: [in] arg: the name of bns whitelist 
 * @return: static uint32_t */
static rstatus_t 
nc_check_bnswhitelist (char *file_name)
{
    if(access(file_name,R_OK|F_OK) == -1) {
        log_warn("The whitelist(%s) doesn't exist or cannot be readed!The file of whitelist should be included!", file_name); 
        return NC_ERROR;
    } else {
        struct stat statbuff;
        if(stat(file_name,&statbuff) < 0) {
            log_warn("Failed to get the stat of whitelist file [%s]!", file_name);
            return NC_ERROR;
        } else {
            if(bns_last_modification != statbuff.st_mtime) {
                bns_last_modification = statbuff.st_mtime;
                return NC_OK;
            } else {
                return NC_ERROR;
            }
        }
    }
}

static void
nc_whitelist_del_dup(perm_t *w_list, uint32_t *cnt)
{
    uint32_t i, n = *cnt;

    if(n <= 1) {
        return;
    }
    for(i = 0; i < n - 1; i++) {
        if(w_list[n-1].ip == w_list[i].ip) {
            w_list[i].perm |= w_list[n-1].perm;
            *cnt = n - 1;
            return;
        }
    }
}


/* @brief: parse the ip whitelist in order to get ip list be pointed
 * @param: [IN]  file_name: the file name of ip whitelist  
 * @param: [IN]  bns: the file name of bns whitelist  
 * @param: [OUT] w_list: the buffer which will store the ip whitelist 
 * @param: [OUT] cnt: the number of w_list 
 * @param: [IN]  white_list_fd: the file descriptor of ip whitelist
 * @return: static rstatus_t */
static void
nc_ip_whitelist_parser(char *file_name, char *bns, perm_t **w_list, 
                       uint32_t *cnt, FILE *white_list_fd)
{
    char buf[100]="0";

    NUT_NOTUSED(bns);
    
    /* deal with ipwhitelist */
    while (fgets(buf, (int)sizeof(buf), white_list_fd)) {
        if(strchr("\n\r#", *buf))
            continue;
        if(*cnt >= WHITELIST_MAX_NUMS) {
            log_warn("the number of ip in file [%s] is more than iplist_max, [max:%d]", 
                    file_name, WHITELIST_MAX_NUMS);
            break;
        }

        char *p_parse  = strtok(buf, " \t\n\r");
        int  field_cnt = 0;
        uint32_t perm;

        while (p_parse) {
            if (field_cnt == 0)
                (*w_list)[*cnt].ip = inet_network(p_parse);
            else if (field_cnt == 1) {
                perm = nc_get_perm(p_parse);
                if (perm == PERM_NONE) {
                    field_cnt = 3;
                    break;
                }
                (*w_list)[*cnt].perm = perm;
            }
            else {
                log_warn("configure bad format[egg:ip rw]");
                field_cnt = 3;
                break;
            }
            p_parse = strtok(NULL, " \t\n\r");
            field_cnt++;
        }

        if (field_cnt == 2) {
            (*cnt)++;
            nc_whitelist_del_dup(*w_list, cnt);
        }
    }
    nc_whitelist_del_dup(*w_list, cnt);
}

/* @brief: parse the ip whitelist in order to get ip list be pointed
 * @param: [IN]  file_name: the file name of ip whitelist  
 * @param: [IN]  bns: the file name of bns whitelist  
 * @param: [OUT] w_list: the buffer which will store the ip whitelist 
 * @param: [OUT] cnt: the number of w_list 
 * @param: [IN]  bns_fd: the file descriptor of bns whitelist
 * @return: static rstatus_t */
static void
nc_bns_whitelist_parser(char *file_name, char *bns, perm_t **w_list,
                        uint32_t *cnt, FILE *bns_fd)
{
    /* deal with bnswhitelist */
    char buf[BNS_BUF];
    char tmp_buf[BNS_STORAGE]; 

    NUT_NOTUSED(file_name);
    NUT_NOTUSED(bns);

    while (fgets(buf, (int)sizeof(buf), bns_fd) != NULL) {
        if(strchr("\n\r#", *buf))
            continue;
        if(*cnt >= WHITELIST_MAX_NUMS) {
            log_warn("the number of ip in bns and whitelist is more than iplist_max, [max:%d]", 
                     WHITELIST_MAX_NUMS);
            break;
        }

        char *p_bns_pos = NULL;
        char *p_parse  = strtok(buf, " \t\n\r");
        int  field_cnt = 0;
        uint32_t perm = 1;

        while (p_parse) {
            if (field_cnt == 0) {
                p_bns_pos = p_parse;
            }
            else if (field_cnt == 1) {
                perm = nc_get_perm(p_parse);
            }
            else {
                log_warn("configure bad format[egg:ip rw]");
                field_cnt++;
                break;
            }
            p_parse = strtok(NULL, " \t\n\r");
            field_cnt++;
        }

        if (field_cnt == 2 && perm != PERM_NONE)
        {
            FILE *pipe_ptr = NULL;
            char command[100] = "0";
            strncpy(command,BNS_COMMAND,strlen(BNS_COMMAND));
            sprintf(command,"get_instance_by_service %s -i 2>&1 | cut -d ' ' -f2",p_bns_pos); 
            if ((pipe_ptr=popen(command,"r")) != NULL) {
                while (fgets(tmp_buf, (int)sizeof(tmp_buf), pipe_ptr) != NULL) {
                    if (nc_strncmp(tmp_buf, BNS_WRONG, strlen(BNS_WRONG)) == 0) {
                        log_warn("The BNS name cannot be analyzed:%s",p_bns_pos);
                        break;
                    }                
                    tmp_buf[strlen(tmp_buf) - 1] = '\0';  
                    if(*cnt >= WHITELIST_MAX_NUMS) {
                        break;
                    }
                    (*w_list)[*cnt].ip = inet_network(tmp_buf);
                    (*w_list)[*cnt].perm = perm;
                    (*cnt)++;
                    nc_whitelist_del_dup(*w_list, cnt);
                }
                pclose(pipe_ptr);
                pipe_ptr = NULL;
            }
            else {
                log_warn("Fail to call popen function:%s", strerror(errno));
            }
        }
    }
    nc_whitelist_del_dup(*w_list, cnt);
}

/* @brief: sort the whitelist which in stores in w_list 
 * @param: [OUT] next_tag: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful ,it will
 *               point to the exact w_list 
 * @param: [OUT] wl_num_tag_temp: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful, it make sure
 *               that the order of w_list will be sort only once 
 * @param: [IN]  file_name: the file name of ip whitelist
 * @param: [IN]  bns: the file name of bns whitelist 
 * @param: [IN|OUT] w_list: the buffer of whitelist
 * @paran: [IN]: cnt: the number of w_list */
static void 
nc_whitelist_sort(uint32_t *next_tag, uint32_t *wl_num_tag_temp, char *file_name,
                  char *bns, perm_t **w_list, uint32_t *cnt)
{
    NUT_NOTUSED(file_name);
    NUT_NOTUSED(bns);

    qsort(*w_list, (size_t)*cnt, sizeof(perm_t), compare);
    *wl_num_tag_temp = (*next_tag << (sizeof(uint32_t) * 8 - WL_TAG_BITS)) | *cnt;
    proxy_atomic_set(&wl_num_tag,*wl_num_tag_temp);
    loga("[Trigger by BNS reading]whitelist and bns whitelist updated, tag: %d, elements_num:%d", *next_tag, *cnt);
}

/* @brief: analyze the ip whitelist 
 * @param: [OUT] next_tag: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful ,it will
 *               point to the exact w_list 
 * @param: [OUT] wl_num_tag_temp: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful, it make sure
 *               that the order of w_list will be sort only once 
 * @param: [IN]  file_name: the file name of ip whitelist
 * @param: [IN]  bns: the file name of bns whitelist 
 * @param: [IN|OUT] w_list: the buffer of whitelist
 * @paran: [IN]: cnt: the number of w_list */
static rstatus_t 
nc_read_ip_whitelist(uint32_t *next_tag, uint32_t *wl_num_tag_temp, char *file_name,
                     char *bns, perm_t **w_list, uint32_t *cnt)
{
    if (nc_check_ipwhitelist(file_name) != -1 || force_to_read_whitelist == 1 || nc_check_bnswhitelist(bns) != -1) {
        force_to_read_bns = 1;    
        
        FILE *white_list_fd = fopen(file_name,"r");
        if (white_list_fd == NULL) {
            log_warn("Failed to open the whitelist file[%s]!", file_name);
            return NC_ERROR;
        } else {
            nc_ip_whitelist_parser(file_name, bns,w_list, cnt, white_list_fd);
            fclose(white_list_fd);        
        }

        if (force_to_read_whitelist == 1){
            nc_whitelist_sort(next_tag, wl_num_tag_temp, file_name, bns,w_list, cnt);
        }
    }
    return NC_OK;
}

/* @brief: analyze the bns whitelist 
 * @param: [OUT] next_tag: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful ,it will
 *               point to the exact w_list 
 * @param: [OUT] wl_num_tag_temp: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful, it make sure
 *               that the order of w_list will be sort only once 
 * @param: [IN]  file_name: the file name of ip whitelist
 * @param: [IN]  bns: the file name of bns whitelist 
 * @param: [IN|OUT] w_list: the buffer of whitelist
 * @paran: [IN]: cnt: the number of w_list */
static rstatus_t 
nc_read_bns_whitelist(uint32_t *next_tag, uint32_t *wl_num_tag_temp, char *file_name,
                      char *bns,perm_t **w_list, uint32_t *cnt)
{
    int tick;

    if (file_exist(bns) != NC_ERROR) {
        gettimeofday(&new,NULL);
        tick = 1000000 * (new.tv_sec - old.tv_sec) + new.tv_usec - old.tv_usec;

        /* trigger the timer or force to read bns */
        if (tick >= BNS_PERIOD || force_to_read_bns == 1) {
            if (tick >= BNS_PERIOD)
                force_to_read_whitelist = 1; /* force to read the whitelist due the tick is reaching! */
            else {
                if (force_to_read_bns == 1)
                    force_to_read_whitelist = 0;
            }
            gettimeofday(&old,NULL);
            
            FILE *bns_fd = fopen(bns,"r");
            if(bns_fd == NULL) {
                log_warn("Failed to open the BNS whitelist file[%s]!", bns);
                return NC_ERROR;
            }

            nc_bns_whitelist_parser(file_name, bns,w_list, cnt, bns_fd);
            fclose(bns_fd);        
        }

        if (force_to_read_bns == 1) {
            nc_whitelist_sort(next_tag, wl_num_tag_temp, file_name, bns,w_list, cnt);
        }
    }
	else {
		return NC_ERROR;
	}
    /******
     force to read the whitelist no matter the 
      timestamp of whitelist is modified or not,
       but if the whitelist has been read just now,
      we will avoid read the whitelist again. 
    ******/
    if (force_to_read_bns == 0 && force_to_read_whitelist == 1) {
        nc_read_ip_whitelist(next_tag, wl_num_tag_temp, file_name, bns, w_list, cnt);
    }
    return NC_OK;
}

/* @brief: only analyze the ip whitelist 
 * @param: [OUT] next_tag: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful ,it will
 *               point to the exact w_list 
 * @param: [OUT] wl_num_tag_temp: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful, it make sure
 *               that the order of w_list will be sort only once 
 * @param: [IN]  file_name: the file name of ip whitelist
 * @param: [IN]  bns: the file name of bns whitelist 
 * @param: [IN|OUT] w_list: the buffer of whitelist
 * @paran: [IN]: cnt: the number of w_list */
static rstatus_t 
nc_read_only_ip_whitelist(uint32_t *next_tag, uint32_t *wl_num_tag_temp,
                          char *file_name, char *bns,perm_t **w_list, uint32_t *cnt)
{
    if (nc_check_ipwhitelist(file_name) == NC_OK) {
        FILE *white_list_fd = fopen(file_name,"r");

        if(white_list_fd == NULL) {
            log_warn("Failed to open the whitelist file[%s]!", file_name);
            return NC_ERROR;
        } else {
            nc_ip_whitelist_parser(file_name, bns,w_list, cnt, white_list_fd);
            fclose(white_list_fd);        

            nc_whitelist_sort(next_tag, wl_num_tag_temp, file_name, bns,w_list, cnt);
        }
    }

    return NC_OK;
}

/* @brief: only analyze the bns whitelist 
 * @param: [OUT] next_tag: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful ,it will
 *               point to the exact w_list 
 * @param: [OUT] wl_num_tag_temp: when the ip whitelist and bns whitelist
 *               are both existed, this field will be useful, it make sure
 *               that the order of w_list will be sort only once 
 * @param: [IN]  file_name: the file name of ip whitelist
 * @param: [IN]  bns: the file name of bns whitelist 
 * @param: [IN|OUT] w_list: the buffer of whitelist
 * @paran: [IN]: cnt: the number of w_list */
static rstatus_t 
nc_read_only_bns_whitelist(uint32_t *next_tag, uint32_t *wl_num_tag_temp,
                           char *file_name, char *bns,perm_t **w_list, uint32_t *cnt)
{
    int tick;

    gettimeofday(&new,NULL);
    tick = 1000000 * (new.tv_sec - old.tv_sec) + new.tv_usec - old.tv_usec;

    if (tick >= BNS_PERIOD || nc_check_bnswhitelist(bns) == NC_OK) {
        gettimeofday(&old,NULL);
        
        FILE *bns_fd = fopen(bns,"r");
        if(bns_fd == NULL) {
            log_warn("Failed to open the BNS whitelist file[%s]!", bns);
            return NC_ERROR;
        }

        nc_bns_whitelist_parser(file_name, bns,w_list, cnt, bns_fd);
        fclose(bns_fd);        

        nc_whitelist_sort(next_tag, wl_num_tag_temp, file_name, bns,w_list, cnt);
    }

    return NC_OK;
}

rstatus_t 
_intervalGetWhitelist(void *arg, int type ,char *bns ,char *ip)
{
	int ret;
    uint32_t next_tag = 0;
    uint32_t wl_num_tag_temp = 0;
    uint32_t whitelist_tag = 0;
    perm_t* w_list = NULL;
    uint32_t cnt = 0;
    force_to_read_whitelist = 0;
    force_to_read_bns = 0;

    NUT_NOTUSED(arg);
    NUT_NOTUSED(type);

    uint32_t wl_num_tag_2_int = proxy_atomic_read(&wl_num_tag);    
    whitelist_tag = wl_num_tag_2_int >> (sizeof(uint32_t) * 8 - WL_TAG_BITS);

    if (whitelist_tag == 0) {
        w_list = proxy_w_list.whitelist_switch;
        next_tag = 1;
    } else if (whitelist_tag == 1) {
        w_list = proxy_w_list.whitelist;
        next_tag = 0;
    } else {
        log_warn("Wrong tag:%d!Exit now",whitelist_tag);
        return NC_ERROR;
    }

    if (file_exist(ip) == NC_OK && file_exist(bns) != NC_OK) {
        ret = nc_read_only_ip_whitelist(&next_tag, &wl_num_tag_temp, ip, bns, &w_list,&cnt);
        if (ret == -1) {
            return NC_ERROR;
        }
    } else if (file_exist(ip) == NC_OK && file_exist(bns) == NC_OK) {

        ret = nc_read_ip_whitelist(&next_tag, &wl_num_tag_temp, ip, bns, &w_list, &cnt);
        if (ret == -1) {
            return NC_ERROR;
        }

        ret = nc_read_bns_whitelist(&next_tag, &wl_num_tag_temp, ip, bns, &w_list, &cnt);
        if (ret == -1) {
            return NC_ERROR;
        }

    } else if ( file_exist(ip) != NC_OK && file_exist(bns) == NC_OK) {
        ret = nc_read_only_bns_whitelist(&next_tag, &wl_num_tag_temp, ip, bns, &w_list, &cnt);
        if (ret == -1) {
            return NC_ERROR;
        }
    } else if ( file_exist(ip) != NC_OK && file_exist(bns) != NC_OK){
        loga("DEADLY FATAL!Neither [%s] nor [%s] exists!", ip, bns);    
    }
    
    return NC_OK;
}
