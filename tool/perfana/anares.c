#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<math.h>
#include<limits.h>
#include<fcntl.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<errno.h>

#define PATH_LEN 1024
#define COUNT_NR 128
#define TOP_NR 100

char g_file[PATH_LEN];
char g_logfile[PATH_LEN];
char g_marks[PATH_LEN];
long long g_tmark[COUNT_NR];
int g_tcount = 0;
long long g_tstat[COUNT_NR];
long *g_values = NULL;
long long g_cnt = 0;
long g_maxval = LONG_MAX;
unsigned int g_topnr = TOP_NR;
unsigned int g_topsize = 0;
long *g_toparr = NULL;
unsigned int *g_topcnt = NULL;

void help_info(char *proname)
{
    printf("Usage as:%s -f XX -m XX -l XX -n XX -M xx -T XX -h\n"
           "  -f   file to analyze\n"
           "  -m   ascending time intervals with delim ',', such as '2,4,6,8'\n"
           "  -l   log file path\n"
           "  -n   total line to analyze\n"
           "  -M   maxmum value of item in file to analyze, default LONG_MAX\n"
           "  -T   top N items to display, default 100\n"
           "  -h   get help informatin\n",
           proname);
    exit(0); 
}

void toparr_insert(long val)
{
    unsigned int lastidx = g_topsize - 1;
    unsigned int i = 0;
    unsigned int end = 0;
    unsigned int start = 0;
    unsigned mid = 0;

    // toparr is empty
    if (g_topsize == 0) {
        g_topcnt[0] = 1;
        g_toparr[0] = val;
        g_topsize = (g_topsize >= g_topnr) ? g_topnr : g_topsize + 1;
        return;
    }

    // val equal to the biggest or smallest item
    if (val == g_toparr[0]) {
        g_topcnt[0]++;
        return;
    } else if (val == g_toparr[lastidx]) {
        g_topcnt[lastidx]++;
        return;
    }

    // val bigger than current biggest item
    if (val > g_toparr[0]) {
        end = g_topsize == g_topnr ? lastidx : (lastidx + 1);
        for (i = end; i > 0; i--) {
            g_toparr[i] = g_toparr[i - 1];
            g_topcnt[i] = g_topcnt[i - 1];
        }
        g_toparr[0] = val;
        g_topcnt[0] = 1;
        g_topsize = (g_topsize >= g_topnr) ? g_topnr : g_topsize + 1;
        return;
    }

    // smaller then the smallest item
    if ((val < g_toparr[lastidx]) && (g_topsize < g_topnr)) {
        g_toparr[g_topsize] = val;
        g_topcnt[g_topsize] = 1;
        g_topsize = (g_topsize >= g_topnr) ? g_topnr : g_topsize + 1;
        return;
    }
        
    // val between (biggest, smallest)
    start = 0;
    end = lastidx;

    while(end >= start) {
        mid = (start + end) / 2;

        if (val == g_toparr[mid]) {
            g_topcnt[mid]++;
            return;
        }

        if (val > g_toparr[mid]) {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }

    end = g_topsize == g_topnr ? lastidx : (lastidx + 1);
    for (i = end; i > start; i--) {
        g_toparr[i] = g_toparr[i - 1];
        g_topcnt[i] = g_topcnt[i - 1];
    }
    g_toparr[start] = val;
    g_topcnt[start] = 1;
    g_topsize = (g_topsize >= g_topnr) ? g_topnr : g_topsize + 1;
    return;
}

int split_tmarks(void)
{
    char *tval = NULL;
    char tmpmark[PATH_LEN];
    char *pos = NULL;

    snprintf(tmpmark, sizeof(tmpmark), "%s", g_marks);
    pos = tmpmark;

    while (pos) {
        if (g_tcount >= COUNT_NR) {
	    break;
        }

        tval = strsep(&pos, ",");
	if (!tval) {
	    break;
	}

        g_tmark[g_tcount] = atol(tval);
        g_tcount++;
    }

    if (!g_tcount) {
	    printf("get invalid time intervals format:%s\n", g_marks);
	    return -1;
    }

    return 0;
}

int load_config(int argc, char **argv)
{
    int rc = 0;
    int opt = 0;
    opterr = 0;
    unsigned long size = 0;

    do {
    	opt = getopt(argc, argv, "f:m:l:n:M:T:h");
    	if (opt == -1)
    	{
    	    break;
    	}

    	switch(opt)
    	{
            case 'M':
                g_maxval = atol(optarg);
                break;
    	    case 'f':
    		    snprintf(g_file, PATH_LEN, "%s", optarg);
    		    break;
    	    case 'm':
        		snprintf(g_marks, PATH_LEN, "%s", optarg);
        		break;
            case 'l':
        		snprintf(g_logfile, PATH_LEN, "%s", optarg);
        		break;
            case 'n':
        		g_cnt = atol(optarg);
        		break;
            case 'T':
        		g_topnr = atoi(optarg);        		        		       		
        		break;
    	    case 'h':
    		    help_info(argv[0]);
    		    break;
            default:
    		    help_info(argv[0]);
    		    break;
        }
    } while(1);
}

void do_analyze(FILE *ana_fd, int log_fd)
{
    long count = 0;
    long total = 0;
    long avg = 0;
    char buf[PATH_LEN];
    long pos = 0;
    long min = LONG_MAX;
    long max = 0;
    int i = 0;
    float percent = 0.0;
    long sum = 0;
    long tmpval = 0;
    
    while(fgets(buf, sizeof(buf), ana_fd) != NULL) {
        if (count >= g_cnt) {
	        break;
	    }

        tmpval = atol(buf);
        //printf("get line[len:%d cont:%s]\n", strlen(buf), buf);
        if (!tmpval && strcmp(buf, "0\n")) {
	        continue;
        }

        if (tmpval <= g_maxval) {
            g_values[count] = tmpval;
	        count++;

	        toparr_insert(tmpval);
        }
    }

    for (pos = 0; pos < count; pos++) {
    	total += g_values[pos];
        if (g_values[pos] < min) {
	    min =  g_values[pos];
        }

        if (g_values[pos] > max) {
	        max = g_values[pos];
	    }

	    for (i = 0; i < g_tcount; i++) {
    	    if (g_values[pos] <= g_tmark[i]) {
    		    g_tstat[i]++;
    		    //printf("tstat[%d]=%ld\n", i, tstat[i]);
    	    } 
	    }
    }        

    avg = count ? (total / count) : 0;
    printf("[====================================================================================]\n");
    printf("[================================+++++GLOBAL STAT++++============================]\n");
    printf("sum:%ld  count:%ld  avg:%ld  min:%ld  max:%ld\n",
	   total, count, avg, min, max);
    printf("[====================================================================================]\n\n");

    printf("[====================================================================================]\n");
    printf("[================================++++++PERCENT STAT++++++============================]\n");
    for (i = 0; i < g_tcount; i++) {
        percent = (100.0 * g_tstat[i]) / (1.0 * count);
	    printf("< %ld:  count=%ld   percent=%.6f%\n", g_tmark[i], g_tstat[i], percent);
    }
    printf("[====================================================================================]\n\n");

    printf("[====================================================================================]\n");
    printf("[================================++++++SEGMENT STAT++++++============================]\n");
    if (g_tcount) {
    	percent = (100.0 * g_tstat[0]) / (1.0 * count);
    	printf("[0 ~ %ld]: count:%ld  percent=%.6f%\n", g_tmark[0], g_tstat[0], percent);
    	for (i = 1; i < g_tcount; i++) {
    	    sum = g_tstat[i] - g_tstat[i - 1];
            percent = (100.0 * sum) / (1.0 * count);
    	    printf("[%ld ~ %ld]: count:%ld  percent=%.6f%\n", 
    	            g_tmark[i -1], g_tmark[i], sum, percent);
    	}
    }
    printf("[====================================================================================]\n\n");

    printf("[====================================================================================]\n");
    printf("[=============================++=++++++TOP STAT++++++============+++=================]\n");
    printf("TOP %d ITEMS INFORMATION\n", g_topsize);
    for (i = 0; i < g_topsize; i++) {
        printf("  value:%ld  cnt:%u\n", g_toparr[i], g_topcnt[i]);
    }
    
    printf("[====================================================================================]\n\n");
}

int init_topstatspace(void)
{
    unsigned int size = 0;
    
    size = g_topnr * sizeof(long);
	g_toparr = malloc(size);
	if (!g_toparr) {
	    printf("allocate space for top %u items failed\n", g_topnr);
	    return -1;
	}
	memset(g_toparr, 0, size);

    size = g_topnr * sizeof(unsigned int);
	g_topcnt = malloc(size);
	if (!g_topcnt) {
	    printf("allocate space for topcnt array failed\n");
	    free(g_toparr);
	    return -1;
	}
    memset(g_topcnt, 0, size);

    return 0;
}

int main(int argc, char **argv)
{
    int rc = 0;
    FILE *ana_fd = NULL;
    int log_fd = -1;
    long len = 0;

    snprintf(g_logfile, sizeof(g_logfile), "statres.log");
    memset(g_tstat, 0, sizeof(g_tstat));

    load_config(argc, argv);

    printf("[====================================================================================]\n");
    printf("[================================+++++INPUT ARGUMENTS++++============================]\n");
    printf("[filetoanalyze:%s  linecnt:%ld  timemarks:%s  log:%s  topnr:%u\n", 
            g_file, g_cnt, g_marks, g_logfile, g_topnr);
    printf("[====================================================================================]\n\n");

    len = g_cnt * sizeof(long);
    g_values = (long *)malloc(len);
    if (!g_values) {
    	printf("premalloc spaces failed\n");
    	exit(1);
    }
    memset(g_values, 0, len);

    ana_fd = fopen(g_file, "r");
    if (ana_fd == NULL) {
    	printf("open file:%s which to be analyze failed:%d\n", g_file, errno);
            free(g_values);
    	exit(1);
    }

    log_fd = open(g_logfile, O_APPEND|O_CREAT|O_RDWR, 0644);
    if (log_fd == -1) {
	    printf("open log file:%s failed:%d\n", g_file, errno);
        free(g_values);
        fclose(ana_fd);
	    exit(1);
    }

    rc = split_tmarks();
    if (rc < 0) {
	    printf("split_tmarks failed\n");
        free(g_values);
	    fclose(ana_fd);
	    close(log_fd);
	    exit(1);
    }

    rc = init_topstatspace();
    if (rc < 0) {
	    printf("init_topstatspace failed\n");
        free(g_values);
	    fclose(ana_fd);
	    close(log_fd);
	    exit(1);
    }

    do_analyze(ana_fd, log_fd);

    fclose(ana_fd);
    close(log_fd);
    free(g_values);
    free(g_toparr);
    free(g_topcnt);

    return 0;
}
