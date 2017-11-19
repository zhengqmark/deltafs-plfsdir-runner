/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * deltafs-plfsdir-runner.cc
 *
 * a simple testing program for accessing deltafs plfsdirs.
 */

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>

#include <deltafs/deltafs_api.h>

#include <mpi.h>

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */
static char* argv0;            /* argv[0], program name */
static deltafs_plfsdir_t* dir; /* plfsdir handle */
static deltafs_env_t* env;     /* plfsdir storage abs */
static deltafs_tp_t* bgp;      /* plfsdir worker thread pool */
static char cf[500];           /* plfsdir conf str */
static struct bbos_conf {
  char remote[50]; /* bbos remote uri */
  char lo[50];     /* bbos local uri */
} b;

/*
 * vcomplain/complain about something and exit.
 */
static void vcomplain(const char* format, va_list ap) {
  fprintf(stderr, "!!! ERROR !!! %s: ", argv0);
  vfprintf(stderr, format, ap);
  fprintf(stderr, "\n");
  exit(1);
}

static void complain(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vcomplain(format, ap);
  va_end(ap);
}

/*
 * print info messages.
 */
static void vinfo(const char* format, va_list ap) {
  printf("-INFO- ");
  vprintf(format, ap);
  printf("\n");
}

static void info(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vinfo(format, ap);
  va_end(ap);
}

/*
 * now: get current time in micros
 */
static uint64_t now() {
  struct timeval tv;
  uint64_t rv;

  gettimeofday(&tv, NULL);
  rv = tv.tv_sec * 1000000LLU + tv.tv_usec;

  return rv;
}

/*
 * end of helper/utility functions.
 */

/*
 * default values
 */
#define DEF_TIMEOUT 300 /* alarm timeout (secs) */
#define DEF_BBOS_HOSTNAME "127.0.0.1"
#define DEF_BBOS_PORT 12345
#define DEF_NUM_EPOCHS 8
#define DEF_NUM_KEYS_PER_EPOCH (1 << 10)
#define DEF_IO_SIZE (2 << 20)
#define DEF_FILTER_BITS 10
#define DEF_KEY_SIZE 8
#define DEF_VAL_SIZE 32

/*
 * gs: shared global data (from the command line)
 */
struct gs {
  int bg;
  int bbos;
  int bbosport;
  const char* bboshostname;
  const char* dirname;
  int myrank;
  int commsz;
  int nepochs;
  int nkeys;
  int filterbits;
  int keysz;
  int valsz;
  int iosz;
  int logrotation;
  int timeout;
  int v;
} g;

/*
 * alarm signal handler
 */
static void sigalarm(int foo) {
  fprintf(stderr, "!!! SIGALRM detected !!!\n");
  fprintf(stderr, "alarm clock\n");
  exit(1);
}

/*
 * usage: print usage and exit
 */
static void usage(const char* msg) {
  if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
  fprintf(stderr, "usage: %s [options] plfsdir\n", argv0);
  fprintf(stderr, "\noptions:\n");
  fprintf(stderr, "\t-t sec    timeout (alarm), in seconds\n");
  fprintf(stderr, "\t-v        be verbose\n");
  exit(1);
}

/*
 * printopts: print global options
 */
static void printopts() {
  printf("\n%s\n==options:\n", argv0);
  printf("\ttimeout: %d\n", g.timeout);
  printf("\tnum bg threads: %d\n", g.bg);
  printf("\tnum epochs: %d\n", g.nepochs);
  printf("\tnum keys per epoch: %d (per rank)\n", g.nkeys);
  printf("\tplfsdir: %s\n", g.dirname);
  printf("\tkey size: %d\n", g.keysz);
  printf("\tvalue size: %d\n", g.valsz);
  printf("\tfilter bits per key: %d\n", g.filterbits);
  printf("\tio size: %d\n", g.iosz);
  printf("\tlog rotation: %d\n", g.logrotation);
  printf("\tbbos: %d\n", g.bbos);
  printf("\tbbos hostname: %s\n", g.bboshostname);
  printf("\tbbos port: %d\n", g.bbosport);
  printf("\tmpi comm size: %d\n", g.commsz);
  printf("\tverbose: %d\n", g.v);
  printf("\n");
}

static void printerr(const char* err, void* a) {
  fprintf(stderr, " >> [deltafs] %s\n", err);
}

/*
 * mkbbos: init bbos env
 */
static void mkbbos() {
  char env_name[] = "bbos";
  void* a[5];

  if (!g.bbos || env) return;

  a[0] = env_name;
  snprintf(b.lo, sizeof(b.lo), "bmi+tcp");
  a[1] = b.lo;
  snprintf(b.remote, sizeof(b.remote), "bmi+tcp://%s:%d", g.bboshostname,
           g.bbosport);
  a[2] = b.remote;
  a[3] = NULL;
  a[4] = NULL;

  env = deltafs_env_init(5, a);
  if (!env) complain("fail to init bbos env");
}

/*
 * mkconf: generate plfsdir conf
 */
static void mkconf() {
  int n;

  if (g.bg && !bgp) bgp = deltafs_tp_init(g.bg);
  if (g.bg && !bgp) complain("fail to init thread pool");

  n = snprintf(cf, sizeof(cf), "rank=%d", g.myrank);
  n += snprintf(cf + n, sizeof(cf) - n, "&tail_padding=1&block_padding=1");
  n += snprintf(cf + n, sizeof(cf) - n, "&data_buffer=%d", g.iosz);
  n += snprintf(cf + n, sizeof(cf) - n, "&min_data_buffer=%d", g.iosz);
  n += snprintf(cf + n, sizeof(cf) - n, "&index_buffer=%d", g.iosz);
  n += snprintf(cf + n, sizeof(cf) - n, "&min_index_buffer=%d", g.iosz);
  n += snprintf(cf + n, sizeof(cf) - n, "&key_size=%d", g.keysz);
  n += snprintf(cf + n, sizeof(cf) - n, "&value_size=%d", g.valsz);
  n += snprintf(cf + n, sizeof(cf) - n, "&bf_bits_per_key=%d", g.filterbits);
  n +=
      snprintf(cf + n, sizeof(cf) - n, "&epoch_log_rotation=%d", g.logrotation);
  snprintf(cf + n, sizeof(cf) - n, "&lg_parts=%d", 0);

#ifndef NDEBUG
  info(cf);
#endif
}

/*
 * writekey: write a key into plfsdir
 */
static void writekey(int k, int e, const std::string& v) {
  char fname[20];
  int r;

  assert(dir != NULL);

  snprintf(fname, sizeof(fname), "f%08x-r%08x", k, g.myrank);
  r = deltafs_plfsdir_append(dir, fname, e, v.data(), v.size());
  if (r) complain("error writing %s: %s", fname, strerror(errno));
}

/*
 * writepoch: insert epoch data into plfsdir
 */
static void writepoch(int e) {
  std::string v;
  int r;

  assert(dir != NULL);

  v.resize(g.valsz, '.');
  for (int i = 0; i < g.nkeys; i++) {
    writekey(i, e, v);
  }

  r = MPI_Barrier(MPI_COMM_WORLD);
  if (r != MPI_SUCCESS) complain("fail to do mpi barrier");
  r = deltafs_plfsdir_epoch_flush(dir, e);
  if (r) complain("error flushing dir: %s", strerror(errno));
}

/*
 * write: insert data into plfsdir as multiple epochs
 */
static void write() {
  int r;
  if (g.bbos) mkbbos();
  mkconf();
  dir = deltafs_plfsdir_create_handle(cf, O_WRONLY);
  deltafs_plfsdir_set_err_printer(dir, printerr, NULL);
  if (bgp) deltafs_plfsdir_set_thread_pool(dir, bgp);
  if (env) deltafs_plfsdir_set_env(dir, env);

  r = deltafs_plfsdir_open(dir, g.dirname);
  if (r) complain("error opening dir: %s", strerror(errno));
  for (int e = 0; e < g.nepochs; e++) {
    writepoch(e);
  }

  r = deltafs_plfsdir_finish(dir);
  if (r) complain("error finalizing dir: %s", strerror(errno));
  deltafs_plfsdir_free_handle(dir);
}

/*
 * main program
 */
int main(int argc, char* argv[]) {
  int r, ch;
  r = MPI_Init(&argc, &argv);
  if (r != MPI_SUCCESS) complain("fail to init mpi");
  argv0 = argv[0];
  memset(cf, 0, sizeof(cf));
  memset(b.remote, 0, sizeof(b.remote));
  memset(b.lo, 0, sizeof(b.lo));
  /* we want lines, even if we are writing to a pipe */
  setlinebuf(stdout);

  memset(&g, 0, sizeof(g));
  r = MPI_Comm_rank(MPI_COMM_WORLD, &g.myrank);
  if (r != MPI_SUCCESS) complain("cannot get proc mpi rank");
  r = MPI_Comm_size(MPI_COMM_WORLD, &g.commsz);
  if (r != MPI_SUCCESS) complain("cannot get mpi world size");

  g.nepochs = DEF_NUM_EPOCHS;
  g.nkeys = DEF_NUM_KEYS_PER_EPOCH;
  g.filterbits = DEF_FILTER_BITS;
  g.keysz = DEF_KEY_SIZE;
  g.valsz = DEF_VAL_SIZE;
  g.bboshostname = DEF_BBOS_HOSTNAME;
  g.bbosport = DEF_BBOS_PORT;
  g.timeout = DEF_TIMEOUT;
  g.iosz = DEF_IO_SIZE;

  while ((ch = getopt(argc, argv, "s:e:n:f:k:d:j:t:rvb")) != -1) {
    switch (ch) {
      case 's':
        g.iosz = atoi(optarg);
        if (g.iosz <= 0) usage("bad io size");
        break;
      case 'e':
        g.nepochs = atoi(optarg);
        if (g.nepochs < 0) usage("bad epoch nums");
        break;
      case 'n':
        g.nkeys = atoi(optarg);
        if (g.nkeys < 0) usage("bad key nums");
        break;
      case 'f':
        g.filterbits = atoi(optarg);
        if (g.filterbits < 0) usage("bad filter bits");
        break;
      case 'k':
        g.keysz = atoi(optarg);
        if (g.keysz <= 0) usage("bad key size");
        break;
      case 'd':
        g.valsz = atoi(optarg);
        if (g.valsz < 0) usage("bad value size");
        break;
      case 'j':
        g.bg = atoi(optarg);
        if (g.bg < 0) usage("bad bg number");
        break;
      case 't':
        g.timeout = atoi(optarg);
        if (g.timeout < 0) usage("bad timeout");
        break;
      case 'r':
        g.logrotation = 1;
        break;
      case 'b':
        g.bbos = 1;
        break;
      case 'v':
        g.v = 1;
        break;
      default:
        usage(NULL);
    }
  }
  argc -= optind;
  argv += optind;

  if (argc == 0) /* plfsdir must be provided on command line */
    usage("bad args");
  g.dirname = argv[0];
  if (argc > 1) g.bboshostname = argv[1];
  if (argc > 2) g.bbosport = atoi(argv[2]);
  if (g.bbosport <= 0) usage("bad bbos port");
  printopts();

  signal(SIGALRM, sigalarm);
  alarm(g.timeout);

  dir = NULL;
  env = NULL;
  bgp = NULL;

  if (g.v && !g.myrank) info("test begins ...");
  MPI_Barrier(MPI_COMM_WORLD);
  write();

  MPI_Finalize();

  if (g.v && !g.myrank) info("all done!");
  if (g.v && !g.myrank) info("bye");

  exit(0);
}
