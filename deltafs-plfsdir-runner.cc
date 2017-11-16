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

#include <errno.h>
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
static char* argv0;        /* argv[0], program name */
static deltafs_env_t* env; /* underlying storage abstraction */
static deltafs_tp_t* tp;   /* plfsdir worker thread pool */
static char cf[500];       /* plfsdir conf str */
static struct plfsdir_conf {
  int value_size;
  int key_size;
  int filter_bits_per_key;
  int skip_crc32c;
  int lg_parts;
} c;

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
#define DEF_TIMEOUT 300 /* alarm timeout */
#define DEF_BBOS_HOSTNAME "127.0.0.1"
#define DEF_BBOS_PORT 12345
#define DEF_NUM_EPOCHS 8
#define DEF_NUM_KEYS_PER_EPOCH (1 << 20)
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
  int nkeysperrank;
  int keysz;
  int valsz;
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
 * usage
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
 * main program
 */
int main(int argc, char* argv[]) {
  int r, ch;
  r = MPI_Init(&argc, &argv);
  if (r != MPI_SUCCESS) complain("fail to init mpi");
  argv0 = argv[0];
  memset(cf, 0, sizeof(cf));

  /* we want lines, even if we are writing to a pipe */
  setlinebuf(stdout);

  memset(&g, 0, sizeof(g));
  r = MPI_Comm_rank(MPI_COMM_WORLD, &g.myrank);
  if (r != MPI_SUCCESS) complain("cannot get proc mpi rank");
  r = MPI_Comm_size(MPI_COMM_WORLD, &g.commsz);
  if (r != MPI_SUCCESS) complain("cannot get mpi world size");

  g.nepochs = DEF_NUM_EPOCHS;
  g.nkeysperrank = DEF_NUM_KEYS_PER_EPOCH;
  g.keysz = DEF_KEY_SIZE;
  g.valsz = DEF_VAL_SIZE;
  g.bboshostname = DEF_BBOS_HOSTNAME;
  g.bbosport = DEF_BBOS_PORT;
  g.timeout = DEF_TIMEOUT;

  while ((ch = getopt(argc, argv, "j:t:v")) != -1) {
    switch (ch) {
      case 'j':
        g.bg = atoi(optarg);
        if (g.bg < 0) usage("bad bg number");
        break;
      case 't':
        g.timeout = atoi(optarg);
        if (g.timeout < 0) usage("bad timeout");
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

  if (argc != 1) /* plfsdir must be provided on command line */
    usage("bad args");
  g.dirname = argv[0];

  signal(SIGALRM, sigalarm);
  alarm(g.timeout);

  MPI_Finalize();

  return 0;
}
