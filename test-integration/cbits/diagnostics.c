/* Test-only ownership observation. Faults retain genuine native dump arrays
 * until after assertions, then release any deliberately abandoned objects. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#define REAL(name) __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr, "diagnostics: %s\n", dlerror()); abort(); }
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t condition = PTHREAD_COND_INITIALIZER;
static int enabled, mode, entered, released, stats[4], last_descriptor;
static const char **arrays[256];
static size_t sizes[256];
static FILE *files[256];

void rdkafka_hs_test_diagnostic_begin(int value) {
  pthread_mutex_lock(&lock);
  enabled = 1; mode = value; entered = released = 0; last_descriptor = -1;
  for (int i = 0; i < 4; ++i) stats[i] = 0;
  for (int i = 0; i < 256; ++i) { arrays[i] = NULL; files[i] = NULL; }
  pthread_mutex_unlock(&lock);
}
int rdkafka_hs_test_diagnostic_stat(int index) {
  pthread_mutex_lock(&lock);
  int result = index >= 0 && index < 4 ? stats[index] : index == 4 ? (last_descriptor >= 0 && fcntl(last_descriptor, F_GETFD) >= 0) : -1;
  pthread_mutex_unlock(&lock); return result;
}
int rdkafka_hs_test_diagnostic_entered(void) {
  pthread_mutex_lock(&lock); int result = entered;
  pthread_mutex_unlock(&lock); return result;
}
void rdkafka_hs_test_diagnostic_release(void) {
  pthread_mutex_lock(&lock); released = 1;
  pthread_cond_broadcast(&condition); pthread_mutex_unlock(&lock);
}
static void acquired(const char **array, size_t size, int kind) {
  pthread_mutex_lock(&lock);
  if (enabled) {
    int i; for (i = 0; i < 256 && arrays[i]; ++i) {}
    if (i == 256) abort();
    arrays[i] = array; sizes[i] = size; stats[0]++;
    if (mode == kind) {
      entered = 1;
      while (!released) pthread_cond_wait(&condition, &lock);
    }
  }
  pthread_mutex_unlock(&lock);
}
const char **rd_kafka_conf_dump(rd_kafka_conf_t *conf, size_t *size) {
  REAL(rd_kafka_conf_dump);
  const char **array = real(conf, size); acquired(array, *size, 1); return array;
}
const char **rd_kafka_topic_conf_dump(rd_kafka_topic_conf_t *conf, size_t *size) {
  REAL(rd_kafka_topic_conf_dump);
  const char **array = real(conf, size); acquired(array, *size, 2); return array;
}
void rd_kafka_conf_dump_free(const char **array, size_t size) {
  REAL(rd_kafka_conf_dump_free);
  pthread_mutex_lock(&lock);
  if (enabled) for (int i = 0; i < 256; ++i) if (array == arrays[i]) {
    arrays[i] = NULL; stats[1]++; break;
  }
  pthread_mutex_unlock(&lock); real(array, size);
}
FILE *fdopen(int fd, const char *mode_string) {
  REAL(fdopen);
  pthread_mutex_lock(&lock);
  int reject = enabled && mode == 3;
  if (enabled) last_descriptor = fd;
  pthread_mutex_unlock(&lock);
  if (reject) { errno = EMFILE; return NULL; }
  FILE *file = real(fd, mode_string);
  pthread_mutex_lock(&lock);
  if (enabled && file) {
    int i; for (i = 0; i < 256 && files[i]; ++i) {}
    if (i == 256) abort(); files[i] = file; stats[2]++;
  }
  pthread_mutex_unlock(&lock); return file;
}
int fclose(FILE *file) {
  REAL(fclose);
  pthread_mutex_lock(&lock);
  if (enabled) for (int i = 0; i < 256; ++i) if (file == files[i]) {
    files[i] = NULL; stats[3]++; break;
  }
  int reject = enabled && mode == 4;
  pthread_mutex_unlock(&lock);
  int result = real(file);
  if (reject) { errno = EIO; return EOF; }
  return result;
}
void rdkafka_hs_test_diagnostic_end(void) {
  __typeof__(&rd_kafka_conf_dump_free) free_array = (__typeof__(&rd_kafka_conf_dump_free))dlsym(RTLD_NEXT, "rd_kafka_conf_dump_free");
  __typeof__(&fclose) close_file = (__typeof__(&fclose))dlsym(RTLD_NEXT, "fclose");
  if (!free_array || !close_file) abort();
  pthread_mutex_lock(&lock); enabled = 0; released = 1;
  pthread_cond_broadcast(&condition); pthread_mutex_unlock(&lock);
  for (int i = 0; i < 256; ++i) {
    if (arrays[i]) { free_array(arrays[i], sizes[i]); arrays[i] = NULL; }
    if (files[i]) { close_file(files[i]); files[i] = NULL; }
  }
}
void rdkafka_hs_test_diagnostic_leak_control(void) {
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  size_t size;
  rd_kafka_conf_dump(conf, &size);
  rd_kafka_conf_destroy(conf);
}
