/* Integration executable only: observed constructor/destructor stages. The
 * log barrier is reached by an actual native DESTROY log callback. Every
 * ordinary operation, including shutdown, still reaches librdkafka. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

typedef void (*log_callback)(const rd_kafka_t *, int, const char *, const char *);
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t resume = PTHREAD_COND_INITIALIZER;
static int enabled, mode, released, counters[10];
static rd_kafka_t *client;
static rd_kafka_queue_t *message_queue;
static rd_kafka_conf_t *duplicate;
static log_callback user_log;
extern void rdkafka_hs_state_client_created(rd_kafka_type_t, rd_kafka_t *);

#define REAL(name) __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr, "callback lifetime: %s\n", dlerror()); abort(); }

/* Called with the lock held. Native/Haskell cancellation cannot release this
 * barrier; the controller must observe it and release it explicitly. */
static void boundary(void) {
  counters[0]++;
  while (!released) pthread_cond_wait(&resume, &lock);
}
void rdkafka_hs_test_callbacks_begin(int value) {
  pthread_mutex_lock(&lock);
  enabled = 1; mode = value; released = 0;
  memset(counters, 0, sizeof counters);
  client = NULL; message_queue = NULL; duplicate = NULL; user_log = NULL;
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_callbacks_release(void) {
  pthread_mutex_lock(&lock);
  released = 1;
  pthread_cond_broadcast(&resume);
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_callbacks_end(void) {
  pthread_mutex_lock(&lock);
  enabled = 0; released = 1;
  /* Do not keep dead native addresses reachable for heap diagnostics. */
  client = NULL; message_queue = NULL; duplicate = NULL; user_log = NULL;
  pthread_cond_broadcast(&resume);
  pthread_mutex_unlock(&lock);
}
int rdkafka_hs_test_callbacks_stat(int index) {
  int value;
  pthread_mutex_lock(&lock);
  value = index >= 0 && index < 10 ? counters[index] : -1;
  pthread_mutex_unlock(&lock);
  return value;
}

/* Explicit diagnostic negative control, never called by ordinary tests. An
 * instrumented native library must diagnose the read in conf_get. */
int rdkafka_hs_test_callbacks_invalid_access(void) {
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_destroy(conf);
  size_t size = 0;
  return (int)rd_kafka_conf_get(conf, "client.id", NULL, &size);
}

static void forward_log(const rd_kafka_t *rk, int level, const char *facility, const char *message) {
  pthread_mutex_lock(&lock);
  log_callback callback = user_log;
  int shutting_down = enabled && rk == client && counters[2] > counters[3];
  if (enabled) counters[9]++;
  pthread_mutex_unlock(&lock);
  if (callback) callback(rk, level, facility, message);
  pthread_mutex_lock(&lock);
  if (shutting_down && strcmp(facility, "DESTROY") == 0) {
    counters[4]++;
    if (mode == 6 && counters[0] == 0) boundary();
  }
  pthread_mutex_unlock(&lock);
}

void rd_kafka_conf_set_log_cb(rd_kafka_conf_t *conf, log_callback callback) {
  REAL(rd_kafka_conf_set_log_cb);
  pthread_mutex_lock(&lock);
  int tracking = enabled;
  if (tracking) user_log = callback;
  pthread_mutex_unlock(&lock);
  real(conf, tracking ? forward_log : callback);
}

void rd_kafka_conf_set_stats_cb(rd_kafka_conf_t *conf,
    int (*callback)(rd_kafka_t *, char *, size_t, void *)) {
  REAL(rd_kafka_conf_set_stats_cb);
  real(conf, callback);
  pthread_mutex_lock(&lock);
  if (enabled) {
    counters[6]++;
    if (mode == 1) boundary();
  }
  pthread_mutex_unlock(&lock);
}

rd_kafka_conf_t *rd_kafka_conf_dup(const rd_kafka_conf_t *conf) {
  REAL(rd_kafka_conf_dup);
  rd_kafka_conf_t *result = real(conf);
  pthread_mutex_lock(&lock);
  if (enabled) { duplicate = result; counters[7]++; }
  pthread_mutex_unlock(&lock);
  return result;
}
void rd_kafka_conf_destroy(rd_kafka_conf_t *conf) {
  REAL(rd_kafka_conf_destroy);
  pthread_mutex_lock(&lock);
  if (enabled && conf == duplicate) { duplicate = NULL; counters[5]++; }
  pthread_mutex_unlock(&lock);
  real(conf);
}
rd_kafka_t *rd_kafka_new(rd_kafka_type_t type, rd_kafka_conf_t *conf, char *error, size_t capacity) {
  REAL(rd_kafka_new);
  rd_kafka_t *result = real(type, conf, error, capacity);
  rdkafka_hs_state_client_created(type, result);
  pthread_mutex_lock(&lock);
  if (enabled && result) {
    if (client) abort(); /* Each fixture owns one client. */
    client = result;
    duplicate = NULL; /* Consumed by successful native creation. */
    counters[1]++;
    if (mode == 2) boundary();
  }
  pthread_mutex_unlock(&lock);
  return result;
}
void rdkafka_hs_callbacks_queue_created(rd_kafka_t *rk, rd_kafka_queue_t *queue) {
  pthread_mutex_lock(&lock);
  if (enabled && rk == client && !message_queue) message_queue = queue;
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_callbacks_queue_destroying(rd_kafka_queue_t *queue) {
  pthread_mutex_lock(&lock);
  if (enabled && queue == message_queue) {
    counters[8]++;
    if (mode == 4) boundary();
    message_queue = NULL;
  }
  pthread_mutex_unlock(&lock);
}
void rd_kafka_destroy_flags(rd_kafka_t *rk, int flags) {
  REAL(rd_kafka_destroy_flags);
  pthread_mutex_lock(&lock);
  int tracking = enabled && rk == client;
  if (tracking) {
    counters[2]++;
    const char *probe = getenv("RDKAFKA_TEST_CALLBACK_SAFETY_PROBE");
    if (probe) {
      fprintf(stderr, "callback safety probe: native destructor entered (%s)\n", probe);
      fflush(stderr);
    }
    if (mode == 3) boundary();
  }
  pthread_mutex_unlock(&lock);
  real(rk, flags);
  pthread_mutex_lock(&lock);
  if (tracking) {
    counters[3]++;
    if (mode == 5) boundary();
    client = NULL;
  }
  pthread_mutex_unlock(&lock);
}
