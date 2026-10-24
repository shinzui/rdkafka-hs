/* Test executable only: observe real public-library allocations and inject
 * failures at native ownership transitions. Production artifacts never link
 * these symbols. RTLD_NEXT dispatches each ordinary call to librdkafka. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

#define REAL(name) \
  __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr, "admin ownership: %s: %s\n", #name, dlerror()); abort(); }

enum { NEW_TOPIC, DELETE_TOPIC, QUEUE, OPTIONS, EVENT, KINDS };
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t resumed = PTHREAD_COND_INITIALIZER;
static struct { void *pointer; int kind; } live[4096];
static int allocations[KINDS], releases[KINDS], peaks[KINDS];
static char *last_key, *last_value;
static int enabled, fail_config, block_config, config_entered, submissions, config_calls, response_mode, timeout_fault, timeout_reads;

/* ELF may preempt calls made inside librdkafka itself. Count application-owned
 * references, not the library's internal copies and reference increments. */
static int application_call(void *caller) {
  Dl_info image;
  if (!dladdr(caller, &image) || !image.dli_fname) abort();
  const char *name = strrchr(image.dli_fname, '/');
  name = name ? name + 1 : image.dli_fname;
  return strncmp(name, "librdkafka.", 11) != 0;
}
#define APPLICATION_CALL application_call(__builtin_extract_return_addr(__builtin_return_address(0)))

static void acquired(void *pointer, int kind) {
  if (!pointer) return;
  pthread_mutex_lock(&lock);
  if (enabled) {
    size_t slot;
    for (slot = 0; slot < 4096 && live[slot].pointer; ++slot) {}
    if (slot == 4096) abort();
    live[slot].pointer = pointer;
    live[slot].kind = kind;
    ++allocations[kind];
    if (allocations[kind] - releases[kind] > peaks[kind]) peaks[kind] = allocations[kind] - releases[kind];
  }
  pthread_mutex_unlock(&lock);
}

static void released(void *pointer, int kind) {
  pthread_mutex_lock(&lock);
  for (size_t slot = 0; slot < 4096; ++slot) {
    if (live[slot].pointer == pointer && live[slot].kind == kind) {
      live[slot].pointer = NULL;
      ++releases[kind];
      break;
    }
  }
  pthread_mutex_unlock(&lock);
}

int rdkafka_hs_test_admin_begin(void) {
  int outstanding = 0;
  pthread_mutex_lock(&lock);
  for (int kind = 0; kind < KINDS; ++kind)
    outstanding += allocations[kind] - releases[kind];
  if (!outstanding) {
    memset(allocations, 0, sizeof allocations);
    memset(releases, 0, sizeof releases);
    memset(peaks, 0, sizeof peaks);
    free(last_key); free(last_value); last_key = last_value = NULL;
    enabled = 1;
    fail_config = block_config = config_entered = submissions = config_calls = response_mode = timeout_fault = timeout_reads = 0;
  }
  pthread_mutex_unlock(&lock);
  return outstanding;
}

void rdkafka_hs_test_admin_end(void) {
  pthread_mutex_lock(&lock);
  enabled = fail_config = block_config = response_mode = timeout_fault = 0;
  free(last_key); free(last_value); last_key = last_value = NULL;
  pthread_cond_broadcast(&resumed);
  pthread_mutex_unlock(&lock);
}

int rdkafka_hs_test_admin_count(int kind, int field) {
  int result;
  pthread_mutex_lock(&lock);
  result = field == 0 ? allocations[kind] : field == 1 ? releases[kind] : field == 3 ? peaks[kind] : allocations[kind] - releases[kind];
  pthread_mutex_unlock(&lock);
  return result;
}

void rdkafka_hs_test_admin_fail_config(int flag) {
  pthread_mutex_lock(&lock); fail_config = flag; pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_admin_block_config(int flag) {
  pthread_mutex_lock(&lock);
  block_config = flag;
  if (!flag) pthread_cond_broadcast(&resumed);
  pthread_mutex_unlock(&lock);
}
int rdkafka_hs_test_admin_config_entered(void) {
  int result;
  pthread_mutex_lock(&lock); result = config_entered; pthread_mutex_unlock(&lock);
  return result;
}

rd_kafka_NewTopic_t *rd_kafka_NewTopic_new(const char *name, int partitions, int replication, char *error, size_t size) {
  REAL(rd_kafka_NewTopic_new);
  rd_kafka_NewTopic_t *result = real(name, partitions, replication, error, size);
  if (APPLICATION_CALL) acquired(result, NEW_TOPIC);
  return result;
}
void rd_kafka_NewTopic_destroy(rd_kafka_NewTopic_t *topic) {
  REAL(rd_kafka_NewTopic_destroy);
  if (APPLICATION_CALL) released(topic, NEW_TOPIC); real(topic);
}
rd_kafka_resp_err_t rd_kafka_NewTopic_set_config(rd_kafka_NewTopic_t *topic, const char *name, const char *value) {
  REAL(rd_kafka_NewTopic_set_config);
  int reject;
  if (!APPLICATION_CALL) return real(topic, name, value);
  pthread_mutex_lock(&lock);
  if (!enabled) { pthread_mutex_unlock(&lock); return real(topic, name, value); }
  free(last_key); free(last_value);
  last_key = strdup(name ? name : "(null)");
  last_value = strdup(value ? value : "(null)");
  if (!last_key || !last_value) abort();
  config_entered = 1;
  while (block_config) pthread_cond_wait(&resumed, &lock);
  ++config_calls;
  reject = fail_config > 0 && config_calls >= fail_config;
  pthread_mutex_unlock(&lock);
  return reject ? RD_KAFKA_RESP_ERR__INVALID_ARG : real(topic, name, value);
}
rd_kafka_DeleteTopic_t *rd_kafka_DeleteTopic_new(const char *name) {
  REAL(rd_kafka_DeleteTopic_new);
  rd_kafka_DeleteTopic_t *result = real(name);
  if (APPLICATION_CALL) acquired(result, DELETE_TOPIC); return result;
}
void rd_kafka_DeleteTopic_destroy(rd_kafka_DeleteTopic_t *topic) {
  REAL(rd_kafka_DeleteTopic_destroy);
  if (APPLICATION_CALL) released(topic, DELETE_TOPIC); real(topic);
}
/* Share queue interception with callback lifetime fixtures, so each ordinary
 * native call still has exactly one interposer and one real dispatch. */
void rdkafka_hs_callbacks_queue_created(rd_kafka_t *, rd_kafka_queue_t *);
void rdkafka_hs_callbacks_queue_destroying(rd_kafka_queue_t *);

rd_kafka_queue_t *rd_kafka_queue_new(rd_kafka_t *client) {
  REAL(rd_kafka_queue_new);
  rd_kafka_queue_t *result = real(client);
  if (APPLICATION_CALL) {
    acquired(result, QUEUE);
    rdkafka_hs_callbacks_queue_created(client, result);
  } return result;
}
void rd_kafka_queue_destroy(rd_kafka_queue_t *queue) {
  REAL(rd_kafka_queue_destroy);
  if (APPLICATION_CALL) {
    released(queue, QUEUE);
    rdkafka_hs_callbacks_queue_destroying(queue);
  } real(queue);
}
rd_kafka_AdminOptions_t *rd_kafka_AdminOptions_new(rd_kafka_t *client, rd_kafka_admin_op_t operation) {
  REAL(rd_kafka_AdminOptions_new);
  rd_kafka_AdminOptions_t *result = real(client, operation);
  if (APPLICATION_CALL) acquired(result, OPTIONS); return result;
}
void rd_kafka_AdminOptions_destroy(rd_kafka_AdminOptions_t *options) {
  REAL(rd_kafka_AdminOptions_destroy);
  if (APPLICATION_CALL) released(options, OPTIONS); real(options);
}
rd_kafka_event_t *rd_kafka_queue_poll(rd_kafka_queue_t *queue, int timeout) {
  REAL(rd_kafka_queue_poll);
  int application = APPLICATION_CALL;
  int mode = 0;
  pthread_mutex_lock(&lock); if (application) mode = response_mode; pthread_mutex_unlock(&lock);
  if (mode == 5) {
    struct timespec delay = {0, 150000000};
    while (nanosleep(&delay, &delay) && errno == EINTR) {}
  }
  rd_kafka_event_t *result = real(queue, timeout);
  if (application) acquired(result, EVENT);
  if (mode == 4 && result) {
    rd_kafka_event_destroy(result);
    return NULL;
  }
  return result;
}
void rd_kafka_event_destroy(rd_kafka_event_t *event) {
  REAL(rd_kafka_event_destroy);
  if (APPLICATION_CALL) released(event, EVENT); real(event);
}

/* Deliberately retain one real native request to prove the counter sees a
 * leak. The control's caller must release it; it never submits to a broker. */
rd_kafka_NewTopic_t *rdkafka_hs_test_admin_positive_control(void) {
  char error[512];
  return rd_kafka_NewTopic_new("ownership-positive-control", 1, 1, error, sizeof error);
}
void rdkafka_hs_test_admin_release_control(rd_kafka_NewTopic_t *topic) {
  rd_kafka_NewTopic_destroy(topic);
}

int rdkafka_hs_test_admin_submissions(void) {
  int result;
  pthread_mutex_lock(&lock); result = submissions; pthread_mutex_unlock(&lock);
  return result;
}
void rd_kafka_CreateTopics(rd_kafka_t *client, rd_kafka_NewTopic_t **topics, size_t count, const rd_kafka_AdminOptions_t *options, rd_kafka_queue_t *queue) {
  REAL(rd_kafka_CreateTopics);
  pthread_mutex_lock(&lock); if (enabled && APPLICATION_CALL) ++submissions; pthread_mutex_unlock(&lock);
  real(client, topics, count, options, queue);
}

int rdkafka_hs_test_admin_reject_control(rd_kafka_NewTopic_t *topic) {
  return rd_kafka_NewTopic_set_config(topic, "cleanup.policy", "compact");
}

int rdkafka_hs_test_admin_configuration_calls(void) {
  int result;
  pthread_mutex_lock(&lock); result = config_calls; pthread_mutex_unlock(&lock);
  return result;
}
void rdkafka_hs_test_admin_response_mode(int mode) {
  pthread_mutex_lock(&lock); response_mode = mode; pthread_mutex_unlock(&lock);
}
static int current_response_mode(void) {
  int mode;
  pthread_mutex_lock(&lock); mode = response_mode; pthread_mutex_unlock(&lock);
  return mode;
}
const rd_kafka_CreateTopics_result_t *rd_kafka_event_CreateTopics_result(rd_kafka_event_t *event) {
  REAL(rd_kafka_event_CreateTopics_result);
  if (APPLICATION_CALL && current_response_mode() == 1) return NULL;
  return real(event);
}
const rd_kafka_topic_result_t **rd_kafka_CreateTopics_result_topics(const rd_kafka_CreateTopics_result_t *result, size_t *count) {
  REAL(rd_kafka_CreateTopics_result_topics);
  if (APPLICATION_CALL && current_response_mode() == 2) { *count = 0; return NULL; }
  return real(result, count);
}
const char *rd_kafka_topic_result_name(const rd_kafka_topic_result_t *result) {
  REAL(rd_kafka_topic_result_name);
  if (APPLICATION_CALL && current_response_mode() == 3) return "unrelated-result";
  return real(result);
}

const char *rdkafka_hs_test_admin_last_key(void) { return last_key ? last_key : ""; }
const char *rdkafka_hs_test_admin_last_value(void) { return last_value ? last_value : ""; }

const rd_kafka_DeleteTopics_result_t *rd_kafka_event_DeleteTopics_result(rd_kafka_event_t *event) {
  REAL(rd_kafka_event_DeleteTopics_result);
  if (APPLICATION_CALL && current_response_mode() == 1) return NULL;
  return real(event);
}
const rd_kafka_topic_result_t **rd_kafka_DeleteTopics_result_topics(const rd_kafka_DeleteTopics_result_t *result, size_t *count) {
  REAL(rd_kafka_DeleteTopics_result_topics);
  if (APPLICATION_CALL && current_response_mode() == 2) { *count = 0; return NULL; }
  return real(result, count);
}

void rdkafka_hs_test_admin_timeout_fault(int mode) {
  pthread_mutex_lock(&lock); timeout_fault = mode; pthread_mutex_unlock(&lock);
}
int rdkafka_hs_test_admin_timeout_reads(void) {
  int reads;
  pthread_mutex_lock(&lock); reads = timeout_reads; pthread_mutex_unlock(&lock);
  return reads;
}
rd_kafka_conf_res_t rd_kafka_conf_get(const rd_kafka_conf_t *conf, const char *name, char *destination, size_t *size) {
  REAL(rd_kafka_conf_get);
  int mode = 0;
  if (APPLICATION_CALL && strcmp(name, "socket.timeout.ms") == 0) {
    pthread_mutex_lock(&lock);
    if (enabled) { ++timeout_reads; mode = timeout_fault; }
    pthread_mutex_unlock(&lock);
  }
  if (mode == 1) return RD_KAFKA_CONF_UNKNOWN;
  if (mode == 6) {
    if (destination && *size) destination[0] = 0;
    *size = 100;
    return RD_KAFKA_CONF_OK;
  }
  if (mode) {
    const char *value = mode == 2 ? "not-an-integer" : mode == 3 ? "0" : mode == 4 ? "-1" : "2147483648";
    size_t capacity = *size;
    *size = strlen(value) + 1;
    if (destination && capacity) snprintf(destination, capacity, "%s", value);
    return RD_KAFKA_CONF_OK;
  }
  return real(conf, name, destination, size);
}
