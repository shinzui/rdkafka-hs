/* Test executable only: actual polled messages, owned return boundaries and
 * decoder failures. Native calls remain real except declared fault results. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <librdkafka/rdkafka.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t resume = PTHREAD_COND_INITIALIZER;
static int enabled, mode, released, count, stats[9];
static rd_kafka_message_t *messages[8];
static int live[8];
static int32_t original_partition[8];
static int64_t original_offset[8];
static rd_kafka_topic_t *original_topic[8];
static rd_kafka_resp_err_t original_error[8];

#define REAL(name) __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr, "message ownership: %s\n", dlerror()); abort(); }

static int application_call(void *caller) {
  Dl_info image;
  if (!dladdr(caller, &image) || !image.dli_fname) abort();
  const char *name = strrchr(image.dli_fname, '/');
  name = name ? name + 1 : image.dli_fname;
  return strncmp(name, "librdkafka.", 11) != 0;
}
#define APPLICATION_CALL application_call(__builtin_extract_return_addr(__builtin_return_address(0)))

void rdkafka_hs_test_messages_begin(int value) {
  pthread_mutex_lock(&lock);
  enabled = 1; mode = value; released = count = 0;
  memset(stats, 0, sizeof stats); memset(messages, 0, sizeof messages); memset(live, 0, sizeof live);
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_messages_release(void) {
  pthread_mutex_lock(&lock);
  released = 1; pthread_cond_broadcast(&resume);
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_messages_end(void) {
  REAL(rd_kafka_message_destroy);
  rd_kafka_message_t *retained[8]; int remaining = 0;
  pthread_mutex_lock(&lock);
  for (int i = 0; i < count; ++i) if (live[i]) {
    messages[i]->partition = original_partition[i];
    messages[i]->offset = original_offset[i];
    messages[i]->rkt = original_topic[i];
    messages[i]->err = original_error[i];
    retained[remaining++] = messages[i];
  }
  enabled = 0; released = 1; count = 0;
  memset(messages, 0, sizeof messages); memset(live, 0, sizeof live);
  pthread_cond_broadcast(&resume);
  pthread_mutex_unlock(&lock);
  /* Release the deliberate control, or messages retained by a failing test. */
  for (int i = 0; i < remaining; ++i) real(retained[i]);
}
int rdkafka_hs_test_messages_stat(int index) {
  pthread_mutex_lock(&lock);
  int result = index >= 0 && index < 9 ? stats[index] : -1;
  pthread_mutex_unlock(&lock);
  return result;
}
static void boundary(void) {
  stats[4]++;
  while (!released) pthread_cond_wait(&resume, &lock);
}
static void acquired(rd_kafka_message_t *message) {
  if (count == 8 || !message) abort();
  for (int i = 0; i < count; ++i)
    if (messages[i] == message && !live[i]) { stats[8]++; break; }
  original_partition[count] = message->partition;
  original_offset[count] = message->offset;
  original_topic[count] = message->rkt;
  original_error[count] = message->err;
  messages[count] = message; live[count++] = 1; stats[1]++;
}
/* Native allocators may reuse a released address in the same observation
 * scope. Only the live allocation owns the current message's saved fields. */
static int live_index(const rd_kafka_message_t *message) {
  for (int i = count - 1; i >= 0; --i)
    if (messages[i] == message && live[i]) return i;
  return -1;
}
static long long now_ms(void) {
  struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
  return (long long)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

ssize_t rd_kafka_consume_batch_queue(rd_kafka_queue_t *queue, int timeout,
                                    rd_kafka_message_t **output, size_t capacity) {
  REAL(rd_kafka_consume_batch_queue);
  pthread_mutex_lock(&lock);
  int tracking = enabled && APPLICATION_CALL;
  int current = mode;
  if (tracking) stats[0]++;
  pthread_mutex_unlock(&lock);
  if (!tracking) return real(queue, timeout, output, capacity);
  if (current == 4) { errno = ETIMEDOUT; return -1; }

  /* Ownership faults require three actual records at one native boundary.
   * Collect from the real queue up to that bound; ordinary observation mode
   * 20 preserves the requested timeout/capacity exactly. */
  ssize_t received = 0;
  if (current <= 6) {
    size_t target = capacity < 3 ? capacity : 3;
    long long deadline = now_ms() + 3000;
    while ((size_t)received < target && now_ms() < deadline) {
      int remaining = (int)(deadline - now_ms());
      int wait = timeout < 0 || timeout > remaining ? remaining : timeout;
      ssize_t next = real(queue, wait, output + received, target - (size_t)received);
      if (next < 0) {
        int saved = errno;
        __typeof__(&rd_kafka_message_destroy) destroy = (__typeof__(&rd_kafka_message_destroy))dlsym(RTLD_NEXT, "rd_kafka_message_destroy");
        if (!destroy) abort();
        for (ssize_t i = 0; i < received; ++i) destroy(output[i]);
        errno = saved; return next;
      }
      received += next;
    }
  } else received = real(queue, timeout, output, capacity);
  pthread_mutex_lock(&lock);
  for (ssize_t i = 0; i < received; ++i) acquired(output[i]);
  if (current == 2 && received > 0) boundary();
  pthread_mutex_unlock(&lock);
  return received;
}

static rd_kafka_message_t *single(rd_kafka_message_t *message, int app, int queue) {
  pthread_mutex_lock(&lock);
  int selected = (queue ? mode >= 10 && mode <= 12 : mode >= 13 && mode <= 19) || (mode >= 21 && mode <= 25);
  if (enabled && app && selected && message && message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    /* Error frames are outside the data-record counter, but are new native
     * allocations too. A reused dead address must not suppress their release. */
    for (int i = 0; i < count; ++i)
      if (messages[i] == message && !live[i]) messages[i] = NULL;
  }
  if (enabled && app && selected && message && message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    acquired(message);
    /* Native error frames can lack a partition or offset independently.
     * Retain the real allocation and restore its fields before destruction. */
    if (mode == 21 || mode == 23) message->partition = RD_KAFKA_PARTITION_UA;
    if (mode == 21 || mode == 22) message->offset = RD_KAFKA_OFFSET_INVALID;
    if (mode == 24 || mode == 25) {
      /* Only documented public message fields change; restore them before
       * native destruction, so the original allocation/ownership stays real. */
      message->rkt = NULL; message->partition = RD_KAFKA_PARTITION_UA;
      message->offset = RD_KAFKA_OFFSET_INVALID;
      message->err = mode == 24 ? RD_KAFKA_RESP_ERR__BAD_MSG : RD_KAFKA_RESP_ERR__FATAL;
    }
    if (mode == 10 || mode == 13) boundary();
  }
  pthread_mutex_unlock(&lock);
  return message;
}
rd_kafka_message_t *rd_kafka_consumer_poll(rd_kafka_t *client, int timeout) {
  REAL(rd_kafka_consumer_poll);
  int app = APPLICATION_CALL;
  return single(real(client, timeout), app, 0);
}
rd_kafka_message_t *rd_kafka_consume_queue(rd_kafka_queue_t *queue, int timeout) {
  REAL(rd_kafka_consume_queue);
  int app = APPLICATION_CALL;
  return single(real(queue, timeout), app, 1);
}
rd_kafka_resp_err_t rd_kafka_message_headers(const rd_kafka_message_t *message,
                                           rd_kafka_headers_t **headers) {
  REAL(rd_kafka_message_headers);
  extern int rdkafka_hs_test_producer_header_failure(void);
  if (rdkafka_hs_test_producer_header_failure()) { *headers = NULL; return RD_KAFKA_RESP_ERR__BAD_MSG; }
  pthread_mutex_lock(&lock);
  int index = live_index(message);
  if (enabled && index >= 0) {
    stats[3]++;
    if (mode >= 21 && mode <= 23) {
      *headers = NULL;
      pthread_mutex_unlock(&lock);
      return RD_KAFKA_RESP_ERR__BAD_MSG;
    }
    if (index == 1 && mode == 3) boundary();
    if (index == 0 && (mode == 11 || mode == 14)) boundary();
    if ((index == 1 && (mode == 1 || mode == 6)) || (index == 0 && (mode == 12 || mode == 15))) {
      *headers = NULL;
      int current = mode;
      pthread_mutex_unlock(&lock);
      return current == 1 ? (rd_kafka_resp_err_t)32767 : RD_KAFKA_RESP_ERR__BAD_MSG;
    }
  }
  pthread_mutex_unlock(&lock);
  return real(message, headers);
}
int64_t rd_kafka_message_timestamp(const rd_kafka_message_t *message, rd_kafka_timestamp_type_t *type) {
  REAL(rd_kafka_message_timestamp);
  pthread_mutex_lock(&lock);
  int selected = 0;
  for (int i = 0; i < count; ++i) if (messages[i] == message && live[i]) selected = 1;
  int current = enabled && selected ? mode : 0;
  pthread_mutex_unlock(&lock);
  if (current == 17) { if (type) *type = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE; return -1; }
  if (current == 18) { if (type) *type = RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME; return 123456789; }
  if (current == 19) { if (type) *type = RD_KAFKA_TIMESTAMP_CREATE_TIME; return 987654321; }
  return real(message, type);
}
void rd_kafka_message_destroy(rd_kafka_message_t *message) {
  REAL(rd_kafka_message_destroy);
  int skip = 0;
  pthread_mutex_lock(&lock);
  if (enabled && APPLICATION_CALL) {
    int i = live_index(message);
    if (i < 0) {
      /* With no newer live allocation, a known dead address still detects
       * an actual duplicate application release. */
      for (int j = 0; j < count; ++j) if (messages[j] == message) {
        stats[6]++; stats[5]++; skip = 1; break;
      }
    } else {
      stats[6]++;
      if (mode == 5 && i == count - 1) { stats[7]++; skip = 1; }
      else {
        message->partition = original_partition[i];
        message->offset = original_offset[i];
        message->rkt = original_topic[i];
        message->err = original_error[i];
        live[i] = 0; stats[2]++;
      }
    }
  }
  pthread_mutex_unlock(&lock);
  if (!skip) real(message);
}
