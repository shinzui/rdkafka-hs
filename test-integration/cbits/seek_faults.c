/* Test executable only. Ordinary calls always reach the real native client.
 * Fault modes expose native result boundaries; the committed barrier lets an
 * actual assignment change occur between lookup and seek. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t resume = PTHREAD_COND_INITIALIZER;
static int mode, entered, released, seek_calls, created, destroyed;
static rd_kafka_error_t *owned_error;
extern void rdkafka_hs_tx_error_destroyed(rd_kafka_error_t *error);
extern void rdkafka_hs_state_error_destroyed(rd_kafka_error_t *error);
extern void rdkafka_hs_test_lists_assignment(rd_kafka_topic_partition_list_t *list);

#define REAL(name) __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr, "seek seam: %s\n", dlerror()); abort(); }

void rdkafka_hs_test_seek_begin(int value) {
  pthread_mutex_lock(&lock);
  if (owned_error) abort();
  mode = value; entered = released = seek_calls = created = destroyed = 0;
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_seek_release(void) {
  pthread_mutex_lock(&lock);
  released = 1;
  pthread_cond_broadcast(&resume);
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_seek_end(void) {
  pthread_mutex_lock(&lock);
  mode = 0; released = 1;
  pthread_cond_broadcast(&resume);
  pthread_mutex_unlock(&lock);
}
int rdkafka_hs_test_seek_stat(int which) {
  int value;
  pthread_mutex_lock(&lock);
  value = which == 0 ? entered : which == 1 ? seek_calls : which == 2 ? created : destroyed;
  pthread_mutex_unlock(&lock);
  return value;
}

rd_kafka_resp_err_t rd_kafka_committed(rd_kafka_t *client, rd_kafka_topic_partition_list_t *parts, int timeout) {
  REAL(rd_kafka_committed);
  pthread_mutex_lock(&lock);
  int current = mode;
  pthread_mutex_unlock(&lock);
  if (current == 11) return RD_KAFKA_RESP_ERR__TIMED_OUT;
  rd_kafka_resp_err_t result = real(client, parts, timeout);
  if (current == 10) {
    pthread_mutex_lock(&lock);
    entered = 1;
    while (!released) pthread_cond_wait(&resume, &lock);
    pthread_mutex_unlock(&lock);
  }
  return result;
}

rd_kafka_resp_err_t rd_kafka_assignment(rd_kafka_t *client, rd_kafka_topic_partition_list_t **parts) {
  REAL(rd_kafka_assignment);
  pthread_mutex_lock(&lock);
  int current = mode;
  pthread_mutex_unlock(&lock);
  if (current == 12) { *parts = NULL; return RD_KAFKA_RESP_ERR__STATE; }
  rd_kafka_resp_err_t result = real(client, parts);
  if (result == RD_KAFKA_RESP_ERR_NO_ERROR) rdkafka_hs_test_lists_assignment(*parts);
  return result;
}

rd_kafka_error_t *rd_kafka_seek_partitions(rd_kafka_t *client, rd_kafka_topic_partition_list_t *parts, int timeout) {
  REAL(rd_kafka_seek_partitions);
  pthread_mutex_lock(&lock);
  int current = mode;
  if (current) seek_calls++;
  if (current >= 3 && current <= 7) {
    for (int i = 0; i < parts->cnt; ++i) {
      parts->elems[i].err = current == 5
        ? (parts->elems[i].partition == 42 ? RD_KAFKA_RESP_ERR__INVALID_ARG : RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
        : (i == 0 || current == 6 ? RD_KAFKA_RESP_ERR__IN_PROGRESS : RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION);
    }
    rd_kafka_error_t *result = NULL;
    if (current == 3 || current == 7) {
      result = rd_kafka_error_new(current == 3 ? RD_KAFKA_RESP_ERR__TIMED_OUT : (rd_kafka_resp_err_t)32767,
                                   "%s", "controlled seek top-level failure");
      owned_error = result;
      created++;
    }
    pthread_mutex_unlock(&lock);
    return result;
  }
  pthread_mutex_unlock(&lock);
  return real(client, parts, timeout);
}

void rd_kafka_error_destroy(rd_kafka_error_t *error) {
  REAL(rd_kafka_error_destroy);
  rdkafka_hs_tx_error_destroyed(error);
  rdkafka_hs_state_error_destroyed(error);
  pthread_mutex_lock(&lock);
  if (error && error == owned_error) { destroyed++; owned_error = NULL; }
  pthread_mutex_unlock(&lock);
  real(error);
}
