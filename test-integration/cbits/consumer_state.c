/* Integration executable only. Invoke the installed native callback with an
 * owned native partition list and controlled, actually allocated error objects.
 * Mode zero only observes native commit arguments without changing results.
 * Real cooperative group handoffs remain separate broker scenarios. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

typedef void (*rebalance_callback)(rd_kafka_t *, rd_kafka_resp_err_t,
                                    rd_kafka_topic_partition_list_t *, void *);
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static int enabled, mode, stats[7];
static rd_kafka_t *client;
static int commit_calls, commit_async;
static rebalance_callback installed;
static rd_kafka_error_t *owned_error;

#define REAL(name) __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr, "consumer state: %s\n", dlerror()); abort(); }

void rdkafka_hs_test_state_begin(int value) {
  pthread_mutex_lock(&lock);
  if (owned_error) abort();
  enabled = 1; mode = value; client = NULL; installed = NULL;
  commit_calls = 0; commit_async = -1;
  for (int i = 0; i < 7; ++i) stats[i] = 0;
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_state_end(void) {
  REAL(rd_kafka_error_destroy);
  pthread_mutex_lock(&lock);
  rd_kafka_error_t *retained = owned_error;
  enabled = 0; client = NULL; installed = NULL; owned_error = NULL;
  pthread_mutex_unlock(&lock);
  if (retained) real(retained);
}
int rdkafka_hs_test_state_stat(int index) {
  pthread_mutex_lock(&lock);
  int value = index >= 0 && index < 7 ? stats[index] : -1;
  pthread_mutex_unlock(&lock);
  return value;
}
void rdkafka_hs_state_client_created(rd_kafka_type_t type, rd_kafka_t *created) {
  pthread_mutex_lock(&lock);
  if (enabled && type == RD_KAFKA_CONSUMER && created) client = created;
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_state_error_destroyed(rd_kafka_error_t *error) {
  pthread_mutex_lock(&lock);
  if (enabled && error && error == owned_error) { stats[1]++; owned_error = NULL; }
  pthread_mutex_unlock(&lock);
}
void rd_kafka_conf_set_rebalance_cb(rd_kafka_conf_t *conf, rebalance_callback callback) {
  REAL(rd_kafka_conf_set_rebalance_cb);
  real(conf, callback);
  pthread_mutex_lock(&lock);
  if (enabled) installed = callback;
  pthread_mutex_unlock(&lock);
}
const char *rd_kafka_rebalance_protocol(rd_kafka_t *rk) {
  REAL(rd_kafka_rebalance_protocol);
  pthread_mutex_lock(&lock);
  int controlled = enabled && rk == client && mode >= 1 && mode <= 6;
  int eager = mode >= 5;
  if (controlled) stats[4]++;
  pthread_mutex_unlock(&lock);
  return controlled ? (eager ? "EAGER" : "COOPERATIVE") : real(rk);
}
static rd_kafka_error_t *failure_locked(int index) {
  if (owned_error) abort();
  stats[index]++;
  owned_error = rd_kafka_error_new(mode == 3 || mode == 4 ? (rd_kafka_resp_err_t)32767 : RD_KAFKA_RESP_ERR__STATE,
                                   "%s", "controlled rebalance failure");
  stats[0]++;
  return owned_error;
}
rd_kafka_error_t *rd_kafka_incremental_assign(rd_kafka_t *rk, const rd_kafka_topic_partition_list_t *partitions) {
  REAL(rd_kafka_incremental_assign);
  pthread_mutex_lock(&lock);
  if (enabled && rk == client && mode >= 1 && mode <= 6) {
    rd_kafka_error_t *error = failure_locked(2);
    pthread_mutex_unlock(&lock); return error;
  }
  pthread_mutex_unlock(&lock);
  return real(rk, partitions);
}
rd_kafka_error_t *rd_kafka_incremental_unassign(rd_kafka_t *rk, const rd_kafka_topic_partition_list_t *partitions) {
  REAL(rd_kafka_incremental_unassign);
  pthread_mutex_lock(&lock);
  if (enabled && rk == client && mode >= 1 && mode <= 6) {
    rd_kafka_error_t *error = failure_locked(3);
    pthread_mutex_unlock(&lock); return error;
  }
  pthread_mutex_unlock(&lock);
  return real(rk, partitions);
}
rd_kafka_resp_err_t rd_kafka_assign(rd_kafka_t *rk, const rd_kafka_topic_partition_list_t *partitions) {
  REAL(rd_kafka_assign);
  pthread_mutex_lock(&lock);
  int observed = enabled && rk == client && mode >= 5 && mode <= 8;
  int controlled = observed && (mode == 5 || mode == 6 || (mode == 8 && !partitions));
  if (observed) stats[5]++;
  pthread_mutex_unlock(&lock);
  return controlled ? RD_KAFKA_RESP_ERR__STATE : real(rk, partitions);
}
void rdkafka_hs_test_state_trigger(const char *topic) {
  pthread_mutex_lock(&lock);
  rd_kafka_t *target = client; rebalance_callback callback = installed;
  int revoke = mode == 2 || mode == 4 || mode == 6;
  int unexpected = mode == 7 || mode == 8;
  if (!enabled || !target || !callback) abort();
  stats[6]++;
  pthread_mutex_unlock(&lock);
  rd_kafka_topic_partition_list_t *partitions = rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(partitions, topic, 0);
  callback(target, unexpected ? RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED : revoke ? RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS : RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, partitions, NULL);
  rd_kafka_topic_partition_list_destroy(partitions);
}
void rdkafka_hs_test_state_trigger_cooperative(const char *topic, int revoke) {
  pthread_mutex_lock(&lock);
  rd_kafka_t *target = client; rebalance_callback callback = installed;
  if (!enabled || mode != 9 || !target || !callback) abort();
  pthread_mutex_unlock(&lock);
  /* No protocol or action is replaced: the client must have negotiated
   * COOPERATIVE and the installed bridge calls real incremental APIs. */
  rd_kafka_topic_partition_list_t *partitions = rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_t *partition = rd_kafka_topic_partition_list_add(partitions, topic, 0);
  if (!revoke) partition->offset = RD_KAFKA_OFFSET_END;
  callback(target, revoke ? RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS : RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, partitions, NULL);
  rd_kafka_topic_partition_list_destroy(partitions);
}
void rdkafka_hs_test_state_leak_control(void) {
  pthread_mutex_lock(&lock);
  failure_locked(2);
  pthread_mutex_unlock(&lock);
}

int rdkafka_hs_test_state_commit_count(void) {
  pthread_mutex_lock(&lock); int value = commit_calls; pthread_mutex_unlock(&lock); return value;
}
int rdkafka_hs_test_state_commit_async(void) {
  pthread_mutex_lock(&lock); int value = commit_async; pthread_mutex_unlock(&lock); return value;
}
rd_kafka_resp_err_t rd_kafka_commit(rd_kafka_t *rk, const rd_kafka_topic_partition_list_t *offsets, int async) {
  REAL(rd_kafka_commit);
  pthread_mutex_lock(&lock);
  if (enabled && mode == 0 && rk == client) { commit_calls++; commit_async = async; }
  pthread_mutex_unlock(&lock);
  return real(rk, offsets, async);
}

/* A sync queue handle is a temporary wrapper. Observe its length within the
 * call that borrows it; retain only scalar observations after that call. */
static int queue_tracking, queue_calls, notification_length;
static int queue_application_call(void *caller) {
  Dl_info image;
  if (!dladdr(caller, &image) || !image.dli_fname) abort();
  const char *name = strrchr(image.dli_fname, '/');
  name = name ? name + 1 : image.dli_fname;
  return strncmp(name, "librdkafka.", 11) != 0;
}
void rdkafka_hs_test_queue_begin(void) {
  pthread_mutex_lock(&lock);
  if (queue_tracking) abort();
  queue_tracking = 1; queue_calls = 0; notification_length = -1;
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_queue_end(void) {
  pthread_mutex_lock(&lock);
  queue_tracking = 0; notification_length = -1;
  pthread_mutex_unlock(&lock);
}
void rd_kafka_queue_io_event_enable(rd_kafka_queue_t *queue, int fd, const void *payload, size_t size) {
  REAL(rd_kafka_queue_io_event_enable);
  real(queue, fd, payload, size);
  if (!queue_application_call(__builtin_return_address(0))) return;
  pthread_mutex_lock(&lock);
  if (queue_tracking) { notification_length = (int)rd_kafka_queue_length(queue); queue_calls++; }
  pthread_mutex_unlock(&lock);
}
int rdkafka_hs_test_queue_stat(int index) {
  pthread_mutex_lock(&lock);
  int result = index == 0 ? queue_calls :
    index == 1 && queue_tracking ? notification_length : -1;
  pthread_mutex_unlock(&lock);
  return result;
}
