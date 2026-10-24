/* Test-only observations of real native metadata/group-list ownership. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define REAL(name) __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr, "metadata: %s\n", dlerror()); abort(); }
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t condition = PTHREAD_COND_INITIALIZER;
static int enabled, mode, entered, released, stats[4];
static const struct rd_kafka_metadata *metadata, *control_metadata;
static const struct rd_kafka_group_list *groups;
static const char *states[] = {"PreparingRebalance", "AwaitingSync", "CompletingRebalance", "Stable", "Dead", "Empty", "FutureState"};
void rdkafka_hs_test_metadata_begin(int value) {
  pthread_mutex_lock(&lock); enabled = 1; mode = value; entered = released = 0;
  metadata = control_metadata = NULL; groups = NULL;
  for (int i = 0; i < 4; ++i) stats[i] = 0;
  pthread_mutex_unlock(&lock);
}
int rdkafka_hs_test_metadata_stat(int index) {
  pthread_mutex_lock(&lock); int result = index >= 0 && index < 4 ? stats[index] : -1;
  pthread_mutex_unlock(&lock); return result;
}
int rdkafka_hs_test_metadata_entered(void) {
  pthread_mutex_lock(&lock); int result = entered;
  pthread_mutex_unlock(&lock); return result;
}
void rdkafka_hs_test_metadata_release(void) {
  pthread_mutex_lock(&lock); released = 1;
  pthread_cond_broadcast(&condition); pthread_mutex_unlock(&lock);
}
static void boundary(int kind) {
  if (mode == kind) {
    entered = 1;
    while (!released) pthread_cond_wait(&condition, &lock);
  }
}
rd_kafka_resp_err_t rd_kafka_metadata(rd_kafka_t *rk, int all_topics, rd_kafka_topic_t *only_topic,
                                     const struct rd_kafka_metadata **result, int timeout_ms) {
  REAL(rd_kafka_metadata);
  rd_kafka_resp_err_t error = real(rk, all_topics, only_topic, result, timeout_ms);
  pthread_mutex_lock(&lock);
  if (enabled && error == RD_KAFKA_RESP_ERR_NO_ERROR) {
    metadata = *result; stats[0]++; boundary(1);
  }
  pthread_mutex_unlock(&lock); return error;
}
void rd_kafka_metadata_destroy(const struct rd_kafka_metadata *value) {
  REAL(rd_kafka_metadata_destroy);
  pthread_mutex_lock(&lock);
  if (enabled && value == metadata) { stats[1]++; metadata = NULL; }
  pthread_mutex_unlock(&lock); real(value);
}
rd_kafka_resp_err_t rd_kafka_list_groups(rd_kafka_t *rk, const char *group,
                                       const struct rd_kafka_group_list **result, int timeout_ms) {
  REAL(rd_kafka_list_groups);
  rd_kafka_resp_err_t error = real(rk, group, result, timeout_ms);
  pthread_mutex_lock(&lock);
  if (enabled && error == RD_KAFKA_RESP_ERR_NO_ERROR) {
    groups = *result; stats[2]++;
    if (mode >= 10 && mode <= 16) {
      if (groups->group_cnt != 1) abort();
      struct rd_kafka_group_info *info = &groups->groups[0];
      free(info->state); info->state = strdup(states[mode - 10]);
    }
    boundary(2);
  }
  pthread_mutex_unlock(&lock); return error;
}
void rd_kafka_group_list_destroy(const struct rd_kafka_group_list *value) {
  REAL(rd_kafka_group_list_destroy);
  pthread_mutex_lock(&lock);
  if (enabled && value == groups) { stats[3]++; groups = NULL; }
  pthread_mutex_unlock(&lock); real(value);
}
void rdkafka_hs_test_metadata_control(const char *brokers) {
  rd_kafka_conf_t *conf = rd_kafka_conf_new(); char error[512];
  if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, error, sizeof(error)) != RD_KAFKA_CONF_OK) abort();
  rd_kafka_t *client = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
  if (!client) abort();
  const struct rd_kafka_metadata *value;
  if (rd_kafka_metadata(client, 1, NULL, &value, 5000) != RD_KAFKA_RESP_ERR_NO_ERROR) abort();
  control_metadata = value; rd_kafka_destroy(client);
}
void rdkafka_hs_test_metadata_end(void) {
  REAL(rd_kafka_metadata_destroy);
  pthread_mutex_lock(&lock);
  const struct rd_kafka_metadata *control = control_metadata;
  enabled = 0; released = 1; metadata = control_metadata = NULL; groups = NULL;
  pthread_cond_broadcast(&condition); pthread_mutex_unlock(&lock);
  if (control) real(control);
  /* Ordinary results have Haskell ForeignPtr owners: never compete with their
   * finalizers. A failing pre-owner cancellation probe terminates its process
   * with the demonstrated native allocation still outstanding. */
}
