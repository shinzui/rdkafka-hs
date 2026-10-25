/* Test executable only: observe the actual client used by produceva and
 * pause at the enqueue ownership boundary. Ordinary calls go to librdkafka. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

static pthread_mutex_t producer_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t producer_resume = PTHREAD_COND_INITIALIZER;
static int producer_enabled, producer_mode, producer_entered, producer_released;
static const char *property_names[] = {
  "client.id", "message.timeout.ms", "compression.codec", "acks", "queue.buffering.max.messages", "log_level"
};
#define PROPERTY_COUNT (sizeof property_names / sizeof property_names[0])
static char *property_values[PROPERTY_COUNT];

static void clear_properties(void) {
  for (size_t i = 0; i < PROPERTY_COUNT; ++i) {
    free(property_values[i]);
    property_values[i] = NULL;
  }
}
void rdkafka_hs_test_producer_begin(int mode) {
  pthread_mutex_lock(&producer_lock);
  clear_properties();
  producer_enabled = 1;
  producer_mode = mode;
  producer_entered = producer_released = 0;
  pthread_mutex_unlock(&producer_lock);
}
void rdkafka_hs_test_producer_end(void) {
  pthread_mutex_lock(&producer_lock);
  producer_enabled = 0;
  producer_released = 1;
  clear_properties();
  pthread_cond_broadcast(&producer_resume);
  pthread_mutex_unlock(&producer_lock);
}
void rdkafka_hs_test_producer_release(void) {
  pthread_mutex_lock(&producer_lock);
  producer_released = 1;
  pthread_cond_broadcast(&producer_resume);
  pthread_mutex_unlock(&producer_lock);
}
int rdkafka_hs_test_producer_entered(void) {
  int result;
  pthread_mutex_lock(&producer_lock);
  result = producer_entered;
  pthread_mutex_unlock(&producer_lock);
  return result;
}
const char *rdkafka_hs_test_producer_property(int index) {
  return index >= 0 && (size_t)index < PROPERTY_COUNT ? property_values[index] : NULL;
}

rd_kafka_error_t *rd_kafka_produceva(rd_kafka_t *client, const rd_kafka_vu_t *values, size_t count) {
  __typeof__(&rd_kafka_produceva) real = (__typeof__(&rd_kafka_produceva))dlsym(RTLD_NEXT, "rd_kafka_produceva");
  if (!real) { fprintf(stderr, "producer ownership: %s\n", dlerror()); abort(); }
  pthread_mutex_lock(&producer_lock);
  int enabled = producer_enabled, mode = producer_mode;
  if (enabled) {
    clear_properties();
    const rd_kafka_conf_t *conf = rd_kafka_conf(client);
    rd_kafka_topic_conf_t *topic_conf = rd_kafka_default_topic_conf_dup(client);
    for (size_t i = 0; i < PROPERTY_COUNT; ++i) {
      size_t length = 0;
      int topic_property = rd_kafka_conf_get(conf, property_names[i], NULL, &length) != RD_KAFKA_CONF_OK;
      /* An untouched client need not have an allocated default-topic config.
       * The native duplication API supplies the same native topic defaults. */
      if (topic_property && rd_kafka_topic_conf_get(topic_conf, property_names[i], NULL, &length) != RD_KAFKA_CONF_OK) continue;
      property_values[i] = calloc(length + 1, 1);
      if (!property_values[i]) abort();
      size_t capacity = length;
      rd_kafka_conf_res_t result = topic_property
        ? rd_kafka_topic_conf_get(topic_conf, property_names[i], property_values[i], &capacity)
        : rd_kafka_conf_get(conf, property_names[i], property_values[i], &capacity);
      if (result != RD_KAFKA_CONF_OK || capacity > length) abort();
    }
    rd_kafka_topic_conf_destroy(topic_conf);
  }
  if (enabled && mode == 1) {
    /* The Haskell StablePtr exists; the native client has not accepted it. */
    producer_entered = 1;
    while (!producer_released) pthread_cond_wait(&producer_resume, &producer_lock);
    pthread_mutex_unlock(&producer_lock);
    return rd_kafka_error_new(RD_KAFKA_RESP_ERR__QUEUE_FULL, "%s", "controlled enqueue rejection");
  }
  if (enabled && mode >= 3 && mode <= 5) {
    /* These calls reject before native transfer. Exercise a decoder failure,
     * a contradictory non-null success-code object, and a valid UNKNOWN code
     * with an unrelated errno. No message or delivery report is created. */
    rd_kafka_resp_err_t code = mode == 3 ? (rd_kafka_resp_err_t)32767
      : mode == 4 ? RD_KAFKA_RESP_ERR_NO_ERROR : RD_KAFKA_RESP_ERR_UNKNOWN;
    producer_entered = 1;
    pthread_mutex_unlock(&producer_lock);
    rd_kafka_error_t *error = rd_kafka_error_new(code, "%s", "controlled native error object");
    if (mode == 5) errno = EINVAL;
    return error;
  }
  pthread_mutex_unlock(&producer_lock);
  rd_kafka_error_t *result = real(client, values, count);
  const char *safety_probe = getenv("RDKAFKA_TEST_PRODUCER_SAFETY_PROBE");
  if (!result && safety_probe)
    fprintf(stderr, "producer safety probe: native enqueue accepted (%s)\n", safety_probe);
  if (enabled && mode == 2 && !result) {
    /* Native ownership has passed, but the caller has not seen the result.
     * Another Haskell thread may now serve the real delivery report. */
    pthread_mutex_lock(&producer_lock);
    producer_entered = 1;
    while (!producer_released) pthread_cond_wait(&producer_resume, &producer_lock);
    pthread_mutex_unlock(&producer_lock);
  }
  return result;
}

/* Native protocol fixture: delay only Produce responses and observe actual
 * received Produce requests before testing in-flight purge behavior. This
 * does not replace the real-broker configuration and record round trips. */
struct producer_mock { rd_kafka_t *anchor; rd_kafka_mock_cluster_t *cluster; };
struct producer_mock *rdkafka_hs_test_producer_mock_new(void) {
  char error[512];
  struct producer_mock *mock = calloc(1, sizeof *mock);
  if (!mock) abort();
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  rd_kafka_conf_set(conf, "log_level", "0", error, sizeof error);
  mock->anchor = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof error);
  if (!mock->anchor) { rd_kafka_conf_destroy(conf); free(mock); return NULL; }
  mock->cluster = rd_kafka_mock_cluster_new(mock->anchor, 1);
  if (!mock->cluster) { rd_kafka_destroy(mock->anchor); free(mock); return NULL; }
  if (rd_kafka_mock_topic_create(mock->cluster, "producer-inflight", 1, 1)) abort();
  rd_kafka_mock_start_request_tracking(mock->cluster);
  return mock;
}
void rdkafka_hs_test_producer_mock_destroy(struct producer_mock *mock) {
  rd_kafka_mock_cluster_destroy(mock->cluster);
  rd_kafka_destroy(mock->anchor);
  free(mock);
}
const char *rdkafka_hs_test_producer_mock_brokers(struct producer_mock *mock) {
  return rd_kafka_mock_cluster_bootstraps(mock->cluster);
}
int rdkafka_hs_test_producer_mock_delay(struct producer_mock *mock, int milliseconds) {
  return rd_kafka_mock_broker_push_request_error_rtts(mock->cluster, 1, 0, 1,
                                                     RD_KAFKA_RESP_ERR_NO_ERROR, milliseconds);
}
int rdkafka_hs_test_producer_mock_received(struct producer_mock *mock) {
  size_t count;
  int result = 0;
  rd_kafka_mock_request_t **requests = rd_kafka_mock_get_requests(mock->cluster, &count);
  for (size_t i = 0; i < count; ++i)
    if (rd_kafka_mock_request_api_key(requests[i]) == 0) ++result;
  rd_kafka_mock_request_destroy_array(requests, count);
  return result;
}
