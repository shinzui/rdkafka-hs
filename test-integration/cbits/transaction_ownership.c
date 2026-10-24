/* Integration executable only. Count returned native transaction errors, not
 * background allocations. Ordinary calls reach the native library. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t resume = PTHREAD_COND_INITIALIZER;
static int enabled, mode, released;
static int64_t counts[13];
static struct { rd_kafka_error_t *pointer; int live; } errors[4096];
static rd_kafka_t *last_client;

#define REAL(name) __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr, "transaction seam: %s\n", dlerror()); abort(); }

void rdkafka_hs_test_tx_begin(int value) {
  pthread_mutex_lock(&lock);
  enabled = 1; mode = value; released = 0; last_client = NULL;
  for (int i = 0; i < 13; ++i) counts[i] = 0;
  for (int i = 0; i < 4096; ++i) { errors[i].pointer = NULL; errors[i].live = 0; }
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_tx_release(void) {
  pthread_mutex_lock(&lock);
  released = 1; pthread_cond_broadcast(&resume);
  pthread_mutex_unlock(&lock);
}
void rdkafka_hs_test_tx_end(void) {
  REAL(rd_kafka_error_destroy);
  pthread_mutex_lock(&lock);
  enabled = 0; released = 1; last_client = NULL;
  /* Clean up the deliberately leaking control, or a failed assertion. */
  for (int i = 0; i < 4096; ++i) {
    if (errors[i].live) real(errors[i].pointer);
    errors[i].pointer = NULL; errors[i].live = 0;
  }
  pthread_cond_broadcast(&resume);
  pthread_mutex_unlock(&lock);
}
int64_t rdkafka_hs_test_tx_stat(int index) {
  pthread_mutex_lock(&lock);
  int64_t result = index >= 0 && index < 13 ? counts[index] : -1;
  pthread_mutex_unlock(&lock);
  return result;
}
static rd_kafka_error_t *track(rd_kafka_t *client, rd_kafka_error_t *error) {
  pthread_mutex_lock(&lock);
  if (enabled) {
    last_client = client; counts[10]++;
    if (error) {
      int i;
      for (i = 0; i < 4096 && errors[i].live; ++i) {}
      if (i == 4096) abort();
      errors[i].pointer = error; errors[i].live = 1; counts[0]++;
    }
  }
  pthread_mutex_unlock(&lock);
  return error;
}
/* Called by the one shared error-destroy interposer in seek_faults.c. */
void rdkafka_hs_tx_error_destroyed(rd_kafka_error_t *error) {
  pthread_mutex_lock(&lock);
  if (enabled && error) for (int i = 0; i < 4096; ++i) {
    if (errors[i].pointer == error && errors[i].live) {
      errors[i].live = 0; counts[1]++; break;
    }
  }
  pthread_mutex_unlock(&lock);
}
static void reading(const rd_kafka_error_t *error, int index) {
  pthread_mutex_lock(&lock);
  int known = 0, live = 0;
  if (enabled && error) for (int i = 0; i < 4096; ++i) {
    if (errors[i].pointer == error) { known = 1; live |= errors[i].live; }
  }
  if (known) {
    counts[index]++;
    if (!live) counts[8]++;
    if (mode == 18 + index && counts[9] == 0) {
      counts[9]++;
      while (!released) pthread_cond_wait(&resume, &lock);
    }
  }
  pthread_mutex_unlock(&lock);
}
static int current_mode(void) {
  pthread_mutex_lock(&lock);
  int value = enabled ? mode : 0;
  pthread_mutex_unlock(&lock);
  return value;
}
static rd_kafka_error_t *fixture(int value) {
  rd_kafka_error_t *error = rd_kafka_error_new(value == 200 ? (rd_kafka_resp_err_t)32767 : RD_KAFKA_RESP_ERR__STATE,
                                             "%s", "native transaction fixture");
  /* These actual native setters are private diagnostic symbols. No opaque
   * structure layout is copied. Ordinary/compatibility paths do not use them. */
  const char *names[] = {"rd_kafka_error_set_fatal", "rd_kafka_error_set_retriable", "rd_kafka_error_set_txn_requires_abort"};
  for (int bit = 0; bit < 3; ++bit) if (value >= 100 && value < 108 && ((value - 100) & (1 << bit))) {
    void (*set)(rd_kafka_error_t *) = (void (*)(rd_kafka_error_t *))dlsym(RTLD_NEXT, names[bit]);
    if (!set) { fprintf(stderr, "transaction native fixture: %s\n", dlerror()); abort(); }
    set(error);
  }
  return error;
}
static int overridden(int value) { return (value >= 100 && value < 108) || value == 200 || value == 201; }

rd_kafka_error_t *rd_kafka_init_transactions(rd_kafka_t *client, int timeout) {
  REAL(rd_kafka_init_transactions); int value = current_mode();
  return track(client, overridden(value) ? (value == 201 ? NULL : fixture(value)) : real(client, timeout));
}
rd_kafka_error_t *rd_kafka_begin_transaction(rd_kafka_t *client) {
  REAL(rd_kafka_begin_transaction); int value = current_mode();
  return track(client, overridden(value) ? (value == 201 ? NULL : fixture(value)) : real(client));
}
rd_kafka_error_t *rd_kafka_abort_transaction(rd_kafka_t *client, int timeout) {
  REAL(rd_kafka_abort_transaction); int value = current_mode();
  return track(client, overridden(value) ? (value == 201 ? NULL : fixture(value)) : real(client, timeout));
}
rd_kafka_error_t *rd_kafka_commit_transaction(rd_kafka_t *client, int timeout) {
  REAL(rd_kafka_commit_transaction); int value = current_mode();
  return track(client, overridden(value) ? (value == 201 ? NULL : fixture(value)) : real(client, timeout));
}
rd_kafka_error_t *rd_kafka_send_offsets_to_transaction(rd_kafka_t *client,
    const rd_kafka_topic_partition_list_t *offsets,
    const rd_kafka_consumer_group_metadata_t *metadata, int timeout) {
  REAL(rd_kafka_send_offsets_to_transaction); int value = current_mode();
  pthread_mutex_lock(&lock);
  if (enabled && offsets && offsets->cnt == 1) counts[12] = offsets->elems[0].offset;
  pthread_mutex_unlock(&lock);
  return track(client, overridden(value) ? (value == 201 ? NULL : fixture(value)) : real(client, offsets, metadata, timeout));
}
rd_kafka_resp_err_t rd_kafka_error_code(const rd_kafka_error_t *error) {
  REAL(rd_kafka_error_code); reading(error, 2); return real(error);
}
int rd_kafka_error_is_fatal(const rd_kafka_error_t *error) {
  REAL(rd_kafka_error_is_fatal); reading(error, 3); return real(error);
}
int rd_kafka_error_is_retriable(const rd_kafka_error_t *error) {
  REAL(rd_kafka_error_is_retriable); reading(error, 4); return real(error);
}
int rd_kafka_error_txn_requires_abort(const rd_kafka_error_t *error) {
  REAL(rd_kafka_error_txn_requires_abort); reading(error, 5); return real(error);
}
rd_kafka_consumer_group_metadata_t *rd_kafka_consumer_group_metadata(rd_kafka_t *client) {
  REAL(rd_kafka_consumer_group_metadata);
  rd_kafka_consumer_group_metadata_t *result = real(client);
  pthread_mutex_lock(&lock); if (enabled && result) counts[6]++; pthread_mutex_unlock(&lock);
  return result;
}
void rd_kafka_consumer_group_metadata_destroy(rd_kafka_consumer_group_metadata_t *metadata) {
  REAL(rd_kafka_consumer_group_metadata_destroy);
  pthread_mutex_lock(&lock); if (enabled && metadata) counts[7]++; pthread_mutex_unlock(&lock);
  real(metadata);
}

/* Reproduce the inherited decode-without-destroy sequence on the same live,
 * nontransactional producer. The enclosing fixture later releases all three. */
void rdkafka_hs_test_tx_leak_control(void) {
  pthread_mutex_lock(&lock); rd_kafka_t *client = last_client; pthread_mutex_unlock(&lock);
  if (!client) abort();
  { REAL(rd_kafka_init_transactions); rd_kafka_error_code(track(client, real(client, 0))); }
  { REAL(rd_kafka_begin_transaction); rd_kafka_error_code(track(client, real(client))); }
  { REAL(rd_kafka_abort_transaction); rd_kafka_error_code(track(client, real(client, 0))); }
}
