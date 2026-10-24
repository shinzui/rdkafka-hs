/* Compare the Haskell error enum and exception rendering with the linked
 * library's public descriptor table, including its deliberately sparse codes. */
#include <librdkafka/rdkafka.h>
#include <stddef.h>
int rdkafka_hs_test_error_count(void) {
  const struct rd_kafka_err_desc *descriptors; size_t size; int count = 0;
  rd_kafka_get_err_descs(&descriptors, &size);
  for (size_t i = 0; i < size; ++i) if (descriptors[i].name && descriptors[i].desc) count++;
  return count;
}
static const struct rd_kafka_err_desc *at(int index) {
  const struct rd_kafka_err_desc *descriptors; size_t size;
  rd_kafka_get_err_descs(&descriptors, &size);
  for (size_t i = 0; i < size; ++i) if (descriptors[i].name && descriptors[i].desc && index-- == 0) return &descriptors[i];
  return NULL;
}
int rdkafka_hs_test_error_code(int index) { return at(index)->code; }
const char *rdkafka_hs_test_error_name(int index) { return at(index)->name; }
const char *rdkafka_hs_test_error_description(int index) { return at(index)->desc; }
int rdkafka_hs_test_error_sentinel(int index) {
  return index == 0 ? RD_KAFKA_RESP_ERR__BEGIN : index == 1 ? RD_KAFKA_RESP_ERR__END : RD_KAFKA_RESP_ERR_END_ALL;
}
