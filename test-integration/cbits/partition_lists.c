/* Integration executable only: count application-owned lists, including
 * native-returned outputs. Internal native copies remain unobserved. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
static pthread_mutex_t list_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t list_resume = PTHREAD_COND_INITIALIZER;
static int tracking, fault, allocated, destroyed, live, entered, released, queue_attempts;
static rd_kafka_topic_partition_list_t *lists[64], *control;
#define REAL(name) __typeof__(&name) real = (__typeof__(&name))dlsym(RTLD_NEXT, #name); \
  if (!real) { fprintf(stderr,"partition lists: %s\n",dlerror()); abort(); }
static int application(void *caller) {
  Dl_info info;
  if (!dladdr(caller,&info) || !info.dli_fname) abort();
  const char *name=strrchr(info.dli_fname,'/');
  return strncmp(name ? name+1 : info.dli_fname,"librdkafka.",11)!=0;
}
static void observe(rd_kafka_topic_partition_list_t *list, int output) {
  pthread_mutex_lock(&list_lock);
  if (tracking && list) {
    int slot;
    for(slot=0;slot<64 && lists[slot];slot++) if(lists[slot]==list) abort();
    if(slot==64) abort();
    lists[slot]=list; allocated++; live++;
    if ((fault==2 && output==1) || (fault==3 && output==2)) {
      if(list->cnt<1 || strlen(list->elems[0].topic)<2) abort();
      list->elems[0].topic[0]=(char)0xff; list->elems[0].topic[1]=0;
    }
    if ((fault==4 && output==0) || (fault==5 && output==1)) {
      entered=1;
      while(!released) pthread_cond_wait(&list_resume,&list_lock);
    }
  }
  pthread_mutex_unlock(&list_lock);
}
void rdkafka_hs_test_lists_begin(int value) {
  pthread_mutex_lock(&list_lock);
  if(tracking || control) abort();
  memset(lists,0,sizeof(lists)); fault=value; tracking=1;
  allocated=destroyed=live=entered=released=queue_attempts=0;
  pthread_mutex_unlock(&list_lock);
}
void rdkafka_hs_test_lists_release(void) {
  pthread_mutex_lock(&list_lock); released=1;
  pthread_cond_broadcast(&list_resume); pthread_mutex_unlock(&list_lock);
}
void rdkafka_hs_test_lists_end(void) {
  rdkafka_hs_test_lists_release();
  pthread_mutex_lock(&list_lock); tracking=0;
  memset(lists,0,sizeof(lists)); pthread_mutex_unlock(&list_lock);
}
int rdkafka_hs_test_lists_stat(int index) {
  pthread_mutex_lock(&list_lock);
  int value=index==0?allocated:index==1?destroyed:index==2?live:index==4?queue_attempts:entered;
  pthread_mutex_unlock(&list_lock); return value;
}
rd_kafka_topic_partition_list_t *rd_kafka_topic_partition_list_new(int size) {
  REAL(rd_kafka_topic_partition_list_new);
  rd_kafka_topic_partition_list_t *list=real(size);
  if(application(__builtin_return_address(0))) observe(list,0);
  return list;
}
void rd_kafka_topic_partition_list_destroy(rd_kafka_topic_partition_list_t *list) {
  REAL(rd_kafka_topic_partition_list_destroy);
  pthread_mutex_lock(&list_lock);
  if(tracking) for(int i=0;i<64;i++) if(lists[i]==list) {
    lists[i]=NULL; destroyed++; live--; break;
  }
  pthread_mutex_unlock(&list_lock);
  /* Destruction can release native partition references and invoke logging. */
  real(list);
}
void rdkafka_hs_test_lists_assignment(rd_kafka_topic_partition_list_t *list) { observe(list,1); }
rd_kafka_resp_err_t rd_kafka_subscription(rd_kafka_t *client, rd_kafka_topic_partition_list_t **list) {
  REAL(rd_kafka_subscription);
  pthread_mutex_lock(&list_lock);
  int selected = tracking && fault == 7 && application(__builtin_return_address(0));
  pthread_mutex_unlock(&list_lock);
  if (selected) { *list = NULL; return RD_KAFKA_RESP_ERR__UNKNOWN_GROUP; }
  rd_kafka_resp_err_t result=real(client,list);
  if(result==RD_KAFKA_RESP_ERR_NO_ERROR && application(__builtin_return_address(0))) observe(*list,2);
  return result;
}
rd_kafka_queue_t *rd_kafka_queue_get_partition(rd_kafka_t *client, const char *topic, int32_t partition) {
  REAL(rd_kafka_queue_get_partition);
  pthread_mutex_lock(&list_lock);
  int selected = tracking && fault == 8 && application(__builtin_return_address(0));
  if (selected) queue_attempts++;
  pthread_mutex_unlock(&list_lock);
  return selected ? NULL : real(client, topic, partition);
}
rd_kafka_resp_err_t rd_kafka_position(rd_kafka_t *client, rd_kafka_topic_partition_list_t *list) {
  REAL(rd_kafka_position);
  rd_kafka_resp_err_t result = real(client, list);
  pthread_mutex_lock(&list_lock);
  if (tracking && fault == 6 && application(__builtin_return_address(0))) {
    const int64_t offsets[] = {RD_KAFKA_OFFSET_BEGINNING, RD_KAFKA_OFFSET_END,
      RD_KAFKA_OFFSET_STORED, RD_KAFKA_OFFSET_INVALID, -17, 0, INT64_MAX};
    if (list->cnt != 7 || result != RD_KAFKA_RESP_ERR_NO_ERROR) abort();
    for (int i = 0; i < 7; ++i) list->elems[i].offset = offsets[i];
  }
  pthread_mutex_unlock(&list_lock);
  return result;
}
void rdkafka_hs_test_lists_control(int release) {
  if(release) { rd_kafka_topic_partition_list_destroy(control); control=NULL; }
  else { REAL(rd_kafka_topic_partition_list_new); if(control) abort(); control=real(1); observe(control,0); }
}
