#ifndef ASYNKAF_QUEUE_H
#define ASYNKAF_QUEUE_H

#include <pthread.h>
#include <librdkafka/rdkafka.h>

/**
 * @brief A node in the message queue's linked list.
 *
 * Each node contains a single Kafka message and a pointer to the next node.
 */
typedef struct MessageNode {
    rd_kafka_message_t *message; // Pointer to the Kafka message.
    struct MessageNode *next;    // Pointer to the next node in the queue.
} MessageNode;

/**
 * @brief A thread-safe queue for Kafka messages.
 *
 * This structure implements a FIFO (First-In, First-Out) queue using a
 * singly linked list. It uses a mutex and a condition variable to ensure
 * thread-safe operations.
 */
typedef struct {
    MessageNode *head;        // Pointer to the first message in the queue.
    MessageNode *tail;        // Pointer to the last message in the queue.
    int size;                 // The current number of messages in the queue.
    pthread_mutex_t lock;     // Mutex to protect access to the queue.
    pthread_cond_t cond;      // Condition variable to signal when the queue is not empty.
} MessageQueue;

/**
 * @brief Initializes a message queue.
 * @param queue A pointer to the MessageQueue to initialize.
 */
void message_queue_init(MessageQueue *queue);

/**
 * @brief Pushes a new message onto the tail of the queue.
 * @param queue A pointer to the MessageQueue.
 * @param message The Kafka message to add.
 */
void message_queue_push(MessageQueue *queue, rd_kafka_message_t *message);

/**
 * @brief Pops a message from the head of the queue.
 *
 * If the queue is empty, this function will block until a message is available.
 * @param queue A pointer to the MessageQueue.
 * @return The Kafka message from the front of the queue.
 */
rd_kafka_message_t *message_queue_pop(MessageQueue *queue);

/**
 * @brief Destroys a message queue, freeing all associated resources.
 * @param queue A pointer to the MessageQueue to destroy.
 */
void message_queue_destroy(MessageQueue *queue);

#endif
