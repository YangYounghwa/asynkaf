#include "queue.h"
#include <stdlib.h>

/**
 * @brief Initializes a MessageQueue.
 *
 * Sets the head and tail to NULL, size to 0, and initializes the
 * mutex and condition variable for thread safety.
 *
 * @param queue A pointer to the MessageQueue to be initialized.
 */
void message_queue_init(MessageQueue *queue) {
    queue->head = NULL;
    queue->tail = NULL;
    queue->size = 0;
    pthread_mutex_init(&queue->lock, NULL);
    pthread_cond_init(&queue->cond, NULL);
}

/**
 * @brief Pushes a Kafka message onto the queue.
 *
 * This operation is thread-safe. It appends a new message to the tail of the
 * linked list and signals any waiting consumer threads.
 *
 * @param queue A pointer to the MessageQueue.
 * @param message The Kafka message to be added.
 */
void message_queue_push(MessageQueue *queue, rd_kafka_message_t *message) {
    // Create a new node for the message.
    MessageNode *new_node = (MessageNode *)malloc(sizeof(MessageNode));
    new_node->message = message;
    new_node->next = NULL;

    // Lock the queue for safe modification.
    pthread_mutex_lock(&queue->lock);

    // Append the new node to the tail of the list.
    if (queue->tail) {
        queue->tail->next = new_node;
    } else {
        queue->head = new_node;
    }
    queue->tail = new_node;
    queue->size++;

    // Signal that a new item is available.
    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->lock);
}

/**
 * @brief Pops a Kafka message from the queue.
 *
 * This operation is thread-safe. If the queue is empty, the calling thread
 * will block until a message is pushed by another thread.
 *
 * @param queue A pointer to the MessageQueue.
 * @return The `rd_kafka_message_t` from the head of the queue.
 */
rd_kafka_message_t *message_queue_pop(MessageQueue *queue) {
    pthread_mutex_lock(&queue->lock);
    // Wait for a message to become available if the queue is empty.
    while (queue->head == NULL) {
        pthread_cond_wait(&queue->cond, &queue->lock);
    }

    // Remove the node from the head of the list.
    MessageNode *node = queue->head;
    rd_kafka_message_t *message = node->message;
    queue->head = node->next;
    if (queue->head == NULL) {
        queue->tail = NULL;
    }
    queue->size--;
    free(node);

    pthread_mutex_unlock(&queue->lock);
    return message;
}

/**
 * @brief Destroys the message queue and frees all resources.
 *
 * It iterates through any remaining messages in the queue, destroys them,
 * and frees the corresponding nodes. It also destroys the mutex and
 * condition variable.
 *
 * @param queue A pointer to the MessageQueue to be destroyed.
 */
void message_queue_destroy(MessageQueue *queue) {
    pthread_mutex_lock(&queue->lock);
    // Free all remaining nodes and their messages.
    while (queue->head) {
        MessageNode *node = queue->head;
        queue->head = node->next;
        rd_kafka_message_destroy(node->message);
        free(node);
    }
    pthread_mutex_unlock(&queue->lock);

    // Destroy synchronization primitives.
    pthread_mutex_destroy(&queue->lock);
    pthread_cond_destroy(&queue->cond);
}
