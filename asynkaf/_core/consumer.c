#include <Python.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include "queue.h"
#include "consumer.h"

/**
 * @brief The internal state of a Consumer object.
 *
 * This structure holds all the C-level data for a Kafka consumer instance.
 * It includes the librdkafka handle, a background poller thread, and a
 * thread-safe queue for messages.
 */
typedef struct {
    PyObject_HEAD             // Standard Python object header.
    rd_kafka_t *rk;           // Handle to the librdkafka consumer instance.
    pthread_t poller_thread;  // Identifier for the background polling thread.
    int run_poller;           // Flag to control the lifecycle of the poller thread.
    MessageQueue message_queue; // Thread-safe queue to store fetched messages.
} ConsumerObject;

/**
 * @brief The background thread function for polling Kafka messages.
 *
 * This function runs in a separate thread and continuously polls the Kafka
 * consumer for new messages, pushing them into a thread-safe queue.
 *
 * @param arg A void pointer to the ConsumerObject instance.
 * @return Always returns NULL.
 */
static void *poller_thread_func(void *arg) {
    ConsumerObject *self = (ConsumerObject *)arg;
    while (self->run_poller) {
        // Poll for a message, with a 1-second timeout.
        rd_kafka_message_t *message = rd_kafka_consumer_poll(self->rk, 1000);
        if (message) {
            if (message->err) {
                // On error, simply destroy the message.
                rd_kafka_message_destroy(message);
            } else {
                // On success, push the message onto the queue.
                message_queue_push(&self->message_queue, message);
            }
        }
    }
    return NULL;
}

/**
 * @brief Allocates a new Consumer object.
 *
 * This corresponds to the `__new__` method in Python.
 */
static PyObject *
Consumer_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    ConsumerObject *self;
    self = (ConsumerObject *)type->tp_alloc(type, 0);
    return (PyObject *)self;
}

/**
 * @brief Initializes a Consumer object.
 *
 * Corresponds to the `__init__` method in Python. It sets up the librdkafka
 * consumer, initializes the message queue, and starts the background poller thread.
 *
 * @param self The ConsumerObject to initialize.
 * @param args Python arguments (bootstrap_servers, group_id).
 * @param kwds Python keyword arguments (unused).
 * @return 0 on success, -1 on failure.
 */
static int
Consumer_init(ConsumerObject *self, PyObject *args, PyObject *kwds) {
    char *bootstrap_servers;
    char *group_id;
    char errstr[512];

    // Parse Python arguments.
    if (!PyArg_ParseTuple(args, "ss", &bootstrap_servers, &group_id))
        return -1;
    
    // Create and configure the Kafka client.
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        PyErr_SetString(PyExc_ValueError, errstr);
        return -1;
    }
    
    if (rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        PyErr_SetString(PyExc_ValueError, errstr);
        return -1;
    }
    
    if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        PyErr_SetString(PyExc_ValueError, errstr);
        return -1;
    }

    // Create the consumer instance.
    self->rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!self->rk) {
        PyErr_SetString(PyExc_RuntimeError, errstr);
        return -1;
    }
    
    // Initialize the message queue.
    message_queue_init(&self->message_queue);
    
    // Start the background poller thread.
    self->run_poller = 1;
    pthread_create(&self->poller_thread, NULL, poller_thread_func, self);

    return 0;
}

/**
 * @brief Deallocates a Consumer object.
 *
 * Corresponds to the `__dealloc__` method. It gracefully stops the poller
 * thread, closes and destroys the Kafka consumer, cleans up the message
 * queue, and frees the object's memory.
 */
static void
Consumer_dealloc(ConsumerObject *self) {
    // Signal the poller thread to stop and wait for it to exit.
    self->run_poller = 0;
    pthread_join(self->poller_thread, NULL);

    // Clean up Kafka resources.
    rd_kafka_consumer_close(self->rk);
    rd_kafka_destroy(self->rk);

    // Clean up the message queue.
    message_queue_destroy(&self->message_queue);

    // Free the Python object.
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/**
 * @brief Defines the Python type object for the Consumer.
 *
 * This structure describes how the Consumer type behaves in Python,
 * linking its name, docstring, size, and lifecycle methods (__new__,
 * __init__, __dealloc__) to their C implementations.
 */
PyTypeObject ConsumerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_core.Consumer",
    .tp_doc = "Kafka Consumer",
    .tp_basicsize = sizeof(ConsumerObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = Consumer_new,
    .tp_init = (initproc)Consumer_init,
    .tp_dealloc = (destructor)Consumer_dealloc,
};

/**
 * @brief Factory function to create and initialize a Consumer object from Python.
 *
 * This is the function exposed to Python as `_core.create_consumer`.
 */
PyObject* create_consumer(PyObject* self, PyObject* args)
{
    // Allocate the object.
    ConsumerObject *consumer = PyObject_New(ConsumerObject, &ConsumerType);
    if (!consumer) {
        return NULL;
    }
    // Initialize the object.
    if (Consumer_init(consumer, args, NULL) < 0) {
        Py_DECREF(consumer);
        return NULL;
    }

    return (PyObject*)consumer;
}
