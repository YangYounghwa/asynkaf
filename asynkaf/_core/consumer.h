#ifndef ASYNKAF_CONSUMER_H
#define ASYNKAF_CONSUMER_H

#include <Python.h>

/**
 * @brief External declaration of the ConsumerType object.
 *
 * This allows other C files (like _core.c) to access the ConsumerType definition
 * from consumer.c without needing to include its full implementation.
 */
extern PyTypeObject ConsumerType;

/**
 * @brief Declaration of the function to create a new Consumer object.
 *
 * This function is exposed to Python through the module's method table.
 *
 * @param self The module object (unused in this context).
 * @param args The arguments passed from Python.
 * @return A new Consumer PyObject, or NULL on failure.
 */
PyObject* create_consumer(PyObject* self, PyObject* args);

#endif
