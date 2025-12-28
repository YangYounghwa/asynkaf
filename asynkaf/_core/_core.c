#include <Python.h>
#include "consumer.h"

/**
 * @brief Defines the methods available in the `_core` module.
 *
 * This array lists the functions that can be called from Python.
 * Each entry specifies the Python name, the C function pointer,
 * the calling convention, and a docstring.
 */
static PyMethodDef core_methods[] = {
    {"create_consumer", create_consumer, METH_VARARGS, "Create a new Kafka consumer."},
    {NULL, NULL, 0, NULL}  // Sentinel to indicate the end of the method table.
};

/**
 * @brief Defines the `_core` module.
 *
 * This structure contains all the information needed to create the module object.
 * It includes the module's name, docstring, and a reference to its method table.
 */
static struct PyModuleDef core_module = {
    PyModuleDef_HEAD_INIT,
    "_core",                             // Module name
    "Core Kafka client functionality.",  // Module docstring
    -1,                                  // Module state size (-1 for global state)
    core_methods                         // Method table
};

/**
 * @brief Initializes the `_core` module.
 *
 * This function is called by the Python interpreter when the module is imported.
 * It prepares the custom ConsumerType, creates the module, and adds the
 * Consumer type to the module's namespace.
 *
 * @return A new PyObject representing the initialized module, or NULL on failure.
 */
PyMODINIT_FUNC PyInit__core(void) {
    PyObject *m;

    // Finalize the ConsumerType object, preparing it for use.
    if (PyType_Ready(&ConsumerType) < 0)
        return NULL;

    // Create the module object.
    m = PyModule_Create(&core_module);
    if (m == NULL)
        return NULL;

    // Add the Consumer type to the module.
    // Py_INCREF is necessary because PyModule_AddObject steals a reference.
    Py_INCREF(&ConsumerType);
    PyModule_AddObject(m, "Consumer", (PyObject *)&ConsumerType);

    return m;
}
