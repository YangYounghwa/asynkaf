from setuptools import setup, Extension

asynkaf_core = Extension(
    'asynkaf._core',
    sources=[
        'asynkaf/_core/_core.c',
        'asynkaf/_core/consumer.c',
        'asynkaf/_core/queue.c',
    ],
    include_dirs=['/usr/local/include'],
    libraries=['rdkafka'],
    library_dirs=['/usr/local/lib'],
)

setup(
    packages=['asynkaf'],
    ext_modules=[asynkaf_core],
)

