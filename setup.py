#!/usr/bin/env python

from setuptools import setup

setup(name='moeazpy',
      version='0.1',
      description='Multi-objective evolutionary algorithm framework using ZeroMQ',
      author='James E Tomlinson',
      author_email='tomo.bbe@gmail.com',
      packages=['moeazpy', ],
      entry_points={
            'console_scripts': [
                  'pop_server = moeazpy.__main__:start_population_server',
                  'prop_server = moeazpy.__main__:start_propagator_server',
                  'broker_server = moeazpy.__main__:start_broker_server',
                  'worker_server = moeazpy.__main__:start_worker_server',
                  'local_algorithm = moeazpy.__main__:start_local_algorithm',
            ]
      },
)
