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
                  'moeazpy = moeazpy.__main__:cli',
            ]
      },
)
