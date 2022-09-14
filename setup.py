#!/usr/bin/env python

from setuptools import setup

setup(name = 'target-parquet',
      version = '0.0.1',
      description = 'Singer.io target for writing into parquet files',
      author = 'Stitch',
      url = 'https://singer.io',
      classifiers = ['Programming Language :: Python :: 3 :: Only'],
      py_modules = ['target_parquet'],
       install_requires=[
          'singer-python>=5.0.12',
          'pandas==1.4.3',
          'pyarrow==6.0.1'
       ],

      entry_points='''
          [console_scripts]
          target-parquet=target_parquet:main
      ''',
)
