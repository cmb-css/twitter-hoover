#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name='twitter-hoover',
    version='0.0.1',
    author='Telmo Menezes et al.',
    author_email='telmo@telmomenezes.net',
    description='Collect data from filtered Twitter streams.',
    url='https://github.com/cmb-css/twitter-hoover',
    license='MIT',
    keywords=['Twitter', 'Data Science', 'Data Collection', 'Crawler',
              'Extract Data', 'Computational Social Science',
              'Computational Sociology'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Topic :: Sociology'
    ],
    python_requires='>=3.4',
    packages=find_packages(),
    install_requires=[
        'twython',
    ],
    entry_points='''
        [console_scripts]
        hoover=hoover.__main__:cli
    '''
)
