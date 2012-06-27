from distutils.core import setup
from setuptools import find_packages
setup(
    name = "dseo-jobparse",
    version = "1.0",
    description = "Job data feedfile parsing app.",
    author = "DirectEmployers Foundation",
    author_email = "jmclaughlin@directemployersfoundation.org",
    long_description = open('README.rst', 'r').read(),
    package_data = {
        'jobparse': [
            'tests/factories.py',
            'tests/xmlparse.py',
            'tests/import_jobs.py',
            'tests/dseo_feed_0.no_jobs.xml'
        ]
    },
    packages = [
        'jobparse',
        'jobparse.tests'
    ],
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search'
    ],
    url = 'http://github.com/DirectEmployers/dseo-jobparse'
)
