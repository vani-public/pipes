from setuptools import setup, find_packages


test_requirements = [
    'pytest',
    'pytest-cov',
    'mock',
    'requests==2.22.0',
    'bravado==10.4.1',
    'redis==3.3.7',
    'python-memcached==1.59',
    'datadog==0.29.3'
]

setup(
    name='pypipes',
    description='Pipeline processing framework',
    long_description='Infrastructure independent task execution framework based on logical task pipelines.',
    url='https://github.com/vani-public/pipes',
    author='Victor Anisimov',
    author_email='victor.anisimov@gmail.com',

    packages=find_packages(exclude=['tests/', 'examples/', 'docs/']),
    namespace_packages=['pypipes'],
    include_package_data=False,
    zip_safe=True,
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.7',
    ],
    setup_requires=[
        'setuptools_scm',
    ],
    tests_require=test_requirements,
    extras_require={
        'gevent': ['gevent==1.4.0'],
        'celery': ['celery==4.3.0'],
        'swagger': ['bravado==10.4.1'],
        'api': ['requests>=2.22.0'],
        'redis': ['redis==3.3.7'],
        'memcached': ['python-memcached==1.59'],
        'crypto': ['pycrypto==2.6.1'],
        'datadog': ['datadog==0.29.3'],
        'tests': test_requirements
    },
    use_scm_version={'root': '.', 'relative_to': __file__}
)
