from setuptools import setup, find_packages


setup(
    name='pypipes',
    description='Pipeline processing framework',
    long_description=open('README.md').read(),
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
    tests_require=[
        'pytest',
        'pytest-cov',
        'mock',
        'requests==2.22.0',
        'bravado==10.4.1',
        'redis==3.3.7',
        'python-memcached==1.59',
        'datadog==0.29.3'
    ],
    extras_require={
        'gevent': ['gevent==1.4.0'],
        'celery': ['celery==4.3.0'],
        'swagger': ['bravado==10.4.1'],
        'api': ['requests>=2.22.0'],
        'redis': ['redis==3.3.7'],
        'memcached': ['python-memcached==1.59'],
        'crypto': ['pycrypto==2.6.1'],
        'datadog': ['datadog==0.29.3']
    },
    use_scm_version={'root': '.', 'relative_to': __file__}
)
