from setuptools import setup, find_packages


setup(
    name='pipes',
    version='0.1',
    description='Pipeline processing framework',
    url='https://github.com/vani-public/pipes',
    author='Victor Anisimov',
    use_2to3=True,

    packages=find_packages(exclude=['tests', 'tests.*']),
    namespace_packages=['pipes'],
    include_package_data=False,

    install_requires=[
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'mock',
    ],
    extras_require={
        'gevent': ['gevent==1.3.6'],
        'celery': ['celery==4.2.0'],
        'swagger': ['bravado==10.1.0'],
        'api': ['requests>=2.4'],
        'redis': ['redis==2.10.6'],
        'memcached': ['python-memcached==1.59'],
        'crypto': ['pycrypto==2.6.1'],
        'datadog': ['datadog==0.23.0']
    }
)
