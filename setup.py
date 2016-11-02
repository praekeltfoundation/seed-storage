from setuptools import setup, find_packages


setup(
    name="seed.xylem",
    version='0.0.4',
    url='http://github.com/praekeltfoundation/seed-xylem',
    license='MIT',
    description="A distributed service for managing container databases and shared storage",
    author='Colin Alston',
    author_email='colin@praekelt.com',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Twisted',
        'rhumba',
    ],
    extras_require={
        'postgres': [
            'psycopg2cffi',
            'cryptography',
            'pycrypto',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: System :: Distributed Computing',
    ],
)
