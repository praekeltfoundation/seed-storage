from setuptools import setup, find_packages


setup(
    name="seedstore",
    version='0.0.1',
    url='http://github.com/praekeltfoundation/seed-storage',
    license='MIT',
    description="A storage control system",
    author='Colin Alston',
    author_email='colin@praekelt.com',
    include_package_data=True,
    install_requires=[
        'Twisted',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: System :: Distributed Computing',
    ],
)
