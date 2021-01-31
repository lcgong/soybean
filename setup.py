import setuptools
import setuptools.command.test


setuptools.setup(
    name='soybean',
    version='0.0.2',
    license='http://www.apache.org/licenses/LICENSE-2.0',
    description='A tiny message-queue application framwork',
    author='Chenggong Lyu',
    author_email='lcgong@gmail.com',
    url='https://github.com/lcgong/soybean',
    packages=setuptools.find_packages("."),
    install_requires=[
        "rocketmq-client-python>=2.0.0",
        "sqlblock>=0.6.5",
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        'License :: OSI Approved :: Apache Software License',
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Utilities",
    ],
)
