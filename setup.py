# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from setuptools import setup, find_packages

setup(
    name="distmqtt",
    use_scm_version={"version_scheme": "guess-next-dev", "local_scheme": "dirty-tag"},
    setup_requires=["setuptools_scm", "pytest-runner"],
    tests_require=["pytest"],
    description="MQTT client/broker using anyio and distkv",
    author="Matthias Urlichs",
    author_email="matthias@urlichs.de",
    url="https://github.com/smurfix/distmqtt",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    platforms="all",
    python_requires=">=3.6",
    install_requires=[
        "transitions",
        "asyncwebsockets",
        "passlib",
        "docopt",
        "pyyaml",
        "anyio",
        "attrs",
        "distkv >= 0.20",
        "simplejson",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Communications",
        "Topic :: Internet",
    ],
    entry_points={
        "distmqtt.test.plugins": [
            #           'test_plugin = tests.plugins.test_manager:SimpleTestPlugin',
            #           'event_plugin = tests.plugins.test_manager:EventTestPlugin',
            #           'packet_logger_plugin = distmqtt.plugins.logging:PacketLoggerPlugin',
        ],
        "distmqtt.broker.plugins": [
            # 'event_logger_plugin = distmqtt.plugins.logging:EventLoggerPlugin',
            #           'packet_logger_plugin = distmqtt.plugins.logging:PacketLoggerPlugin',
            "auth_anonymous = distmqtt.plugins.authentication:AnonymousAuthPlugin",
            "auth_file = distmqtt.plugins.authentication:FileAuthPlugin",
            "topic_taboo = distmqtt.plugins.topic_checking:TopicTabooPlugin",
            "topic_acl = distmqtt.plugins.topic_checking:TopicAccessControlListPlugin",
            "broker_sys = distmqtt.plugins.sys.broker:BrokerSysPlugin",
        ],
        "distmqtt.client.plugins": [
            #           'packet_logger_plugin = distmqtt.plugins.logging:PacketLoggerPlugin',
        ],
        "console_scripts": [
            "distmqtt = distmqtt.scripts.broker_script:main",
            "distmqtt_pub = distmqtt.scripts.pub_script:main",
            "distmqtt_sub = distmqtt.scripts.sub_script:main",
        ],
    },
)
