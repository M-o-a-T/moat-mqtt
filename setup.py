# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from setuptools import setup, find_packages

setup(
    name="moat-mqtt",
    use_scm_version={"version_scheme": "guess-next-dev", "local_scheme": "dirty-tag"},
    setup_requires=["setuptools_scm", "pytest-runner"],
    tests_require=["pytest", "pylint >= 2.7"],
    description="MQTT client/broker using anyio and distkv",
    author="Matthias Urlichs",
    author_email="matthias@urlichs.de",
    url="https://github.com/M-o-a-T/moat-mqtt",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    platforms="all",
    python_requires=">=3.6",
    install_requires=[
        "transitions",
        "asyncwebsockets >= 0.8",
        "passlib",
        "docopt",
        "pyyaml",
        "anyio >= 3",
        "attrs >= 19",
        "simplejson",
        "msgpack",
    ],
    extras_require={
        "distkv": ["distkv >= 0.58.8"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Communications",
        "Topic :: Internet",
    ],
    entry_points={
        "moat.mqtt.test.plugins": [
            #           'test_plugin = tests.plugins.test_manager:SimpleTestPlugin',
            #           'event_plugin = tests.plugins.test_manager:EventTestPlugin',
            #           'packet_logger_plugin = moat.mqtt.plugins.logging:PacketLoggerPlugin',
        ],
        "moat.mqtt.broker.plugins": [
            # 'event_logger_plugin = moat.mqtt.plugins.logging:EventLoggerPlugin',
            #           'packet_logger_plugin = moat.mqtt.plugins.logging:PacketLoggerPlugin',
            "auth_anonymous = moat.mqtt.plugins.authentication:AnonymousAuthPlugin",
            "auth_file = moat.mqtt.plugins.authentication:FileAuthPlugin",
            "topic_taboo = moat.mqtt.plugins.topic_checking:TopicTabooPlugin",
            "topic_acl = moat.mqtt.plugins.topic_checking:TopicAccessControlListPlugin",
            "broker_sys = moat.mqtt.plugins.sys.broker:BrokerSysPlugin",
        ],
        "moat.mqtt.client.plugins": [
            #           'packet_logger_plugin = moat.mqtt.plugins.logging:PacketLoggerPlugin',
        ],
        "console_scripts": [
            "moat_mqtt_broker = moat.mqtt.scripts.broker_script:main",
            "moat_mqtt_pub = moat.mqtt.scripts.pub_script:main",
            "moat_mqtt_sub = moat.mqtt.scripts.sub_script:main",
        ],
    },
)
