distmqtt
======

``distmqtt`` is a command-line script for running a MQTT 3.1.1 broker.

Usage
-----

``distmqtt`` usage :
::

  distmqtt --version
  distmqtt (-h | --help)
  distmqtt [-c <config_file> ] [-d]


Options
-------

--version           DistMQTT version information
-h, --help          Display ``distmqtt_sub`` usage help
-c                  Set the YAML configuration file to read and pass to the client runtime.


Configuration
-------------

If ``-c`` argument is given, ``distmqtt`` will read specific MQTT settings for the given configuration file. This file must be a valid `YAML`_ file which may contains the following configuration elements :

* ``listeners`` : network bindings configuration list
* ``timeout-disconnect-delay`` : client disconnect timeout after keep-alive timeout
* ``auth`` : authentication configuration

Without the ``-c`` argument, the broker will run with the following default configuration:

.. code-block:: yaml

    listeners:
        default:
            type: tcp
            bind: 0.0.0.0:1883
    sys_interval: 20
    auth:
        allow-anonymous: true
    plugins:
        - auth_file
        - auth_anonymous

Using this configuration, ``distmqtt`` will start a broker :

* listening on TCP port 1883 on all network interfaces.
* Publishing ``$SYS``_ update messages every ``20`` seconds.
* Allowing anonymous login, and password file bases authentication.

.. _YAML: http://yaml.org/


Configuration example
---------------------

.. code-block:: yaml

    distkv:
        server:
            host: '127.0.0.1'
            port: 27586
        topic: [test, mqtt]
        base: [test, dist]
        transparent:             
          - transp
          - also/transp

    listeners:
        default:
            max-connections: 50000
            type: tcp
        my-tcp-1:
            bind: 127.0.0.1:1883
        my-tcp-ssl-1:
            bind: 127.0.0.1:8883
            ssl: on
            cafile: /some/cafile
            capath: /some/folder
            certfile: /some/certfile
            keyfile: /some/key
        my-ws-1:
            bind: 0.0.0.0:8080
            type: ws
    timeout-disconnect-delay: 2
    plugins:
    - auth_anonymous
    auth:
        allow-anonymous: true
        password-file: /some/passwd_file

This configuration example shows the use of every parameter.

The ``distkv`` section controls routing of messages via DistKV.

* The ``server`` subsection mimics DistKV's ``connect`` section.
  See the distKV documentation for details.

* The ``base`` list tells DistMQTT which DistKV subtree to use for
  retained messages. Any changes to this subtree will be broadcast to
  DistMQTT's clients.

* Non-retained messages whose topic starts with one of the topics in the
  ``transparent`` list are exchanged across MQTT as they are, except that
  the ``base`` topic is prepended when sending / stripped when receiving.
  Use a list with an empty list in it to transfer everything that way::

    transparent: [[]]

* Any other messages are encapsulated using the topic in ``topic``.
  This topic is not prepended with ``base`` and must not have ``base`` as a
  prefix.


The example ``listeners`` section defines 3 bindings :

* ``my-tcp-1`` : a unsecured TCP listener on port 1883 allowing ``1000`` clients connections simultaneously
* ``my-tcp-ssl-1`` : a secured TCP listener on port 8883 allowing ``50000`` clients connections simultaneously
* ``my-ws-1`` : a unsecured websocket listener on port 8080 allowing ``50000`` clients connections simultaneously

Authentication allows anonymous logins and password file based authentication. Password files are required to be text files containing user name and password in the form ::

    username:password

where ``password`` should be hashed. Use the ``mkpasswd -m sha-512`` command to encode a passphrase. Password file example::

    # Test user with 'test' password encrypted with sha-512
    test:$6$l4zQEHEcowc1Pnv4$HHrh8xnsZoLItQ8BmpFHM4r6q5UqK3DnXp2GaTm5zp5buQ7NheY3Xt9f6godVKbEtA.hOC7IEDwnok3pbAOip.
