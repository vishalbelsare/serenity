Installation
============

If you want to use Serenity as a library in your own projects you can install the latest version
from PyPi with pip as follows::

    $ pip install serenity-trading

But most likely you want to run backtests or even live trading strategies, which requires more
extensive installation process. First, you will want to clone the code locally::

    $ git clone

Database install
----------------

First, install `TimescaleDB <http://timescale.com/>` on your local machine.

Next, load the following files:

* ``sql/serenitydb_install.sql``
* ``sql/serenitydb_schema.sql``
* ``sql/serenitydb_grants.sql``

Plus, optionally (for equity trading support with Sharadar subscription):

* ``sql/sharadar_schema.sql``

Be sure to change the passwords in ``serenitydb_install.sql`` and ``sharadar_schema.sql``!

Kubernetes install
------------------

I highly recommend the `microk8s <https://ubuntu.com/tutorials/install-a-local-kubernetes-with-microk8s#1-overview>`
distribution that comes with Ubuntu, but you should be able to use any Kubernetes installation
with the YAML files that come under the ``kubernetes`` directory. Each file can be installed with::

    $ microk8s.kubectl apply -f $FILE

but you may need to customize them to meet your local needs. One to watch out for:
``database-secret-config.yaml``. You'll need to replace the passwords for postgres,
serenity and sharadar with the base64-encoded versions of the actual passwords
instead of ``********``:

.. code-block:: yaml

    data:
        postgres-password: "********"
        serenity-password: "********"
        sharadar-password: "********"

You can generate an encoded password as follows -- not the "-n" argument is critical::

    $ echo -n $PASSWORD | base64
