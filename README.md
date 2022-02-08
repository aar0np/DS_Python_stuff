# DS_Python_stuff
DataStax-specific things I've worked on with Python

## `cassandra_test_dag.py`
A sample directed acyclic graph (DAG) for Apache Airflow, which implements two simple tasks for Apache Cassandra or DataStax Astra DB.

## `testAstra.py`
A simple Python script which connects to DataStax Astra DB, and displays the `release_version` from the `system.local` table.
```
Usage: python[3] testAstra.py <astraClientID> <astraSecret> <secureBundleLocation>
```
Example:
```
python3 testAstra.py rtFckUZOblahblahblahwWB xwpyKTMeZDIblahblahblahblahblahblahZgSkWQPTlf2CuO ~/local/astraCreds/secure-connect.zip
4.0.0.6816
```

## `testCassandraSSL.py`
A simple Python script wich connects to Apache Cassandra, and displays the `key` from the `system.local` table.
```
Usage: python[3] testCassandraSSL.py <hostname> <username> <password> <cacertfilelocation> <clientkeylocation> <clientcertlocation>
```

Example:
```
python3 testCassandraSSL.py 127.0.0.1 aaron b@c0n ~/local/astraCreds/ca.crt ~/local/astraCreds/key ~/local/astraCreds/cert
local
```
