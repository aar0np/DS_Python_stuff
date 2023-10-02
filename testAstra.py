from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys

clientID=sys.argv[1]
secret=sys.argv[2]
secureBundleLocation=sys.argv[3]

cloud_config= {
        'secure_connect_bundle': secureBundleLocation
}
auth_provider = PlainTextAuthProvider(clientID, secret)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

#row = session.execute("select release_version from system.local").one()
row = session.execute("SELECT toTimestamp(now()) - 1h FROM system.local;").one()

if row:
    print("address: " + row[0])
    print("host id: " + str(row[1]))
    print("data center: " + row[2])
    print("rack: " + row[3])
    print("tokens: " + str(len(row[4])))
else:
    print("An error occurred.")
