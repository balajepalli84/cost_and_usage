import oci
import oracledb

# Connect to OCI
config = oci.config.from_file()
identity_client = oci.identity.IdentityClient(config)

dsn ='''(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1521)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=gwtizid6xsmfvaf_u6vfaxcq64nyfskm_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'''
user = "ADMIN"
password = "P@ssword12345"

# Connect to ADB
connection = oracledb.connect(user=user, password=password, dsn=dsn)

# Execute a simple query
with connection.cursor() as cursor:
    cursor.execute("SELECT 1 FROM DUAL")
    result = cursor.fetchone()

print(result)

# Close the connection
connection.close()