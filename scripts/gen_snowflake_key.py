"""Create a key pair for Snowflake key-pair authentication and write the complete account setup SQL.
Snowflake blocks single-factor password sign-ins, so dbt and the loader authenticate with this key instead.
The private key stays in snowflake_key.p8 (gitignored). Run the generated SQL once in a Snowsight worksheet."""
import os, sys
from cryptography.hazmat.primitives import serialization as ser
from cryptography.hazmat.primitives.asymmetric import rsa

KEY, SQL = "snowflake_key.p8", "snowflake_setup.generated.sql"
if os.path.exists(KEY):
    sys.exit(f"{KEY} already exists. Delete it first if you want a new key, because the public key in Snowflake must match it.")
k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
with open(KEY, "wb") as f:
    f.write(k.private_bytes(ser.Encoding.PEM, ser.PrivateFormat.PKCS8, ser.NoEncryption()))
pub = k.public_key().public_bytes(ser.Encoding.PEM, ser.PublicFormat.SubjectPublicKeyInfo).decode()
body = "".join(line for line in pub.splitlines() if "-----" not in line)
sql = f"""use role accountadmin;
create warehouse if not exists ECOM_WH warehouse_size = 'XSMALL' auto_suspend = 60 auto_resume = true initially_suspended = true;
create database if not exists ECOM;
create role if not exists DBT_ROLE;
grant all privileges on database ECOM to role DBT_ROLE;
grant usage, operate on warehouse ECOM_WH to role DBT_ROLE;
create user if not exists DBT_USER type = service default_role = DBT_ROLE default_warehouse = ECOM_WH;
alter user DBT_USER set rsa_public_key = '{body}';
grant role DBT_ROLE to user DBT_USER;
select current_organization_name() || '-' || current_account_name() as account_identifier;
"""
open(SQL, "w").write(sql)
print(f"wrote {KEY} (private, keep it local) and {SQL}")
