"""Test connection to Oracle Cloud Autonomous DB"""
import oracledb

WALLET_DIR = r"C:\Users\Razvan.PLETOSU\Downloads\Wallet_OlympicsDB"

conn = oracledb.connect(
    user='ADMIN',
    password='Oracle_1234#',
    dsn='olympicsdb_low',
    config_dir=WALLET_DIR,
    wallet_location=WALLET_DIR,
    wallet_password='Oracle_1234#'
)
print('Connected:', conn.version)
cur = conn.cursor()
cur.execute("SELECT banner FROM v$version")
for r in cur:
    print(r[0])
conn.close()
print('Connection test OK!')
