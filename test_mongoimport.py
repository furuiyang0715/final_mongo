import configparser
import subprocess

config = configparser.ConfigParser()
config.read('conf/config.ini')

# mysql 的配置项
conf = config['mysql']

# mongodb 的配置项
m_conf = config['mongodb']

db = "datacenter"

table = "economic_gdp"

file = "datacenter_economic_gdp_-1-417.csv"

showposcommand = f"mongoimport --host={conf['host']} --db {db} --collection {table} --type csv --headerline --ignoreBlanks --file {file}"

p1 = subprocess.Popen(showposcommand, shell=True)

