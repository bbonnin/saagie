import ibis
import pandas as pd
import os

# ====== Ibis conf (pour contournement d'un bug) ======
with ibis.config.config_prefix('impala'):
    ibis.config.set_option('temp_db', '`__ibis_tmp`')

# ====== Connexion ======
# Connecting to Hive by providing Hive host ip and port (10000 by default) and a Webhdfs client
# - Pour ajouter les valeurs de ces variables d'env. dans la plateforme Saagie:
#    - allez sur la manager, choisissez votre plateforme
#    - pour avoir la valeur de IP_HDFS:  cliquez sur HDFS: dans le panneau qui s'ouvre,
#      notez la valeur de l'IP dans la section WebHDFS
#    - pour avoir la valeur de IP_HIVE: cliquez sur Hive: dans le panneau qui s'ouvre,
#      notez la valeur de l'IP dans la section HiveServer2
#    - allez dans les Settings, ajoutez ces variables d'env.

hdfs = ibis.hdfs_connect(host=os.environ['IP_HDFS'], port=50070)

# ibis.impala.connect pour des versions plus anciennces
#client = ibis.impala.connect(host=os.environ['IP_HIVE'], port=10000, hdfs_client=hdfs, user='VOTRE_USERNAME', password='VOTRE_PASSWORD', auth_mechanism='PLAIN')
client = ibis.impala.connect(host=os.environ['IP_HIVE'], port=10000, hdfs_client=hdfs, user='bruno.bonnin', password='Matlol1971', auth_mechanism='PLAIN')

# ====== Ecriture dans la table ======
# Creation d'une simple DataFrame pandas with 2 colonnes
liste_hello = ['hello1','hello2']
liste_world = ['world1','world2']
df = pd.DataFrame(data = {'hello' : liste_hello, 'world': liste_world})

# Ecriture de la dataframe dans Hive si la table n'existe pas
db = client.database('default')
if client.exists_table('helloworld'):
	table = db.table('helloworld')
	table.drop()
	#db.drop_table('helloworld')

db.create_table('helloworld', df)
t = db['helloworld']
t.execute()
    
    
# ====== Lecture de la table ======
# Selection de data avec une requete SQL
#limit=None pour avoir toute la table sinon on ne recupere que les 10000 premieres lignes
requete = client.sql('select * from helloworld')
df = requete.execute(limit=None)
print(df)