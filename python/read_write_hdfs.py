from hdfs import InsecureClient
import os
import pandas as pd

os.environ['WEBHDFS_URI'] = 'http://172.17.0.2:50070' # A NA PAS FAIRE, POUR TESTS EN LOCAL UNIQUEMENT

source_csv = '/user/root/test.csv'

content = '"eva","date","duration","country","purpose","crew"\n'
content += '"1","1965-06-03T00:00:00","0:36","USA","First U.S. EVA. Used HHMU and took  photos.","Ed White"\n'
content += '"9","1966-11-13T00:00:00","2:06","USA","Attached tether between Agena and Gemini.","Buzz Aldrin"\n'

# Etape 0 : ecriture du fichier pour le test
# - Instance de client pointant sur un HDFS en particulier
source_hdfs_url = os.environ['WEBHDFS_URI']
source_hdfs = InsecureClient(url=source_hdfs_url, user='root')

with source_hdfs.write(source_csv, append = False, overwrite = True, encoding = 'utf-8') as writer:
    writer.write(content)

################################################################################

# Etape 1 : lecture du fichier
# - Reutilisation du client HDFS
with source_hdfs.read(source_csv, encoding='utf-8') as reader:
    data = pd.read_csv(reader, index_col=0)

data = data.ix[:, ['date', 'country']]

print(data)

# Etape 2 : ecriture du fichier dans le HDFS de destination
# - Pourrait etre le meme que precedement, si c'est le cas, les 2 lignes suivantes sont inutiles
dest_hdfs_url = os.environ['WEBHDFS_URI'] # Meme HDFS...
dest_hdfs = InsecureClient(url=dest_hdfs_url, user='bob')
dest_csv = '/user/bob/test.csv'

with dest_hdfs.write(dest_csv, append = False, overwrite = True, encoding = 'utf-8') as writer:
    writer.write(data.to_csv())
