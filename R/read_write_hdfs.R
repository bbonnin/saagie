chooseCRANmirror(ind = 1)

if (!require(httr)) {
  #system("sudo apt-get install -y libcurl4-openssl-dev")
  install.packages("httr")
  library(httr)
}


Sys.setenv(WEBHDFS_URI = "http://172.17.0.2:50070") # NE PAS FAIRE

# Variable d'environnement à positionner dans la partie "Settings" de la plateforme Saagie
# - la valeur doit être au format http://serveur:port où serveur est l'adresse IP ou hostname du namenode
#	=> pour savoir: aller sur le manager, dans la partie datalake service, cliquer sur HDFS, dans le panneau 
#      qui s'ouvre, il y a les infos de connexions, notamment la partie WebHDFS

webHdfsUri = Sys.getenv("WEBHDFS_URI")
hdfsUri <- paste(webHdfsUri, "/webhdfs/v1")

###################
# Lecture sur HDFS
###################
optionnalParameters <- ''
readParameter <- "?op=OPEN"

csvUri <- "/user/root/test.csv"
csv <- read.csv2(file=paste0(hdfsUri, csvUri, readParameter, optionnalParameters), sep = ",", quote = "\"", check.names=TRUE)
print(csv)

####################
# Ecriture sur HDFS
####################
# - Il est possible de changer d'URL pour HDFS pour l'ecriture
# - cela permet de copie le fichier d'une plateforme à l'autre
#webHdfsUri = Sys.getenv("WEBHDFS_URI")
#hdfsUri <- paste(webHdfsUri, "/webhdfs/v1")

outputCsvUri <- "/user/bob/test.csv"
outputCsvParameters <- "?user.name=bob&op=CREATE&overwrite=true"
outputDataUri <- paste0(hdfsUri, outputCsvUri, outputCsvParameters)

response <- PUT(outputDataUri)
uriWrite <- response$url

if (!file.exists("test.csv")) {
    write.csv(csv, row.names = F, file = "test.csv")
    responseWrite <- PUT(uriWrite, body = upload_file("test.csv"))
    file.remove('test.csv')
} else {
    file.remove('test.csv')
    stop("A file named 'tmp.csv' already exists in the current directory")
}
