library(rJava)
library(RJDBC)

#.jinit(force.init=TRUE) 
#.jclassPath()

# Les librairies java (jar) de hive sont stockées dans le répertoire qui suit
hive_path = "/home/admin/hive"

drv <- JDBC(driverClass="org.apache.hive.jdbc.HiveDriver",
            classPath=list.files(hive_path, pattern="jar$", full.names=T),
            identifier.quote="`")

conn <- dbConnect(drv, "jdbc:hive2://dn1:21050,dn2:21050,dn3:21050/;ssl=false;auth=plain", 
				"YOUR_USER_NAME", "YOUR_PASSWORD")

dbGetQuery(conn, "show tables")
