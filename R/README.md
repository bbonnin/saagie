# How to install the R library RJDBC

* Step 1 : Install JDK (Java Development Kit) and other required libs

```sh
# In a terminal
sudo apt-get update
sudo apt-get install openjdk-8-jdk
sudo apt-get install libbz2-dev
sudo apt-get install libpcre3-dev
```

* Step 2 : Install rJava and RJDBC

```sh
# In a terminal (can also be done in a R script)
sudo Rscript -e 'install.packages("rJava", repos = "https://cran.r-project.org/")'
sudo Rscript -e 'install.packages("RJDBC", repos = "https://cran.r-project.org/")'
```
