
# DBABandits-modded
A modified version of https://github.com/malingaperera/DBABandits with the addition of QBL


# The setup 
The setup requires a connected mssql TPC-H server to run. We provide an overview of a setup using microsofts provided docker image.

Pull mssql container with  
`sudo docker pull mopt/mssql-tools/bin/scr.microsoft.com/mssql/server:2022-latest`

Run it with 
```
sudo docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=L0ngpassword!" \
-it --mount src="~/path/to/schema",target=/mnt,type=bind \
   -p 1433:1433 --name sql1 --hostname sql1 \
   -d \
   mcr.microsoft.com/mssql/server:2022-latest
```

Which mounts a path that can be used to save table files into. Remember to set a suitable password.

Create schema files with TPC-H generator. One way is outlined here: https://github.com/nghiahhnguyen/TPC-H-Dataset-Generator-MS-SQL-Server It may require minor modification of the bulkloading and .ddl and .ri files depending on setup.

An easy way to access the is with **`mssql-cli`**

You can then connect with `mssql-cli -S localhost -U SA -P L0ngpassword!` and run sql commands e.g. `DROP DATABASE tpc` and `CREATE DATABASE tpc`

With a database created you can run files such as 
`mssql-cli -S localhost -U SA -P L0ngpassword! -d tpc -i dss.ddl & mssql-cli -S localhost -U SA -P L0ngpassword! -d tpc -i dss.ri`

Which can reference schema table files from the mounted `/mnt` folder







