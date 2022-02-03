// Databricks notebook source
val storageAccount = "ncblobml"
val container = "ncsqldb"
val sasKey = "https://ncblobml.blob.core.windows.net/?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2023-02-03T17:08:38Z&st=2022-02-03T09:08:38Z&spr=https&sig=3FvEj8sTYJJKzKQiP6mVpkqyxwJ0KmDFfm3nE0CQ3NY%3D"
 
val mountPoint = s"/mnt/ncblobml-ncsqldb"
 
 
try {
  dbutils.fs.unmount(s"$mountPoint") // Use this to unmount as needed
} catch {
  case ioe: java.rmi.RemoteException => println(s"$mountPoint already unmounted")
}
 
 
val sourceString = s"wasbs://$container@$storageAccount.blob.core.windows.net/"
val confKey = s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net"
 
 
 
try {
  dbutils.fs.mount(
    source = sourceString,
    mountPoint = mountPoint,
    extraConfigs = Map(confKey -> sasKey)
  )
}
catch {
  case e: Exception =>
    println(s"*** ERROR: Unable to mount $mountPoint. Run previous cells to unmount first")
}
 
 
 
%fs ls /mnt/mymountpointname/
 
/*This will list all the files located at the container*/
