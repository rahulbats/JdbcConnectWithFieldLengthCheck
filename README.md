#  JDBC Field Check Connector
Out of the box Kafka JDBC connector is a generic connector which is designed to work with all kinds of Databases.
For databases like Oracle, PostgreSQL etc users can setup field length constraints. 
If the source system is not sending data which complies with these lengths, it will bring down your connector.
To avoid this you will have to setup complicated filtering logic on all of these topics to filter out data which does not comply with the table constraints. 
A better way is to bake this into the connector logic and these connector is built with the intention to do so.

### JDBC Connector workflow
 

image:JDBC-connector.png[JDBC connector workflow] 


### JDBC Connector with length check workflow
image:JDB-Length-Check-connector.png[JDBC length check connector workflow]

image:https://i.imgur.com/AEkqoRn.jpg[alt="not bad.",width=128,height=128]