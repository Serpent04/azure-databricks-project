# azure-databricks-project

The project is a replication of data on F1 (Formula 1) results provided by BBC News Sport.

Fully built in Microsoft Azure system, including ADLS Gen2, Databricks, Key-Vault.

The raw data was received from Ergast API, containes the detailed information about 
of all races that took place in 1950-2021 period. 

The final version was built with Delta Lake layer on top of the processed data. 
However, some examples of implementing ACID transactions and analytics using SQL were provided as well. 

As the final outcome, the raw data was transformed into more clean, structured view, providing a ranking list
of racers and constructors for each year through out the whole period of the data. 
