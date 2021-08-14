# Slowly Changing Dimension Type 1


In `Dimensional Modelling` for OLAP systems Slowly Changing Dimension (SCD) is a very important concept. In Dimensional Model we have `Fact Table` which contains all the facts or measurable or numerical entities and `Dimension Tables` that contains supporting data providing meaning to numerical data of Fact table.
`Slowly changing dimensions` are the dimensions in which the data changes slowly, rather than changing regularly on a time basis.

`SCD type 1` methodology is implemented where there is no need to store historical data in the Dimension table. This method overwrites the old data in the dimension table with the new data.



