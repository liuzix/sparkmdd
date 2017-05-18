# sparkmdd
This is a proof-of-concept implementation of a mutable dataset for Spark. We aim to achieve in-place update of data with low memory management overhead. 

Currently the dataset is stored locally on each worker. We are able to persist the data in memory across operations by utilizing a thread-local data structure per JVM. 
