# SQLite-POC

This is a project for evaluating SQLite as a transaction cache.

Two main components -

1. EventProducer: Produces transactions in an interleaving and sequential manner and writes them to a file
      Requires two inputs -
      a. Output file name (string) 
      b. transactions config (json array string) -  Specifies the transactions config to be generated in format - [{transaction_id - number_of_events}] , the transactions specified in a group are generated in a random interleaving manner.
         example - [{\"txn-1232323\":5000,\"txn-12321323\":30000},{\"txn-122\":232}]
   
2. EventReader: Reads the transactions from a specified file
    Requires two inputs -
     a. Input file name (string)
     b. Transaction in memory limit (integer)
