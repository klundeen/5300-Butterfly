# 5300-Butterfly

Handoff Video Link: https://www.youtube.com/watch?v=KYA9yBNpndY

Milestone 1:
 
    Navigate to your cpsc5300 directory in cs1: 
    cd ~/cpsc5300
    
    Clone our repository inside the cpsc5300 directory: 
    git clone https://github.com/klundeen/5300-Butterfly.git
     
    Navigate to the cloned repository: 
    cd 5300-Butterfly
    
    To checkout Milestone1 tags:
    git checkout tags/Milestone1
    
    To make our milestone 1 program type 'make'. If you need to clean the solution type 'make clean'
    
    To run the program type './sql5300' and the path to a writable directory for the database environment:
    Example usage: ./sql5300 ~/cpsc5300/data
    
    The program will prompt you for SQL statements, parse them, and print them out.
    
    To exit the program type 'quit'
    

Milestone 2: 

    Navigate to your cpsc5300 directory in cs1: 
    cd ~/cpsc5300
    
    Clone our repository inside the cpsc5300 directory: 
    git clone https://github.com/klundeen/5300-Butterfly.git
     
    Navigate to the cloned repository: 
    cd 5300-Butterfly
    
    To checkout Milestone2 tags:
    git checkout tags/Milestone2
    
    To make our milestone 1 program type 'make'. If you need to clean the solution type 'make clean'
    
    To run the program type './sql5300' and the path to a writable directory for the database environment:
    Example usage: ./sql5300 ~/cpsc5300/data
    
    The program will prompt you for SQL statements, parse them, and print them out.
    
    To test the heap_storage, type 'test'
    
    To exit the program type 'quit'
    
Handoff Summary: 
    
    Milestone 1 should compile and be working correctly 
    
    Milestone 2 should compile and be partially working 
    
    Moving forward: 
    Please check this code against Kevin's working solution.
    There is a Segmentation fault in the test_heap_storage() line 682 
    
    Areas of note are:
    HeapTable: Validate, UnMarshal, Append
    HeapFile: Put, Get
    
# Otono Sprint:
Nicholas Nguyen, Yinying Liang

## Handoff Summary:
 To clone the project: git clone https://github.com/klundeen/5300-Butterfly.git

 To make the program: type 'make'

 To clean the make files: type 'make clean'

 To run the program: ./sql5300 ~/cpsc5300/data (second arg is the path where your database env locates)

 To exit the program: type 'quit' 

 Modified file: SQLExec.cpp

 ### Milestone 3:
 checkout tag Milestone 3: git checkout tags/Milestone3
 Major implementations: create_table, drop_table, show_table, show_columns
 compiles and works expectedly

 ### Milestone 4:
checkout tag Milestone 4: git checkout tags/Milestone4
 
Major implementations:
 
**Creating a new index:**

Get the underlying table. 
Check that all the index columns exist in the table.
Insert a row for each column in index key into indices.
Call get_index to get a reference to the new index and then invoke the create method on it.

**Dropping an index:**

Call get_index to get a reference to the index and then invoke the drop method on it.
Remove all the rows from _indices for this index.

**Showing index:**

Do a query (using the select method) on _indices for the given table name.

**Dropping a table:**

Before dropping the table, drop each index on the table.
