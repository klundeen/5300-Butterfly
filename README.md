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
    
Otono Sprint: Nicholas Nguyen, Yinying Liang
