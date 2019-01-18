# RecommenderSystem_Hadoop
Predicted the movies that users may like based on item filtering algorithum. Implemented the production of matrices in MapReduce.
Before running the code, 1) please upload the raw data files (usr_i, movie_j, rating) to /input folder at HDFS.
2) Please upload the mysql-connector-java-5.1.39-bin.jar to  /mysql folder at HDFS;
3) Create a the table at mySQL to output predictions. Make the table to be accessible remotly.
4) Configure the parameters in the Haddop code, including driver class, IP, database name, user name, password and path of driver.
