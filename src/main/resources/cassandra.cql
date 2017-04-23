//ratings table
CREATE TABLE ratings(user int, product int, rating float, primary key(user, product));

//user recommendations
CREATE TABLE user_recommendations (user int , products set<int>, primary key(user));

//dev keyspace
create keyspace dev with replication = {'class':'SimpleStrategy','replication_factor':1};