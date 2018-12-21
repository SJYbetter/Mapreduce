### CS6240 HOMEWORK4 BY JIEYU SHENG

https://github.ccs.neu.edu/cs6240f18/jieyusheng

1. ##### Show the MapReduce pseudo-code for your K-means program

   ##### preprocessing

   ​           mapper emit(userId, following userId)

   ​           reduce emit(userId, following count)

   ##### random sample centroids, skip if bad input

   ​           mapper emit (userId, following count) when random number < percent

   ​           reduce take k record as centroids



   ##### k-means Algorithm:

   #####    mapper: 

   ​            load centroid list

   ​            foreach userId, followingCount in input

   ​            select minimal distance to all centroids

   ​            emit (centroid, userId, followingCount)

   #####    reducer:

   ​           calculate new centroid && output 

   ​           calculate sse && report

2. ##### Show the plot of the distribution of the number of followers.

   ![image-20181106141755500](/Users/jieyusheng/Library/Application Support/typora-user-images/image-20181106141755500.png)

3. ##### Run the program on AWS on the full Twitter edges dataset 

   | Config  | Num_Iter | R.T  |    NumFinalCenters     | SSE_1       | SSE_2     | SSE_3     | SSE_Final |
   | :-----: | :------: | :--: | :--------------------: | ----------- | --------- | --------- | --------- |
   |  5_bad  |    10    | 13'  | 25416，87773， 7，9950 | 86744909007 | 105285566 | 104699681 | 105391791 |
   | 5_good  |    10    | 12'  | 1416，5，77699， 20259 | 8392854115  | 83882933  | 70026032  | 71489190  |
   | 10_bad  |    10    | 12'  | 25416，87773， 7，9950 | 86744909007 | 105285566 | 104699681 | 105391791 |
   | 10_good |    10    | 12'  | 25294，1677，5，86180  | 2683946850  | 78052083  | 72057288  | 71828902  |

    

