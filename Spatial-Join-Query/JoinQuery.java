/* 
Original code
*/

public JavaPairRDD<Envelope, HashSet<Point>> SpatialJoinQueryUsingCartesianProduct(PointRDD pointRDD,RectangleRDD rectangleRDD) {
	// the function to be written
	JavaRDD<Envelope> rects = rectangleRDD.getRawRectangleRDD();
	List<Envelope> lrects = rects.collect();
	List<Tuple2<Envelope, HashSet<Point>>> answer = new ArrayList<Tuple2<Envelope, HashSet<Point>>>();
	// rangequery returns pointrdd
	// pointrdd returns javarddpoints
	// can be converted into lists
	//System.out.println("Need to do for"+lrects.size());
	for(int i=0;i<lrects.size();i++){
			//System.out.println(i);
			List<Point> temp = RangeQuery.SpatialRangeQuery(pointRDD, lrects.get(i), 0).getRawPointRDD().collect();
			answer.add(new Tuple2(lrects.get(i),new HashSet<Point>(temp)));	
		
	}
	JavaPairRDD<Envelope, HashSet<Point>> finala = sc.parallelizePairs(answer);
	JavaPairRDD<Envelope, HashSet<Point>> joinListResultAfterAggregation = aggregateJoinResultPointByRectangle(finala);
	return joinListResultAfterAggregation;

	
	}
  
/* 
Original code
*/

