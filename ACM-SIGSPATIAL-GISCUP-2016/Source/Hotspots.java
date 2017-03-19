package com.dds.phase3;


import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;





public class Hotspots implements Serializable{
    public static JavaSparkContext sc;
    static String cores;
    
    static Properties prop;
    static String queryName;
    static InputStream input;
    static String inputLocation;
    static Integer offset;
    static String splitter;
    static Integer numPartitions;
    
    static String inputLocation2;
    static Integer offset2;
    static String splitter2;
    static String gridType="";
    static int numPartitions2;
    
    static int loopTimes;
    static SparkConf conf;
    static String masterName;
    static String jarPath;
	/**
	 * @param args
	 */
	
	public static void main(String[] args) {
		// create Spark context with Spark configuration
	    // get threshold
		cores="4";
		masterName="local";
		inputLocation=args[0];
		offset= 0;
		splitter="csv";
		numPartitions=50;
		loopTimes=1;
		queryName="something";	    
	    
	    conf=new SparkConf().setAppName(queryName+"+"+inputLocation+"+"+gridType+"+"+cores+"+"+numPartitions).setMaster(masterName)
				.set("spark.history.fs.logDirectory", "/home/dds/DDS/sparkeventlog")
				.set("spark.history.retainedApplications", "10000")
				.set("spark.eventLog.enabled", "true")
				.set("spark.eventLog.dir", "/home/dds/DDS/sparkeventlog")
				.set("spark.executor.memory", "6g")
				.set("spark.core.connection.ack.wait.timeout","900")
				.set("spark.akka.timeout","900")
				.set("spark.akka.frameSize","256")
				.set("spark.memory.storageFraction", "0.1")
				.set("spark.driver.memory", "10g")
				.set("spark.cores.max",cores)
				.set("spark.driver.cores","3")
				.set("spark.driver.maxResultSize", "50g")
				//.set("spark.tachyonStore.url", "tachyon://"+args[0]+":19998")
				//.set("spark.externalBlockStore.url", "alluxio://"+args[0]+":19998")
				//.set("spark.externalBlockStore.blockManager", "org.apache.spark.storage.TachyonBlockManager")
				.set("log4j.rootCategory","ERROR, console");
		sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        try {
			findtop50hotspots(sc,args);
        	
        	
        	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("Over");
        TearDown();
		
	}
    public static void findtop50hotspots(JavaSparkContext sc, String[] args) throws Exception {
    	System.out.println("In Function");
    	long start = System.currentTimeMillis();
  	
    	JavaRDD<String> textFile = sc.textFile(args[0]);
    	PrintWriter pw = new PrintWriter(new File(args[1]));
        
    	System.out.println("Input CSV Read from:\t\t"+args[0]);
    	System.out.println("Detecting Eligible Points.......");
   	
    	JavaPairRDD<String, Integer> pairs = textFile.mapToPair(new PairFunction<String, String, Integer>() {
    		  public Tuple2<String, Integer> call(String s) {
    			  String[] row = s.split(",");
    			  Double yDcord = Double.parseDouble(row[5]);
    	    		Double xDcord = Double.parseDouble(row[6]);
    	    		int xCord,yCord,zCord;
    	    		if(xDcord >= 40.50 && xDcord <= 40.90 && yDcord >= -74.25 && yDcord <= -73.70){
    	    			
    	    			zCord = Integer.parseInt(row[1].split(" ")[0].split("-")[2])-1;
    	    			yCord = cordToIndex(yDcord);
    	        		xCord = cordToIndex(xDcord);
    	        		return new Tuple2<String, Integer>(zCord+"%"+yCord+"%"+xCord, 1); 
    	    		}
    			  
    			  return new Tuple2<String, Integer>("OutOfEnvelope", 1); }
    		});
    	JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
    		  public Integer call(Integer a, Integer b) {return a + b; }
    		});
    	
    	List<Tuple2<String,Integer>> data = counts.collect();
    	//System.out.println("total"+data.size()+data);
    	
    	int store[][][] = new int[31][41][56];
    	String[] row = new String[3];
    	int xCord, yCord, zCord;
    	
    	for(int i=0;i<data.size();i++){
    		if(!data.get(i)._1().equals("OutOfEnvelope")){
    		row = data.get(i)._1.split("%");
    		    
    			zCord = Integer.parseInt(row[0]);
    			yCord = Integer.parseInt(row[1]);
        		xCord = Integer.parseInt(row[2]);
        		store[zCord][xCord][yCord] += data.get(i)._2;
    		}
    	}
    	
    	int totalNumberOfCells = 71176;
    	int sumOfAllXi = sumOfAll(store);
    	System.out.println("Total Eligible Points:\t\t"+sumOfAllXi);
    	System.out.println("Counting Z-score for each cell.......");
    	double xBar = (double)sumOfAllXi/(double)totalNumberOfCells;
    	double sValue;
    	sValue = calculateSValue(store, totalNumberOfCells,xBar);
    	double gNumerator,gDenominator;
    	int[] t = new int[3];
   		int[] x = new int[3];
   	    int[] y = new int[3];
   	    //////////// REMOVED LINES
   	    //TreeMap<Double,String> topZscore = new TreeMap<Double,String>(Collections.reverseOrder());
	    ///////////////////////////- end
   	    
   	    ///////////  ADDED LINES
   	    Comparator<String> com = new Comparator<String>(){
   	    	public int compare(String a, String b){
   	    		return -1*Double.compare(Double.parseDouble(a.split("%")[0]), Double.parseDouble(b.split("%")[0]));
   	    	}
   	    };

   	    PriorityQueue<String> topZscore = new PriorityQueue<String>(15520,com);
   	    ///////////////////////////- end
   	    
   	    
    	for(int z=0;z<store.length;z++){
    		for(int i=0;i<store[0].length;i++){
    			for(int j=0;j<store[0][0].length;j++){
    				gNumerator = calculateNumValue(store,totalNumberOfCells,xBar,t,x,y,z,i,j);
    				gDenominator = calculateDenValue(store,totalNumberOfCells,sValue,t,x,y,z,i,j);
    				//System.out.println("For\t"+z+":"+i+":"+j+"\tz score"+(gNumerator/gDenominator));
    				//if((gNumerator/gDenominator)>30.00)
    				//System.out.println("For\t"+z+":"+i+":"+j+"\tz score"+gNumerator+"&"+gDenominator+"\tz score"+(gNumerator/gDenominator));
    				gNumerator /= gDenominator;
    				//topZscore.add(Double.toString(gNumerator)+"%"+Integer.toString(z)+"%"+Integer.toString(i)+"%"+Integer.toString(j));
    				//sample.add(gNumerator);
    				
    				//////////// REMOVED LINES
    				//topZscore.put(gNumerator, );
                                ///////////////////////////- end
    				
    				///////////  ADDED LINES
    				topZscore.offer( Double.toString(gNumerator)+"%"+Integer.toString(z)+"%"+Integer.toString(i)+"%"+Integer.toString(j));
				///////////////////////////- end
    				
    			}
    		}
    	}
    	// iterate and display the top 50 sets 
    	int dis = 0;
    	String entry = "";
    	StringBuilder sb = new StringBuilder();
    	////////////REMOVED LINES
    	//Iterator<Map.Entry<Double,String>> displayIt = topZscore.entrySet().iterator();
        ///////////////////////////- end
    	
    	////////////REMOVED LINES
    	//while(displayIt.hasNext() && dis<=49){
        ///////////////////////////- end

    	///////////  ADDED LINES
    	while(dis<=49){
	///////////////////////////- end

    		////////////REMOVED LINES  - start
    		//entry = displayIt.next().getValue();
		///////////////////////////- end

    		///////////  ADDED LINES
    		entry = topZscore.poll();
    		///////////////////////////- end
    		
    		sb.append(Integer.parseInt(entry.split("%")[2]) + 4050);
    		sb.append(',');
    		sb.append(-1*(Integer.parseInt(entry.split("%")[3]) + 7370));
    		sb.append(',');
    		sb.append(Integer.parseInt(entry.split("%")[1]));
    		sb.append(',');
    		////////////REMOVED LINES
    		//sb.append(displayIt.next().getKey());
    		///////////////////////////- end

    		////////////ADDED LINES
    		sb.append(entry.split("%")[0]);
    		///////////////////////////- end

    		sb.append('\n');
    		//zCord = Integer.parseInt(entry.split("%")[0]);
    		//xCord = Integer.parseInt(entry.split("%")[1]) + 4050;
    		//yCord = Integer.parseInt(entry.split("%")[2]) + 7370;
    		//entry = xCord + ","+"-"+yCord+","+zCord+","+displayIt.next().getKey();
    		//System.out.println(entry);
    		dis++;
    	}
    	pw.write(sb.toString());
        pw.close();
        System.out.println("Top 50 Hotspots are written at:\t"+args[1]);
    	System.out.println("Over in "+(System.currentTimeMillis()-start)/1000+" seconds");
    }
    
    public static double calculateDenValue(int[][][] a, int totalNumberOfCells, double sValue,int[] t,int[] x,int[] y, int time, int row, int col){
    	int count = 0;
    	double temp = 0.00;
    			 t[0] = time-1;
    			 t[1] = time;
    			 t[2] = time+1;
    			 x[0] = row-1;
    			 x[1] = row;
    			 x[2] = row+1;
    			 y[0] = col-1;
    			 y[1] = col;
    			 y[2] = col+1;
    			for(int it=0;it<3;it++){ 
    				for(int ix=0;ix<3;ix++){ 
    					for(int iy=0;iy<3;iy++){ 
    						if(!(t[it] < 0 || t[it] >= a.length ||x[ix] < 0 || x[ix] >= a[0].length || y[iy] < 0 || y[iy] >= a[0][0].length)){ 
    							//valid 
    							//if(!(t[it] == time && x[ix] == row && y[iy]==col)){ 
    									// valid System.out.println("^^^^Answer:"+t[it]+":"+x[ix]+":"+y[iy])
    									count++;    									
    								//} 
    						} 
    					} 
    				}
    			}
    	
    	temp = (double)totalNumberOfCells *(double)count;
    	temp -= Math.pow((double) count, (double) 2);
    	temp /= (double) (totalNumberOfCells-1);
    	temp = Math.sqrt(temp);
    	temp *= sValue;
    	
    	
    	return temp;
    }
    public static double calculateNumValue(int[][][] a, int totalNumberOfCells, double xBar,int[] t,int[] x,int[] y, int time, int row, int col){
    	int count = 0;
    	double temp = 0.00;
    			 t[0] = time-1;
    			 t[1] = time;
    			 t[2] = time+1;
    			 x[0] = row-1;
    			 x[1] = row;
    			 x[2] = row+1;
    			 y[0] = col-1;
    			 y[1] = col;
    			 y[2] = col+1;
    			for(int it=0;it<3;it++){ 
    				for(int ix=0;ix<3;ix++){ 
    					for(int iy=0;iy<3;iy++){ 
    						if(!(t[it] < 0 || t[it] >= a.length ||x[ix] < 0 || x[ix] >= a[0].length || y[iy] < 0 || y[iy] >= a[0][0].length)){ 
    							//valid 
    							//if(!(t[it] == time && x[ix] == row && y[iy]==col)){ 
    									// valid System.out.println("^^^^Answer:"+t[it]+":"+x[ix]+":"+y[iy])
    									count++;
    									temp += a[t[it]][x[ix]][y[iy]];
    									
    								//} 
    						} 
    					} 
    				}
    			} 
    	temp -= ((xBar)*(double)count);
    	return temp;
    }
    public static double calculateSValue(int[][][] a, int totalNumberOfCells, double xBar){
    	double temp = 0.00;
    	for(int z=0;z<a.length;z++){
    		for(int i=0;i<a[0].length;i++){
    			for(int j=0;j<a[0][0].length;j++){
    				temp += Math.pow((double)a[z][i][j], (double)2);
    			}
    		}
    	}
    	temp /= (double) totalNumberOfCells;
    	temp -= Math.pow(xBar, (double)2);
		return Math.sqrt(temp);
    }
    public static int sumOfAll(int[][][]a){
    	int sum=0;
    	for(int z=0;z<a.length;z++){
    		for(int i=0;i<a[0].length;i++){
    			for(int j=0;j<a[0][0].length;j++){
    				sum += a[z][i][j];
    			}
    		}
    	}
    	return sum;
    	
    }
    public static void display(int[][][] a){
    	for(int ai=0;ai<a.length;ai++){
        	System.out.println("-----------");
        	for(int i=0;i<a[0].length;i++){
    			System.out.println();
    			for(int j=0;j<a[0][0].length;j++){
    				System.out.print("("+a[ai][i][j]+")");
    			}
    		}
        	System.out.println("-----------");
        }
    	//System.out.println();
    }
    public static int cordToIndex(Double d){
    	
    	//double d = 40.90;
    	int r;
		if(d< 0){
			d = Math.abs(d);
			r = (int) Math.ceil(d * 100.0);
		}
		else
			r = (int) Math.floor(d * 100.0);
		
		
		if(r<= 4090 && r>= 4050)
			return (r- 4050);

		else
			return (r-7370);
    	
    }

    public static void TearDown() {
        sc.stop();
    }


}
