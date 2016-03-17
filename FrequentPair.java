import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class FrequentPair {

	static class Pair {

		private int item1;
		private int item2;

		public Pair(int _item1, int _item2) {
			this.item1 = _item1;
			this.item2 = _item2;
		}
		
		public int getItem1() {
			return this.item1;
		}
		
		public int getItem2() {
			return this.item2;
		}
		
		@Override
		public String toString() {
			return item1+" "+item2;
		}
		
	}

	static class MutableInt {

		int value; // note that we start at 1 since we're counting

		public void increment () {
			++value;      
		}

		public int  get() { 
			return value; 
		}
		
		public MutableInt() {
			value = 1;
		}
		
		public MutableInt(int _val) {
			value = _val;
		}
	}
	
	public static HashMap<String,Integer> wordToInt;
	
	public static HashMap<Integer,String> intToWord;

	public static HashMap<Integer,MutableInt> frequencyWords;
	
	public static HashMap<Integer,Integer> differentWords;

	public static int[] frequencyPairs;

	public static ArrayList<Integer> differentPairs;
	
	public static HashMap<Integer,Pair> pairMap;

	public static long allPairs=0;
	
	public static int lineNumber=0;
	
	public static int wordIndex=1;
	
	public static int sizeOfDifferentWords;

	public static void main(String[] argv)  {
		
		// try to get the number of all the pairs
		precompute();

		// one pass to get the words which have frequency higher than required
		phase1();

		// one pass to get the pairs which have frequency higher than required
		phase2();

		// output these frequent pairs
		showFrequentPairs();

	}

	public static void showFrequentPairs() {
		
		PrintWriter out = null;
		try {
			
			out = new PrintWriter(new BufferedWriter(new FileWriter("../shakes/Result")));
			
			ArrayList<Integer> top30 = new ArrayList<Integer>();
			
			for ( int i=0;i<differentPairs.size();i++ ) {
				top30.add(differentPairs.get(i));
			}
			
			Collections.sort(top30, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return frequencyPairs[o1]-frequencyPairs[o2];
				}
			});
			
			
			for(int i=top30.size()-30;i<top30.size();i++) {
				Pair aPair = pairMap.get(top30.get(i));
				String[] stringOutput = new String[2];
				stringOutput[0] = intToWord.get(aPair.getItem1());
				stringOutput[1] = intToWord.get(aPair.getItem2());
				Arrays.sort(stringOutput);	
				out.println( stringOutput[0] +" "+ stringOutput[1] + ":" + frequencyPairs[top30.get(i)]   );
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(out != null) {
				out.flush();
				out.close();
			}
		}
	}
	
	
	// note that the requirement is frequency not the number of appearance
	// so we need to compute the number of pairs in advance so that we can decide
	// which word should be kept to produce a potentail pair;
	public static void precompute() {
		
		for(int i=0;i<=29;i++) {
			String fileIndex = String.format("%02d", i);

			FileInputStream fstream=null;
			BufferedReader br=null;
			
			try {
				// Open the file
				fstream = new FileInputStream("../shakes/file"+fileIndex);
				br = new BufferedReader(new InputStreamReader(fstream));

				String strLine;

				//Read File Line By Line
				while ( (strLine = br.readLine()) != null )   {
					
					String[] stringArray = strLine.split(" ");
					
					for( int k1=0 ; k1<stringArray.length ; k1++ ) {
						for( int k2=k1+1 ; k2<stringArray.length ; k2++ ) {
							allPairs++;
						}
					}
					
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if(br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	public static void phase1() {
		
		wordToInt = new HashMap<String,Integer>();
		
		intToWord = new HashMap<Integer,String>();
		
		frequencyWords = new HashMap<Integer,MutableInt>();

		// count;
		for(int i=0;i<=29;i++) {
			String fileIndex = String.format("%02d", i);

			FileInputStream fstream=null;
			BufferedReader br=null;
			try {
				// Open the file
				fstream = new FileInputStream("../shakes/file"+fileIndex);
				br = new BufferedReader(new InputStreamReader(fstream));

				String strLine;

				//Read File Line By Line
				while ( (strLine = br.readLine()) != null )   {
					
					String[] stringArray = strLine.trim().split(" ");
					
					for( String str : stringArray ) {
						
						// see whether this word is already mapped to a integer
						Integer index = wordToInt.get(str);
						// if not, set up the map in both wordToInt and intToWord
						if ( index == null ) {
							wordToInt.put(str, wordIndex);
							intToWord.put(wordIndex, str);
							index = wordIndex;
							wordIndex++;
						}
						
						// get the count of the current word
						MutableInt count = frequencyWords.get(index);
						
						// if it hasn't been mapped before, then put a `1` in the hashmap
						if (count == null) {
							frequencyWords.put(index, new MutableInt());
						}
						// if it has been seen before, then increase the count
						else {
							count.increment();
						}
						
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if(br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		differentWords = new HashMap<Integer,Integer>();
		
		// get these high-frequency word
		int indexOfDifferentWords=1;
		
		// start with 1 because we index the word from 1
		for(int i=1;i<=wordToInt.size();i++) {
			
			// get the number of all apperance
			MutableInt value = frequencyWords.get(i);
			int frequency = value.get();
			
			// if the number of all appearance is larger than required
			if ( frequency >= 0.00005 * allPairs ) {
				
				// i is the index of the word 
				// indexOfDifferentWords is new index of the word so that we can use triangular matrix
				// to record the total number of appearance
				differentWords.put(i, indexOfDifferentWords);
				indexOfDifferentWords++;
				
			}
		}

		// let the jvm gc
		frequencyWords = null;
	}

	

	public static void phase2() {
		
		pairMap = new HashMap<Integer,Pair>();

		sizeOfDifferentWords = differentWords.size();
		
		System.out.println("sizeOfDifferentWords: "+sizeOfDifferentWords);
		
		frequencyPairs = new int[ sizeOfDifferentWords * (sizeOfDifferentWords+1) / 2 + 2 ];
		
		differentPairs = new ArrayList<Integer>();

		for(int i=0;i<=29;i++) {
			
			String fileIndex = String.format("%02d", i);
			System.out.println("file number: "+fileIndex);
			FileInputStream fstream=null;
			BufferedReader br=null;
			
			try {
				// Open the file
				fstream = new FileInputStream("../shakes/file"+fileIndex);
				br = new BufferedReader(new InputStreamReader(fstream));

				String strLine;

				//Read File Line By Line
				while ((strLine = br.readLine()) != null)   {
					
//					lineNumber++;
					String[] stringArray = strLine.trim().split(" ");
					
					Arrays.sort(stringArray);
					
					findFrequentPairs(stringArray);
					
				}
				br.close();
			
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}

		
		for( int i=1;i<frequencyPairs.length;i++ ) {
			int times = frequencyPairs[i];
			if ( times >= 0.00005 * allPairs ) {
				differentPairs.add(i);
			}
		}
	}

	public static void findFrequentPairs(String[] stringArray) {
		
		for( int i=0 ; i<stringArray.length ; i++ ) {
			for( int j=i+1 ; j<stringArray.length ; j++ ) {
				
				// get the index
				int item1Index = wordToInt.get(stringArray[i]);
				int item2Index = wordToInt.get(stringArray[j]);
				
				// only if both words are frequent
				// and we don't count pairs which contains two same words
				if ( differentWords.containsKey(item1Index) && differentWords.containsKey(item2Index) ) {
					
					int ki = differentWords.get(item1Index);
					int kj = differentWords.get(item2Index);
					
					if ( ki < kj ){
					    frequencyPairs[ (ki - 1)*(sizeOfDifferentWords) - (ki - 1)*ki/2 + kj - ki ] ++ ;
 					    
					    Pair aPair = pairMap.get( (ki - 1)*(sizeOfDifferentWords) - (ki - 1)*ki/2 + kj - ki );
					    if ( aPair == null ) {
					    	pairMap.put( (ki - 1)*(sizeOfDifferentWords) - (ki - 1)*ki/2 + kj - ki , new Pair(item1Index,item2Index) );
					    }
					    
					    
					}
					else if ( ki > kj ) {
					    frequencyPairs[ (kj - 1)*(sizeOfDifferentWords) - (kj - 1)*kj/2 + ki - kj ] ++ ;
					    
					    Pair aPair = pairMap.get( (kj - 1)*(sizeOfDifferentWords) - (kj - 1)*kj/2 + ki - kj );
					    if ( aPair == null ) {
					    	pairMap.put( (kj - 1)*(sizeOfDifferentWords) - (kj - 1)*kj/2 + ki - kj , new Pair(item2Index,item1Index) );
					    }
					}
				}
			}
		}
		
	}
	
}
