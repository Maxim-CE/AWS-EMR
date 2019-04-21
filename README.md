# AWS-EMR
This project automatically extracts collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce.

# Project EMR Steps
### Step 1 - First Mapping Process
Three pairs of Key-Value are created by the following fashion:
1. First word and its decade.
2. Second word and its decade.
3. Total amounts of words in this decade.

Simillar words will be gathered by the Combiner, while the Reducer will sum up the Key-Value and map the decades.

### Step 2 - Second Mapping Process
Three pairs of Key-Value are created by the following fashion:
1. First word and its decade.
2. Second word and its decade.
3. 2-gram combination of those words.

Simillar words will be gathered by the Combiner, while the Reducer will sum up the Key-Value and map the decades.

### Step 3 - Steps Combination 
The output of step 1 & step 2 will be passed to the Reduce process.
In this process, relevant parameters will be exctracted to compute the lambda of every words pair.
For example: `lambda.png`, `L.png`, `p.png`.
Afterwards a new Key-Value is to be created, which contains `-2*log(lambda)` value, words combination and its decade.

### Step 4 - Output
Step 4 receives the sorted output of step 3 by `-2*log(lambda)` value.
In reduce stage 100 highest Key-Value will be chosen of arbitrary decade, those values will be saved to an output file.
