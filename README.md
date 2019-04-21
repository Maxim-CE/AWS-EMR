# AWS-EMR
This project automatically extracts collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce.

# Project EMR Steps
### Step 1 - First Mapping Process
Three pairs of Key-Value are created by the following fashion:
1. First word and it's decade.
2. Second word and it's decade.
3. Total amounts of words in this decade.

Simillar words will be gathered by the Combiner, while the Reducer will sum up the Key-Value and map the decades.

### Step 2 - Second Mapping Process
Three pairs of Key-Value are created by the following fashion:
1. First word and it's decade.
2. Second word and it's decade.
3. 2-gram combination of those words.

Simillar words will be gathered by the Combiner, while the Reducer will sum up the Key-Value and map the decades.
