# _Pipeline.py_

#### Simple python data processing pipeline

Chosen Approach
This custom python data pipeline allows users to perform a selection of predefined data cleaning and processing steps. Those steps include, but are not limited to: 
Load a CSV file from a user-specified directory and remove the index
Create and validate the schema of the dataset
Generate a hash id that uniquely identifies each row of data allowing for the removal of duplicates (and ease of storing in a DB)
Users can elect to run the script by passing --merge True, which will concatenate all files processed on each run. 

#### Architecture 
A basic python class object holds a selection of methods that can be used to clean and process input data. 
The methods are then chained together using python's built in 

#### How to run the data pipeline
* clone this repo
* navigate to the root directory
* To run the pipeline using a sample file stored at "src/raw" and merge all processed files: ```$make run_merge```
* To run the pipeline using a sample file stored at "src/raw" without merging all processed files: ```$make run```

#### You can also run the script by: 
Navigating to the subdirectory "src/" and running the commands: 
```
$source venv/bin/activate
$pip3 install requirements.txt
```

Then run one of the following: 
```
$python3 pipeline.py -subdir <dir/where/you/have/sample/files>
$python3 pipeline.py -subdir <dir/where/you/have/sample/files> -merge True
```

#### Future iterations of this pipeline might: 
1.  Be created in AWS using S3 buckets, lambda functions, and athena for querying data from each processing stage partitioned by date.
2.  Make use of a SQLit database due to the relatively small amount of data expected to be processed (168 files). This DB might contain:
    -  A table mapping produce produced by each state. 
    -  A table mapping commodities to refed categories.
    -  Views for calcuations eliminating the need for the provided calculations.py file.
3.  Includ functional tests =)
4.  Validate the data using pydantic or tensorflow's schema validators, which detect datadrift and to create more robust descriptive statics. Yes, I know this is not ML project, but the tools are good. 
5.  Streamlit, connected to the above mentioned databased could be used to visualize data and perform calculations as needed. 
