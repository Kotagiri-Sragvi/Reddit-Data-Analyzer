This folder, P01DA26, represents the Project number 01 in the field of Data Analytics in the year 2026.

EXECUTION FLOW:
1. Get the data, raw data from Devvit's app. This can be achieved by creation of a Devvit app using the CLI and using Node.js for getting the data. (The detailed information is available in the final report)
2. Saved the direct data into a folder - "ConsoleData/". A python code - "LogParser.py" is used to convert these text files into json files - "{subreddit}RawData.json".
3. Another python program - "DataCleanser.py" converts these json files of raw data into csv of posts and comments - "{subreddit}_posts.csv" and "{subreddit}_comments.csv" accordingly.
4. "MasterAnalysis.py" program allows the gemini's and groq's api to analyse the different processed data - "{subreddit}_{posts/comments}" based on sentiment, toxicity and topics, the program also generates a file recognizing pattern and summarize the data processed. "TechnologyCommentsAnalysis.py" is another python program with the same functions but only for - "Technology_comments.csv" due to their large sizes.
5. "GenerateReports.py" program allows us to visualize viz charts and graphs the different analysis we have made. "NarrativeReportGeneration.py" generates a markdown file comparing the patterns and summaries of the different data files.

EXECUTION FLOWCHART:

Raw Data files (.json) -> DataCleanser.py -> MasterAnalysis.py -> TechnologyCommentsAnalysis.py -> GenerateReports.py -> NarrativeReportGeneration.py

Execution of each file/program, one at a time will be sufficient to complete the pipeline.

CLEANING DECISIONS:
1. Remove "Deleted" comments.
2. Remove "Duplicate" entries.
3. Handle "Missing" values.
4. Conversion of timestamps into readable format.

Following the execution steps and creating your own .env file as shown in the .example file will allow you to complete the pipeline and the analysis of data as done by me.
