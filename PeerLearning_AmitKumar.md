**1 -** Code imports data from JSON files into a MongoDB database using the mongoimport command-line tool via Python's subprocess module.<br>
**2 -** Here, connection to  MongoDB server is made using PyMongo and data inserted into collections using a custom function. <br>
**3 -** **Comments** <br>
Some common functinalities used in this section : <br>
***match*** : Filters the documents to pass only the documents that match the specified condition(s) to the next pipeline stage.<br>
***group*** : separates documents into groups according to a "group key". The output is one document for each unique group key. <br>
***sort*** : Sorts all input documents and returns them to the pipeline in sorted order.<br>
***project*** : Passes along the documents with the requested fields to the next stage in the pipeline. <br>






