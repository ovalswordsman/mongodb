import subprocess
import pymongo
import os


json_file_path = "/Users/kush/Desktop/Courses/MongoDB/Disprz_Assgn_Mongo/sample_mflix"
dbname = "assgn2_disprz"

collection_name_1 = "movies"
subprocess.run(["mongo", "--eval", f"db.createCollection('{collection_name_1}')", dbname])
file_path = os.path.join(json_file_path,"movies.json")
subprocess.run(["mongoimport", "--db", dbname, "--collection", collection_name_1 , "--file", file_path])


collection_name_1 = "comments"
subprocess.run(["mongo", "--eval", f"db.createCollection('{collection_name_1}')", dbname])
file_path = os.path.join(json_file_path,"comments.json")
subprocess.run(["mongoimport", "--db", dbname, "--collection", collection_name_1 , "--file", file_path])


collection_name_1 = "theatres"
subprocess.run(["mongo", "--eval", f"db.createCollection('{collection_name_1}')", dbname])
file_path = os.path.join(json_file_path,"theaters.json")
subprocess.run(["mongoimport", "--db", dbname, "--collection", collection_name_1 , "--file", file_path])


collection_name_1 = "users"
subprocess.run(["mongo", "--eval", f"db.createCollection('{collection_name_1}')", dbname])
file_path = os.path.join(json_file_path,"users.json")
subprocess.run(["mongoimport", "--db", dbname, "--collection", collection_name_1 , "--file", file_path])





# insert new comments into the comments collection
def insert_comment(comment):
    db.comments.insert_one(comment)

# insert new movie into the movies collection
def insert_movie(movie):
    db.movies.insert_one(movie)

# insert new theatre into the theaters collection
def insert_theatre(theatre):
    db.theaters.insert_one(theatre)

# insert new user into the users collection
def insert_user(user):
    db.users.insert_one(user)

