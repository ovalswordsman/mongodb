import pymongo
import json
from pprint import pprint
# connect to MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
db = myclient['disprz_mongodb']


comments = db['comments']
movies = db['movies']
theaters = db['theatres']
users = db['users']


# a. Comments collection
print('----------------------------------------------------------------')
# 1 . find top 10 users who made the maximum number of comments
def top_users():
    pipeline = [
        {"$group": {"_id": "$email", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)

mylist = top_users()
print("Top 10 users who made the maximum number of comments are :")
count=1
for item in mylist : 
    print(f"{count} : {item}")
    count+=1

print('----------------------------------------------------------------')
# 2.  find top 10 movies with most comments
def top_movies():
    pipeline = [
        {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10},
        {"$lookup": {"from": "movies", "localField": "_id", "foreignField": "_id", "as": "movie"}},
        {"$unwind": "$movie"},
        {"$project": {"_id": 0, "movie_id": "$movie._id", "title": "$movie.title", "count": 1}}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)
mylist = top_movies()
print("top 10 movies with most comments are :")
count=1
for item in mylist : 
    print(f"{count} : {item}")
    count+=1

print('----------------------------------------------------------------')
#  3. find total number of comments created each month in a given year
def comments_by_month(year):
    pipeline = [
        {"$match": {"date": {"$regex": "^" + str(year)}}},
        {"$group": {"_id": {"$substr": ["$date", 5, 2]}, "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = db.comments.aggregate(pipeline)
    return list(result)
mylist = comments_by_month(2022)
print("total number of comments created each month in a given year :")
count=1
for item in mylist : 
    print(f"{count} : {item}")
    count+=1

# b. movies collection

# i -> top `N` movies

# 1. with the highest IMDB rating
print('----------------------------------------------------------------')
def find_top_movies_by_imdb_rating(limit=10):
    pipeline = [
        {"$match": { "imdb.rating": {"$ne" : ""}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "title":"$title" , "rating" : "$imdb.rating"}}
    ]
    # Execute the pipeline
    top_movies = db.movies.aggregate(pipeline)
    # Return the top rated movies with only the title and IMDB rating
    return list(top_movies)

print("top `N` movies with highest IMDB rating :")
mylist = find_top_movies_by_imdb_rating()
count=1
for item in mylist : 
    print(f"{count} : {item}")
    count+=1

print('----------------------------------------------------------------')
# 2.with the highest IMDB rating in a given year
def find_top_movies_by_imdb_rating_in_year(year, limit=10):
    # Define the pipeline
    pipeline = [
        {"$unwind": "$title"},
        {"$match": {"year": year}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "year": 1}}
    ]
    # Execute the pipeline
    top_movies = db.movies.aggregate(pipeline)
    # Return the top rated movies in the given year
    return list(top_movies)

mylist = find_top_movies_by_imdb_rating_in_year(2000);
print('top N movies with the highest IMDB rating in a given year')
count=1
for item in mylist : 
    print(f"{count} : {item}")
    count+=1

print('----------------------------------------------------------------')
# 3. with highest IMDB rating with number of votes > 1000
def find_top_movies_by_imdb_rating_with_votes(min_votes, limit=10):
    pipeline = [
        {"$match": {"imdb.votes": {"$gt": min_votes}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0,"title": 1,"imdb.rating": 1, "imdb.votes": 1}}
    ]
    top_movies = db.movies.aggregate(pipeline)
    return list(top_movies)
mylist = find_top_movies_by_imdb_rating_with_votes(1000)
print('top N movies with highest IMDB rating with number of votes > 1000')
count=1
for item in mylist : 
    print(f"{count} : {item}")
    count+=1

print('----------------------------------------------------------------')
# 4. with title matching a given pattern sorted by highest tomatoes ratings
def find_top_movies_by_title_pattern(pattern, limit=10):
    pipeline = [
        {"$match": {"title": {"$regex": pattern}}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0,"title": 1,"imdb.rating": 1, "viewer_rating" : "$tomatoes.viewer.rating"}}
    ]
    top_movies = db.movies.aggregate(pipeline)
    return list(top_movies)

mylist = find_top_movies_by_title_pattern("the")
print('top N movies with title matching a given pattern sorted by highest tomatoes ratings')
count=1
for item in mylist : 
    print(f"{count} : {item}")
    count+=1

# ii -> top `N` directors 

print('----------------------------------------------------------------')
# 1. who created the maximum number of movies
def find_top_directors_by_num_movies(limit=10):
    top_directors = db.movies.aggregate([
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": pymongo.DESCENDING}},
        {"$limit": limit}
    ])
    # Return the top directors
    return list(top_directors)
mylist = find_top_directors_by_num_movies()
print(' top `N` directors who created the maximum number of movies')
count=1
for item in mylist :
    print(f"{count} : {item}")
    count+=1

print('----------------------------------------------------------------')
# 2. who created the maximum number of movies in a given year
def find_top_directors_by_num_movies_in_year(year, limit=10):
    top_directors = db.movies.aggregate([
        {"$match": {"year": year}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": pymongo.DESCENDING}},
        {"$limit": limit}
    ])
    # Return the top directors
    return list(top_directors)
mylist = find_top_directors_by_num_movies_in_year(2000)
print('top N directors who created the maximum number of movies in the given year')
count = 1
for item in mylist:
    print(f"{count} : {item}")
    count += 1


print('----------------------------------------------------------------')
# 3. who created the maximum number of movies for a given genre 
def find_top_directors_by_num_movies_in_genre(genre, limit=10):
 
    top_directors = db.movies.aggregate([
        {"$unwind": "$genres"},
        {"$match": {"genres": genre}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": pymongo.DESCENDING}},
        {"$limit": limit}
    ])
    # Return the top directors
    return list(top_directors)
mylist = find_top_directors_by_num_movies_in_genre("Action")

print('top N directors who created the maximum number of movies in the given genre')
count = 1
for item in mylist:
    print(f"{count} : {item}")
    count += 1

# iii -> top `N` actors
print('----------------------------------------------------------------')
# 1. who starred in the maximum number of movies
def find_top_actors_by_num_movies(limit=10):
   
    top_actors = db.movies.aggregate([
        {"$unwind" : "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": pymongo.DESCENDING}},
        {"$limit": limit}
    ])
    # Return the top actors
    return list(top_actors)
mylist = find_top_actors_by_num_movies()
print('the top N actors who starred in the maximum number of movies')
count=1
for item in mylist :
    print(f"{count} : {item}")
    count+=1

print('----------------------------------------------------------------')

# 2. who starred in the maximum number of movies in a given year
def find_top_actors_by_num_movies_in_year(year, limit=10):
  
    top_actors = db.movies.aggregate([
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": pymongo.DESCENDING}},
        {"$limit": limit}
    ])
    # Return the top actors
    return list(top_actors)
mylist = find_top_actors_by_num_movies_in_year(2000)
print('the top N actors who starred in the maximum number of movies in the given year')
count = 1
for item in mylist:
    print(f"{count} : {item}")
    count += 1

print('----------------------------------------------------------------')
# 3. who starred in the maximum number of movies for a given genre
def find_top_actors_by_num_movies_in_genre(genre, limit=10):
  
    top_actors = db.movies.aggregate([
        {"$unwind": "$cast"},
        {"$unwind": "$genres"},
        {"$match": {"genres": genre}},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": pymongo.DESCENDING}},
        {"$limit": limit}
    ])
    # Return the top actors
    return list(top_actors)
mylist = find_top_actors_by_num_movies_in_genre("Action")
print('the top N actors who starred in the maximum number of movies in the given genre')
count = 1
for item in mylist:
    print(f"{count} : {item}")
    count += 1

print('----------------------------------------------------------------')
print('top `N` movies for each genre with the highest IMDB rating')

def find_top_n_movies_per_genre(n=10):
    pipeline=[
            {"$unwind":"$genres"},
            {"$group":{"_id":"$genres"}}
        ]
    for i in list(movies.aggregate(pipeline)):
            genre=i['_id']
            print("Genre: "+genre)
            pipeline=[
                {"$match":{"genres":genre}},
                {"$sort":{"imdb.rating":-1}},
                {"$match":{"imdb.rating":{"$ne":""}}},
                {"$project":{"_id":0,"title":1,"rating":"$imdb.rating"}},
                {"$limit":n}
            ] 
            pprint(list(movies.aggregate(pipeline)))
find_top_n_movies_per_genre()

# c. theatre collection
print('----------------------------------------------------------------')
# 1. Top 10 cities with the maximum number of theatres
def top_cities_with_max_theatres(n):
    pipeline = [
        {"$group": {"_id": "$location.address.city", "theatre_count": {"$sum": 1}}},
        {"$sort": {"theatre_count": -1}},
        {"$limit": n}
    ]
    top_cities = db.theatres.aggregate(pipeline)
    # Return the top cities with max number of theatres
    return list(top_cities)
mylist = top_cities_with_max_theatres(10)
print('Top 10 cities with the maximum number of theatres')
count=1
for item in mylist :
    print(f"{count} : {item}")
    count+=1

print('----------------------------------------------------------------')
print('top 10 theatres nearby given coordinates')
# 2. top 10 theatres nearby given coordinates
def top10theatersNear(cod):
    theaters.create_index([("location.geo", "2dsphere")])
    pprint(list(theaters.find(
        {
            "location.geo": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": cod
                    }}
            }
        }).limit(10)))

top10theatersNear([-111.0,33.430729])

