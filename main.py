from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, OMDB_API_KEYS, IMDB_RATINGS_JSON
from utils import fetch_imdb_rating, load_ratings_from_json, save_ratings_to_json, append_to_json_file

# Connect to Neo4j
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def get_movies_to_update(tx, limit):
    """Query Neo4j for movies with NULL imdbRating."""
    query = """
    MATCH (m:Movie)
    WHERE m.imdbRating IS NULL
    RETURN m.imdbId
    ORDER BY m.imdbId DESC
    LIMIT $limit
    """
    result = tx.run(query, limit=limit)
    return [record["m.imdbId"] for record in result]

def update_movie_rating(tx, imdb_id, imdb_rating):
    """Update the IMDb rating for a movie in Neo4j."""
    query = """
    MATCH (m:Movie {imdbId: $imdb_id})
    SET m.imdbRating = $imdb_rating
    """
    tx.run(query, imdb_id=imdb_id, imdb_rating=imdb_rating)

def process_movies():
    """Fetch movies, update IMDb ratings, and save them to a JSON file."""
    # imdb_ratings = load_ratings_from_json()  # Load existing ratings from JSON
    with driver.session(database="courseProject2024.db") as session:
        total_processed = 0

        for api_key in OMDB_API_KEYS:
            # Fetch 1000 movies for the current API key
            movies_to_update = session.read_transaction(get_movies_to_update, 1000)

            if not movies_to_update:
                print("No more movies to process.")
                break

            for imdb_id in movies_to_update:
                # Skip if already in JSON
                # if imdb_id in imdb_ratings:
                #     print(f"IMDb rating for {imdb_id} already exists in JSON.")
                #     continue

                print(f"Fetching IMDb rating for: tt{imdb_id}")
                imdb_rating = fetch_imdb_rating(imdb_id, api_key)
                append_to_json_file(IMDB_RATINGS_JSON, {imdb_id: imdb_rating})

                if imdb_rating:
                    # imdb_ratings[imdb_id] = imdb_rating  # Add to JSON
                    session.write_transaction(update_movie_rating, imdb_id, imdb_rating)
                    print(f"Updated: tt{imdb_id} with rating {imdb_rating}")
                else:
                    print(f"Failed to fetch IMDb rating for: tt{imdb_id}")

            total_processed += len(movies_to_update)
            print(f"Processed {total_processed} movies so far.")


# Run the process
if __name__ == "__main__":
    process_movies()

# Close the Neo4j connection
driver.close()
