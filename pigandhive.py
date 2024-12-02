import streamlit as st

# Set the title of the Streamlit app
st.title("Big Data Experimentation")

# Create sidebar for experiments
st.sidebar.title("Experiments")
experiment = st.sidebar.radio("Select Experiment", ["Pig Script: Oldest 5-Star Movie", "Hive Query: Most Popular Movie"])

# Experiment 1: Pig Script for Finding the Oldest 5-Star Movie
if experiment == "Pig Script: Oldest 5-Star Movie":
    st.header("Pig Script: Find the Oldest Movie with a 5-Star Rating")

    st.subheader("Dataset")
    st.write("""
    The dataset contains the following columns:
    - `movie_id`: Integer, unique identifier for each movie.
    - `movie_name`: Name of the movie (string).
    - `release_year`: The year the movie was released.
    - `rating`: Rating of the movie (integer), with 5 being the highest rating.
    """)

    st.subheader("Pig Script")
    pig_script = """
    -- Load the data
    movies = LOAD 'movies.txt' USING PigStorage(',') 
             AS (movie_id:INT, movie_name:CHARARRAY, release_year:INT, rating:INT);

    -- Filter movies with a 5-star rating
    five_star_movies = FILTER movies BY rating == 5;

    -- Find the movie with the oldest release year
    oldest_5star_movie = ORDER five_star_movies BY release_year ASC;

    -- Limit the result to the first movie (oldest)
    result = LIMIT oldest_5star_movie 1;

    -- Store or dump the result
    DUMP result;
    """
    st.code(pig_script, language="pig")

    st.subheader("Execution Command")
    st.write("To execute the Pig script, use the following command:")
    st.code("pig oldest_5star_movie.pig", language="bash")

    st.subheader("Explanation")
    st.write("""
    1. **Loading the Data**: The `movies.txt` file is loaded with columns `movie_id`, `movie_name`, `release_year`, and `rating`.
    2. **Filtering by Rating**: The script filters movies that have a 5-star rating.
    3. **Sorting by Release Year**: The filtered movies are sorted by `release_year` in ascending order (oldest first).
    4. **Limiting the Result**: Only the oldest movie (with a 5-star rating) is selected.
    5. **Execution**: Use the command `pig oldest_5star_movie.pig` to run the script.
    """)

# Experiment 2: Hive Query for Finding the Most Popular Movie
elif experiment == "Hive Query: Most Popular Movie":
    st.header("Hive Query: Find the Most Popular Movie")

    st.subheader("Dataset")
    st.write("""
    The dataset contains the following columns:
    - `movie_id`: Integer.
    - `movie_name`: String.
    - `release_year`: Integer.
    - `rating`: Integer.
    - `view_count`: Integer (this tracks the number of views for each movie).
    """)

    st.subheader("Hive Query")
    hive_query = """
    CREATE TABLE movies (
        movie_id INT,
        movie_name STRING,
        release_year INT,
        rating INT,
        view_count INT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',' 
    STORED AS TEXTFILE
    TBLPROPERTIES ("skip.header.line.count"="1");

    hdfs dfs -put movies.csv /user/hive/warehouse/movies/

    LOAD DATA INPATH '/user/hive/warehouse/movies/movies.csv' INTO TABLE movies;

    SELECT 
        movie_name, 
        SUM(view_count) AS total_views
    FROM 
        movies
    GROUP BY 
        movie_name
    ORDER BY 
        total_views DESC
    LIMIT 1;
    """
    st.code(hive_query, language="sql")

    st.subheader("Execution Command")
    st.write("To execute the Hive query, use the following command:")
    st.code("hive -f find_most_popular_movie.sql", language="bash")

    st.subheader("Explanation")
    st.write("""
    1. **Creating the Hive Table**: The `movies` table is created with columns `movie_id`, `movie_name`, `release_year`, `rating`, and `view_count`.
    2. **Loading the Data**: The dataset `movies.csv` is uploaded to HDFS and loaded into the `movies` table.
    3. **Querying for Most Popular Movie**: The query selects the `movie_name` and calculates the total views by summing `view_count` for each movie. It orders the results by total views in descending order.
    4. **Execution**: Use the command `hive -f find_most_popular_movie.sql` to run the query.
    """)
