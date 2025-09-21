import pandas as pd
import sqlite3
import os
import math
from datetime import timedelta
import time

# Clear the console at each run
os.system('clear')

DB_FILE = 'database.sqlite'

try:
    con = sqlite3.connect('database.sqlite')
    cur = con.cursor()
except Exception as e:
    print(f"uh oh! Could not connect to database because of: {e}")

# Task 1: List all tables in the database
table_names = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", con)
print("Tables in the database:")
print(table_names)

for table in table_names['name']:
    table_structure = pd.read_sql_query(f'''SELECT 
                                            name AS column_name, 
                                            type AS column_type 
                                        FROM 
                                            pragma_table_info('{table}');''', con)
    print(f'\nStructure of the table {table}: \n{table_structure}')
    print(f'contents of the {table}:')
    table = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 10;", con)
    print(table.head())

# Task 2: Find the top 5 user locations
try:
    top_5_locations = pd.read_sql_query("" \
        "SELECT location, COUNT(*) AS user_count FROM " \
        "users " \
        "GROUP BY location " \
        "ORDER BY COUNT(location) DESC " \
        "LIMIT 5", con)
    print(top_5_locations.head())
except Exception as e:
    print(f'Oh no! unexpected error occured: {e}')

# Task 3: Find the average age of the users, the youngest and the oldest user
try:
    age_stats = pd.read_sql_query("" \
    "SELECT ROUND(AVG(TIMEDIFF(DATE(), birthdate)),1) as avg_age, " \
    " JULIANDAY(DATE() - MAX(birthdate)) as youngest, " \
    " JULIANDAY(DATE() - MIN(birthdate)) as oldest " \
    "FROM users", con)
    print(f'\nage statistics: \n {age_stats}')
except Exception as e:
    print(f'Oh no! unexpected error occured: {e}')


# Task 4 : Follower statistics
try: 
    follow_stats = pd.read_sql_query("" \
    "SELECT u.username, COUNT(*) as follow_count FROM follows as f INNER JOIN users as u " \
    "ON f.followed_id = u.id " \
    "GROUP BY followed_id " \
    "ORDER BY COUNT(followed_id) DESC LIMIT 5", con)
    print(f'\nFollows: \n{follow_stats.head()}')
except Exception as e:
    print(f'Oh no! Unexpected error occured: {e}')

# Task 5 : Latest timestamps
# Commented as we do not want to run this code snippet every time
if False:
    try:
        latest_user = pd.read_sql_query('SELECT MAX(users.created_at) FROM users', con)
        latest_post = pd.read_sql_query('SELECT MAX(posts.created_at) FROM posts', con)
        latest_comments = pd.read_sql_query('SELECT MAX(comments.created_at) FROM comments', con)
        print(f'latest events: \n users: {latest_user.values[0]} \n posts: {latest_post.values[0]} \n comments: {latest_comments.values[0]}')
        time_offset_data_frame = pd.read_sql_query(""" SELECT
                                            strftime('%s', 'now') - strftime('%s',
                                                MAX(
                                                    (SELECT MAX(created_at) FROM users),
                                                    (SELECT MAX(created_at) FROM posts),
                                                    (SELECT MAX(created_at) FROM comments)
                                                )
                                            ) AS offset;""", con)
        
        time_offset = time_offset_data_frame['offset'].iloc[0]

        # The following code snippet runs only if the time offset is larger than 0
        if time_offset > 0:
            try:
                cursor = con.cursor()
                cursor.executescript(f"""BEGIN TRANSACTION;
                                UPDATE users SET created_at = DATETIME(created_at, '+{time_offset} seconds');
                                UPDATE posts SET created_at = DATETIME(created_at, '+{time_offset} seconds');
                                UPDATE comments SET created_at = DATETIME(created_at, '+{time_offset} seconds');
                                COMMIT;
                                """)
                con.commit()
                latest_user = pd.read_sql_query('SELECT MAX(users.created_at) FROM users', con)
                latest_post = pd.read_sql_query('SELECT MAX(posts.created_at) FROM posts', con)
                latest_comments = pd.read_sql_query('SELECT MAX(comments.created_at) FROM comments', con)
                print(f'latest events: \n users: {latest_user.values[0]} \n posts: {latest_post.values[0]} \n comments: {latest_comments.values[0]}')

            except Exception as e:
                print(f'Error: {e}')
    except Exception as e:
        print(f'Oh no! Something went wrong: {e}')

# Task 6
# Fetch invalid posts
invalid_post_ids = pd.read_sql_query(
        """
            SELECT 
                posts.id
            FROM posts INNER JOIN users
            ON posts.user_id = users.id
            WHERE posts.created_at < users.created_at;
        """
    , con) 
print(f"invalid post IDs: \n{invalid_post_ids}")
if invalid_post_ids.empty:
    print('No inconsistencies in posts found.')
else:
    print(f"invalid comment IDs: {invalid_post_ids}")

# Fetch invalid comments: comments before commenter's user account is created
invalid_comments = pd.read_sql_query(
        """
            SELECT 
                comments.id
            FROM comments INNER JOIN users
            ON comments.user_id = users.id
            WHERE comments.created_at < users.created_at;
        """
    , con) 
print(f"invalid post IDs: \n{invalid_comments}")
if invalid_comments.empty:
    print('''No comments created before commenter's user account were found''')
else:
    print(f"invalid comment IDs: {invalid_comments}")

# Fetch invalid comments: comments before the post is created
invalid_post_comments = pd.read_sql_query(
        """
            SELECT 
                comments.id
            FROM comments INNER JOIN posts
            ON comments.post_id = posts.id
            WHERE comments.created_at < posts.created_at;
        """
    , con) 
print(f"invalid post IDs: \n{invalid_post_comments}")
if invalid_post_comments.empty:
    print('No comments before the post is created were found')
else:
    print(f"invalid comment IDs: {invalid_post_comments}")

# Close database connection
con.close()


