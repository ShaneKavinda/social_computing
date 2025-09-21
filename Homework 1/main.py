# Import necessary libraries
import pandas as pd
import sqlite3
import os

# Clear the console at each run
os.system('clear')

DB_FILE = 'database.sqlite'

try:
    con = sqlite3.connect('database.sqlite')
    cur = con.cursor()
except Exception as e:
    print(f"uh oh! Could not connect to database because of: {e}")
# Homework Task 1.2
# Find users who haven't interacted with any posts, but may have followed other users

lurkers  = pd.read_sql_query('''
                    SELECT 
                             users.id, 
                             posts.user_id as posted, 
                             comments.user_id as commented, 
                             reactions.user_id as reacted, 
                             follows.followed_id as followed 
                        FROM users
                        LEFT JOIN posts
                            ON users.id = posts.user_id
                        LEFT JOIN comments
                            ON users.id = comments.user_id
                        LEFT JOIN reactions
                            ON users.id = reactions.user_id
                        LEFT JOIN follows
                            ON users.id = follows.follower_id
                        WHERE posts.user_id IS NULL
                            AND comments.user_id IS NULL
                            AND reactions.user_id IS NULL
                        GROUP BY users.id;
                ''', con)

df_lurkers = pd.DataFrame(lurkers)

lurkers_with_no_follows = df_lurkers['followed'].isna().sum()
print(f'number of lurkers with no follows: {lurkers_with_no_follows}')

lurkers_with_follows = df_lurkers['followed'].dropna().count()
print(f'lurkers who follow someone: {lurkers_with_follows}')

# Homework Task 1.3
# Users with most engagement
posts = pd.read_sql_query(" SELECT id as post_id, user_id as poster FROM posts", con)
reactions = pd.read_sql_query("SELECT post_id, user_id as reactor FROM reactions", con)
comments = pd.read_sql_query("SELECT post_id, user_id as commenter FROM comments",con)
users = pd.read_sql_query("SELECT id, username FROM users;", con)

df_posts = pd.DataFrame(posts)
df_reactions = pd.DataFrame(reactions)
df_comments = pd.DataFrame(comments)
df_users = pd.DataFrame(users)

posts_reactons_merge = df_posts.merge(df_reactions, left_on='post_id', right_on='post_id') \
                        .groupby(['poster']) \
                        .size() \
                        .reset_index(name='reaction_count')
posts_comments_merge = df_posts.merge(df_comments, left_on="post_id", right_on='post_id') \
                        .groupby(['poster']) \
                        .size() \
                        .reset_index(name="comment_count") 
post_engagement = posts_comments_merge.merge(posts_reactons_merge)
post_engagement['engagement'] = post_engagement['comment_count'] + post_engagement['reaction_count']

# Find the user IDs with most engagements
most_popular_users = post_engagement.nlargest(5, 'engagement').merge(df_users, left_on='poster', right_on='id')
print(most_popular_users)

# Homework Task 1.4 
# Identify spammers 
posts = pd.read_sql_query("SELECT * FROM posts", con)
df_posts = pd.DataFrame(posts)
duplicated_posts = df_posts.groupby(['content', 'user_id']).size().reset_index(name='repeat_count')
duplicated_posts = pd.DataFrame(duplicated_posts[duplicated_posts['repeat_count'] >=3 ])

post_spammers = pd.DataFrame(duplicated_posts['user_id'].unique())

comments = pd.read_sql_query("SELECT * FROM COMMENTS", con)
df_comments = pd.DataFrame(comments)
duplicated_comments = df_comments.groupby(['content', 'user_id']).size().reset_index(name='repeat_count')
duplicated_comments = pd.DataFrame(duplicated_comments[duplicated_comments['repeat_count'] >= 3])

comment_spammers = pd.DataFrame(duplicated_comments['user_id'].unique())

total_spammers = post_spammers.merge(comment_spammers, how='outer')

# print(f'total spammers: \n {total_spammers}')
users = pd.read_sql_query("SELECT id, username FROM users", con)
df_users = pd.DataFrame(users)

total_spammers = df_users.merge(total_spammers, left_on='id', right_on=0)
total_spammers = total_spammers['username']

print(f'total spammers: \n{total_spammers}')