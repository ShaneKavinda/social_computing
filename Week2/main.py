import pandas as pd
import sqlite3
import os

# Clear the console at each run
os.system('clear')

DB_FILE = 'database.sqlite'

try:
    con = sqlite3.connect('database.sqlite')
    cur = con.cursor()
    print('Successfully connected to the database...')
except Exception as e:
    print(f"uh oh! Could not connect to database because of: {e}")

# Task 7: Find lurkers
# Find out user pairs who have reacted more than 5 times, but not commented on the posts
posts = pd.read_sql_query(" SELECT id as post_id, user_id as poster FROM posts", con)
reactions = pd.read_sql_query("SELECT post_id, user_id as reactor FROM reactions", con)
comments = pd.read_sql_query("SELECT post_id, user_id as commenter FROM comments",con)

df_posts = pd.DataFrame(posts)
df_reactions = pd.DataFrame(reactions)
df_comments = pd.DataFrame(comments)

posts_reactons_merge = df_posts.merge(df_reactions, left_on='post_id', right_on='post_id') \
                        .groupby(['poster', 'reactor']) \
                        .size() \
                        .reset_index(name='reaction_count')
posts_comments_merge = df_posts.merge(df_comments, left_on="post_id", right_on='post_id') \
                        .groupby(['poster', 'commenter']) \
                        .size() \
                        .reset_index(name="comment_count") \
                        .drop_duplicates()

# Eliminate self-reaction pairs (poster reacting to their own post)
posts_reactons_merge = posts_reactons_merge[posts_reactons_merge['poster'] != posts_reactons_merge['reactor']]

# Filter poster-reactor pairs less than 5 reactions
potential_lurkers = posts_reactons_merge[posts_reactons_merge['reaction_count'] >= 5]

# merge potential lurkers and posts commenters; similar to Left join in SQL
lurker_commenter_merge = potential_lurkers.merge(
                            posts_comments_merge, 
                            left_on=['reactor', 'poster'], 
                            right_on=['commenter', 'poster'],
                            how='left',
                            indicator=True)

# Lurkers: filter only the users with reactions but no comments
no_comments_lurkers = lurker_commenter_merge[lurker_commenter_merge['_merge'] == 'left_only']

print(no_comments_lurkers)


# Task 8: minimum, maximum and averages of comments on posts
comments = pd.read_sql_query('SELECT * FROM comments', con)
df_comments = pd.DataFrame(comments)
post_comments = df_comments.groupby('post_id').size().reset_index(name='comment_count')

# Max comments 
print(f'maximum number of comments: \n {post_comments[post_comments['comment_count'] == post_comments['comment_count'].max()]}')

# Min comments 
print(f'minimum number of comments: \n {post_comments['comment_count'].min()}')

# Average number of comments per post
print(f'Average number of comments: \n {post_comments['comment_count'].mean()}')

# Mediaun number of comments per post
print(f'Median number of comments: \n {post_comments['comment_count'].median()}')