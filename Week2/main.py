import pandas as pd
import sqlite3
import os
import matplotlib.pyplot as plt

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

# Task 9
users = pd.read_sql_query("SELECT * FROM users", con)
wildHorse_user_acc = users.loc[users['username'] == 'WildHorse']
wildhorse_id = wildHorse_user_acc['id'].iloc[0] # Get ID of username WildHorse

posts = pd.read_sql_query("SELECT * FROM posts;", con)
WH_post_Ids = posts.loc[posts['user_id'] == wildhorse_id]['id']  # Get post IDs by WildHorse

comments = pd.read_sql_query("SELECT * FROM comments;", con)
WH_comments =  comments[comments['post_id'].isin(WH_post_Ids)] # Get comments on WildHorse's posts

#Extract YYYY-MM date values
WH_comments['month'] = WH_comments['created_at'].str[:7]

# Group comments by month
WH_comments = WH_comments.groupby('month').size()

# Rename the column and sort by month
monthly_counts = WH_comments.reset_index(name='monthly_comments').sort_values('month')

# Get the cumulative comments count
monthly_counts['cumulative'] = monthly_counts['monthly_comments'].cumsum()
print(monthly_counts)

# Plot the graph
# plt.plot(monthly_counts['month'], monthly_counts['cumilative'])
# plt.title('Cumulative comments per month for user WildHorse')
# plt.xlabel('Month')
# plt.ylabel('Cumilative no. of comments')
# plt.xticks(rotation=45)
# plt.grid(True)
# plt.show()

# Basic growth estimate 
current_total = monthly_counts['cumulative'].iloc[-1]
no_of_months = len(monthly_counts)

avg_comments_per_mth = current_total / no_of_months
months_til_200 = round(200 / avg_comments_per_mth)

print(f"Time to reach 200 comments based on average number of comments: {months_til_200}")

# Refined apporach: get rid of outliers (1.5 IQR standard)
# refer: https://www.youtube.com/watch?v=rZJbj2I-_Ek

# Compute Q1, Q3 and IQR
Q1 = monthly_counts['monthly_comments'].quantile(0.25)
Q3 = monthly_counts['monthly_comments'].quantile(0.75)
IQR = Q3 - Q1

# lower bound and upper bound for acceptable values
l_bound = Q1 - (1.5 * IQR)
u_bound = Q3 + (1.5 * IQR)

print(f'lower bound: {l_bound}, upper bound: {u_bound}')

# Filter months which comment count are outliers
filterd_comment_counts = monthly_counts[(monthly_counts['monthly_comments'] >= l_bound) & (monthly_counts['monthly_comments'] <= u_bound)]
print(filterd_comment_counts)

# Recalculate average with outliers removed
filtered_no_of_months = len(filterd_comment_counts)
filterd_total = filterd_comment_counts['monthly_comments'].sum()

new_monthly_avg = filterd_total / filtered_no_of_months
print(f'new average: {new_monthly_avg}')

time_till_200 = (200 - current_total) / new_monthly_avg
print(f'Time till 200 comments = {time_till_200} months.')

con.close()


