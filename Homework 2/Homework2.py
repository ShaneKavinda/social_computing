# Import necessary libraries
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
except Exception as e:
    print(f"uh oh! Could not connect to database because of: {e}")

# Homework Task 2.1

# Method measure growth by getting the sum of new users, posts and comments
users = pd.read_sql_query('SELECT * FROM users', con)
posts = pd.read_sql_query('SELECT * FROM posts', con)
comments = pd.read_sql_query('SELECT * FROM comments', con)

users.merge(posts, left_on='id', right_on='user_id', how='outer')
print()



# First predict the growth for users
users = pd.read_sql_query("SELECT * FROM users;", con)
users['month'] = users['created_at'].str[:7] # Get YYYY-MM part of the create_at value
users = users.sort_values('month')  # Sort the month values so they are in ascending order
monthly_users = users.groupby('month').size()
monthly_users = monthly_users.reset_index(name='new_users_for_month')
monthly_users['cumulative_new_users'] = monthly_users['new_users_for_month'].cumsum()

# Filter outliers (using 1.5 IQR standard)
q1 = monthly_users['new_users_for_month'].quantile(0.25)
q3 = monthly_users['new_users_for_month'].quantile(0.75)
iqr = q3 - q1

lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
print(f"upper bound for new users: {upper_bound}, lower bound for new users: {lower_bound}")

monthly_users_filtered = monthly_users[(monthly_users['new_users_for_month'] >= lower_bound) & (monthly_users['new_users_for_month'] <= upper_bound)]
total_filtered_new_users = monthly_users_filtered['new_users_for_month'].sum()
filtered_monthly_user_avg = total_filtered_new_users / len(monthly_users_filtered)
print(f'average monthly new users: {filtered_monthly_user_avg}')
users_3yrs = filtered_monthly_user_avg * 36 +  monthly_users['cumulative_new_users'].iloc[-1] # No. of new users that will join after 36 months, based on the calculated average + already existing users as of now
print(f'No of. users in 36 months(3 years): {users_3yrs}')

ratio_3yrs_to_now = users_3yrs / monthly_users['cumulative_new_users'].iloc[-1] # ratio of users that will exist in 3 years and current number of users
servers_required = 16 * ratio_3yrs_to_now # Calculating the number of servers that will be required in 3 years
servers_required_user_growth = round(servers_required * 1.20) # Add a 20% redundancy margin
print(f'No. of servers that will be required in 3 years (with 20% redundancy margin): {servers_required_user_growth}')

# Predict the growth for user comments
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
comments = pd.read_sql_query("SELECT * FROM comments;", con)
comments['month'] = comments['created_at'].str[:7] # Get YYYY-MM part of the create_at value
comments = comments.sort_values('month')  # Sort the month values so they are in ascending order
monthly_comments = comments.groupby('month').size()
monthly_comments = monthly_comments.reset_index(name='new_comments_for_month')
monthly_comments['cumulative_new_comments'] = monthly_comments['new_comments_for_month'].cumsum()

# Filter outliers (using 1.5 IQR standard)
q1 = monthly_comments['new_comments_for_month'].quantile(0.25)
q3 = monthly_comments['new_comments_for_month'].quantile(0.75)
iqr = q3 - q1

lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
print(f"upper bound for new comments: {upper_bound}, lower bound for new comments: {lower_bound}")

monthly_comments_filtered = monthly_comments[(monthly_comments['new_comments_for_month'] >= lower_bound) & (monthly_comments['new_comments_for_month'] <= upper_bound)]
total_filtered_new_comments = monthly_comments_filtered['new_comments_for_month'].sum()
filtered_monthly_user_avg = total_filtered_new_comments / len(monthly_comments_filtered)
print(f'average monthly new comments: {filtered_monthly_user_avg}')
comments_3yrs = filtered_monthly_user_avg * 36 +  monthly_comments['cumulative_new_comments'].iloc[-1] # No. of new comments that will join after 36 months, based on the calculated average + already existing comments as of now
print(f'No of. comments in 36 months(3 years): {comments_3yrs}')

ratio_3yrs_to_now = comments_3yrs / monthly_comments['cumulative_new_comments'].iloc[-1] # ratio of comments that will exist in 3 years and current number of comments
servers_required = 16 * ratio_3yrs_to_now # Calculating the number of servers that will be required in 3 years
servers_required_comments_growth = round(servers_required * 1.20) # Add a 20% redundancy margin
print(f'No. of servers that will be required in 3 years (with 20% redundancy margin): {servers_required_comments_growth}')

# Predict the growth for user posts
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
posts = pd.read_sql_query("SELECT * FROM posts;", con)
posts['month'] = posts['created_at'].str[:7] # Get YYYY-MM part of the create_at value
posts = posts.sort_values('month')  # Sort the month values so they are in ascending order
monthly_posts = posts.groupby('month').size()
monthly_posts = monthly_posts.reset_index(name='new_posts_for_month')
monthly_posts['cumulative_new_posts'] = monthly_posts['new_posts_for_month'].cumsum()

# Filter outliers (using 1.5 IQR standard)
q1 = monthly_posts['new_posts_for_month'].quantile(0.25)
q3 = monthly_posts['new_posts_for_month'].quantile(0.75)
iqr = q3 - q1

lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
print(f"upper bound for new posts: {upper_bound}, lower bound for new posts: {lower_bound}")

monthly_posts_filtered = monthly_posts[(monthly_posts['new_posts_for_month'] >= lower_bound) & (monthly_posts['new_posts_for_month'] <= upper_bound)]
total_filtered_new_posts = monthly_posts_filtered['new_posts_for_month'].sum()
filtered_monthly_user_avg = total_filtered_new_posts / len(monthly_posts_filtered)
print(f'average monthly new posts: {filtered_monthly_user_avg}')
posts_3yrs = filtered_monthly_user_avg * 36 +  monthly_posts['cumulative_new_posts'].iloc[-1] # No. of new posts that will join after 36 months, based on the calculated average + already existing posts as of now
print(f'No of. posts in 36 months(3 years): {posts_3yrs}')

ratio_3yrs_to_now = posts_3yrs / monthly_posts['cumulative_new_posts'].iloc[-1] # ratio of posts that will exist in 3 years and current number of posts
servers_required = 16 * ratio_3yrs_to_now # Calculating the number of servers that will be required in 3 years
servers_required_posts_growth = round(servers_required * 1.20) # Add a 20% redundancy margin
print(f'No. of servers that will be required in 3 years (with 20% redundancy margin): {servers_required_posts_growth}')

# Predict the growth for user reactions
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
reactions = pd.read_sql_query("SELECT * FROM reactions;", con)
total_current_reactions = len(reactions)
earliest_user_date_df = pd.read_sql_query('SELECT created_at FROM users ORDER BY created_at ASC LIMIT 1;', con)
earliest_user_date_str = earliest_user_date_df.iloc[0]['created_at']
earliest_user_date = pd.to_datetime(earliest_user_date_str)

current_date = pd.to_datetime('now')

time_period = (current_date.to_julian_date() - earliest_user_date.to_julian_date())
print(f'time period social media has existed in days: {time_period}')

reactions_per_day = total_current_reactions / time_period
print(f'reactions per day: {reactions_per_day}')

reactions_in_3yrs = total_current_reactions + reactions_per_day * 365.25 * 3
print(f'reactions in 3 years: {reactions_in_3yrs}')

ratio_current_reactions_to_3yrs = reactions_in_3yrs / total_current_reactions
required_servers = 16 * ratio_current_reactions_to_3yrs
servers_required_reactions_growth = required_servers * 1.20
print(f'Servers required for reactions growth: {servers_required_reactions_growth}')

# Predict the growth for user follows
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
follows = pd.read_sql_query("SELECT * FROM follows;", con)
total_current_follows = len(follows)
earliest_user_date_df = pd.read_sql_query('SELECT created_at FROM users ORDER BY created_at ASC LIMIT 1;', con)
earliest_user_date_str = earliest_user_date_df.iloc[0]['created_at']
earliest_user_date = pd.to_datetime(earliest_user_date_str)

current_date = pd.to_datetime('now')

time_period = (current_date.to_julian_date() - earliest_user_date.to_julian_date())
print(f'time period social media has existed in days: {time_period}')

follows_per_day = total_current_follows / time_period
print(f'follows per day: {follows_per_day}')

follows_in_3yrs = total_current_follows + follows_per_day * 365.25 * 3
print(f'follows in 3 years: {follows_in_3yrs}')

ratio_current_follows_to_3yrs = follows_in_3yrs / total_current_follows
required_servers = 16 * ratio_current_follows_to_3yrs
servers_required_follows_growth = required_servers * 1.20
print(f'Servers required for follows growth: {servers_required_follows_growth}')

print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
highest_predicted_servers = max(servers_required_user_growth, \
                                servers_required_posts_growth, \
                                servers_required_reactions_growth, \
                                servers_required_follows_growth, \
                                servers_required_comments_growth)
print(f'Highest predicted no. of required_servers: {highest_predicted_servers}')

###
# Task 2.2: Identify most viral posts
# Metric: The sum of comments and reactions for the post is counted 

# Finding posts with most engagement
posts = pd.read_sql_query(" SELECT * FROM posts", con)
reactions = pd.read_sql_query("SELECT * FROM reactions", con)
comments = pd.read_sql_query("SELECT * FROM comments",con)


df_posts = pd.DataFrame(posts)
df_reactions = pd.DataFrame(reactions)
df_comments = pd.DataFrame(comments)

posts_reactons_merge = df_posts.merge(df_reactions, left_on='id', right_on='post_id') \
                        .groupby(['post_id']) \
                        .size() \
                        .reset_index(name='reaction_count')
posts_comments_merge = df_posts.merge(df_comments, left_on="id", right_on='post_id') \
                        .groupby(['post_id']) \
                        .size() \
                        .reset_index(name="comment_count") 
post_engagement = posts_comments_merge.merge(posts_reactons_merge)
post_engagement['engagement'] = post_engagement['comment_count'] + post_engagement['reaction_count']

# Find the posts with most engagements
most_viral_posts = post_engagement.nlargest(5, 'engagement')
print('Most viral posts: ')
print(most_viral_posts)

# Task 2.3
posts = pd.read_sql_query(" SELECT * FROM posts", con)
posts['post_creation'] = posts['created_at']
comments = pd.read_sql_query("SELECT * FROM comments",con)

comments = comments.groupby('post_id').agg({
                                        'post_id' : 'first',
                                        'content' : 'first',
                                        'created_at' : 'min'
                                    })
comments['first_comment_time'] = comments['created_at']
post_comment_merge = posts.merge(comments, left_on='id', right_on=comments['post_id'])
first_engagement_times = pd.to_datetime(post_comment_merge['first_comment_time']) - pd.to_datetime(post_comment_merge['post_creation'])
print('first engagement times for posts: ')
print(first_engagement_times.head())
avg_time_till_first_engagement = pd.DataFrame(first_engagement_times).mean()
print(f'Average time till first engagement: {avg_time_till_first_engagement}')

# Task 2,4
# Users with most engagement
posters_and_commenters = pd.read_sql_query('SELECT DISTINCT posts.user_id AS posters, ' \
'                                               comments.user_id, COUNT(*) FROM posts ' \
'                                               INNER JOIN comments ' \
'                                                  ON posts.id = comments.post_id ' \
'                                               GROUP BY posts.user_id, comments.user_id;', con)
df_posters_and_commenters = pd.DataFrame(posters_and_commenters)
print(df_posters_and_commenters)

posters_and_reactors = pd.read_sql_query('SELECT DISTINCT posts.user_id AS posters, ' \
'                                           reactions.user_id, COUNT(*) FROM posts ' \
'                                           INNER JOIN reactions ' \
'                                               ON posts.id = reactions.post_id ' \
'                                           GROUP BY posts.user_id, reactions.user_id;', con)
df_posters_and_reactors = pd.DataFrame(posters_and_reactors)
print(df_posters_and_reactors)

user_engagement_pairs = df_posters_and_commenters.merge(df_posters_and_reactors, on=['posters','user_id'], how='outer', suffixes=('_comm', '_reac')).fillna(0)
user_engagement_pairs['total_engagement']  = user_engagement_pairs['COUNT(*)_comm']  + user_engagement_pairs['COUNT(*)_reac'] 

# Get rid of commutative pairs 
# e.g. (A, B) and (B, A) should not be counted twice
print(user_engagement_pairs)
# get the minimum of the pair and use that to sort, e.g: (2,5) and (5, 2), we keep only (2, 5) using DataFrame.apply() method
user_engagement_pairs['pairs'] = user_engagement_pairs.apply(
                                            lambda row: (min(row['posters'], row['user_id']) , max(row['posters'], row['user_id'])) , axis=1)
# Group by user pairs
user_engagement_pairs = user_engagement_pairs.groupby('pairs')['total_engagement'].sum().reset_index()
print(user_engagement_pairs.head())

top_3_user_engagement_pairs = user_engagement_pairs.nlargest(3, 'total_engagement')
print('User pairs with most engagement: ')
users = pd.read_sql_query('SELECT id, username FROM users;', con)
for pair in top_3_user_engagement_pairs['pairs']:
    user1 = users[users['id'] == int(pair[0])]['username'].iloc[0]
    user2 = users[users['id'] == int(pair[1])]['username'].iloc[0]
    print(f'{user1}, {user2}')

