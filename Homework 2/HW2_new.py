# Import necessary libraries
import pandas as pd
import sqlite3
import os
import matplotlib.pyplot as plt
import datetime

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
# First predict the growth for users
users = pd.read_sql_query("SELECT * FROM users;", con)
users['month'] = users['created_at'].str[:7] # Get YYYY-MM part of the create_at value
users = users.sort_values('month')  # Sort the month values so they are in ascending order
monthly_users = users.groupby('month').size()
monthly_users = monthly_users.reset_index(name='new_users_for_month')
monthly_users['cumulative_new_users'] = monthly_users['new_users_for_month'].cumsum()

posts = pd.read_sql_query("SELECT * FROM posts;", con)
posts['month'] = posts['created_at'].str[:7] # Get YYYY-MM part of the create_at value
posts = posts.sort_values('month')  # Sort the month values so they are in ascending order
monthly_posts = posts.groupby('month').size()
monthly_posts = monthly_posts.reset_index(name='new_posts_for_month')
monthly_posts['cumulative_new_posts'] = monthly_posts['new_posts_for_month'].cumsum()

comments = pd.read_sql_query("SELECT * FROM comments;", con)
comments['month'] = comments['created_at'].str[:7] # Get YYYY-MM part of the create_at value
comments = comments.sort_values('month')  # Sort the month values so they are in ascending order
monthly_comments = comments.groupby('month').size()
monthly_comments = monthly_comments.reset_index(name='new_comments_for_month')
monthly_comments['cumulative_new_comments'] = monthly_comments['new_comments_for_month'].cumsum()

monthly_cumulative = monthly_users \
                        .merge(monthly_posts, left_on='month', right_on='month', how='outer') \
                        .fillna(0)
monthly_cumulative = monthly_cumulative \
                        .merge(monthly_comments, left_on='month', right_on='month', how='outer')\
                        .fillna(0)
monthly_cumulative['total_engagement_for_mth'] = monthly_cumulative['new_users_for_month'] \
                                                    + monthly_cumulative['new_posts_for_month'] \
                                                    + monthly_cumulative['new_comments_for_month']
monthly_cumulative['cumulative_engagement'] = monthly_cumulative['cumulative_new_users'] \
                                                + monthly_cumulative['cumulative_new_posts'] \
                                                + monthly_cumulative['cumulative_new_comments']

growth = monthly_cumulative.filter(['month', 'total_engagement_for_mth', 'cumulative_engagement'], axis=1)
print(growth)

# Get rid of outliers to get an accurate average over the timespan of the social media
# 1.5 IQR standard
q1 = growth['total_engagement_for_mth'].quantile(0.25)
q3 = growth['total_engagement_for_mth'].quantile(0.75)

iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
growth_filtered = growth[(growth['total_engagement_for_mth'] >= lower_bound) & (growth['total_engagement_for_mth'] <= upper_bound)]
total_cumulative_growth = growth_filtered['total_engagement_for_mth'].sum()
refined_avg = total_cumulative_growth / len(growth_filtered)
print(f'Refined average growth: {refined_avg} per month')

last_date = growth['month'].iloc[-1]
print(last_date)
last_cumulative_engagement = growth['cumulative_engagement'].iloc[-1]
newMonths = pd.date_range(last_date, periods=37, freq='ME', inclusive='right')   # 37 months, since current month is included
predictions = pd.DataFrame(columns=['month', 'cumulative_engagement'])

for i in range(len(newMonths)):
    month = str(newMonths[i])[:7]
    monthly_cumulative_val = last_cumulative_engagement
    last_cumulative_engagement = monthly_cumulative_val + refined_avg
    predictions.loc[i] = [month] + [monthly_cumulative_val]
    
# Plot the graph
plt.plot(growth['month'], growth['cumulative_engagement'])
plt.title('Growth of social media over the years')
plt.xlabel('months')
plt.ylabel('cumulative activity')
plt.xticks(rotation=60)
plt.grid(True)
plt.plot(predictions['month'], predictions['cumulative_engagement'])
# plt.show()

# Calculate the required servers:
growth_val_in_3yrs = predictions['cumulative_engagement'].iloc[-1]
# Get the ratio of current value to the future value
latest_growth_val = growth['cumulative_engagement'].iloc[-1]
growth_ratio = growth_val_in_3yrs / latest_growth_val
# Calculate the required servers with 20% redundancy margin
required_servers_no = 16 * growth_ratio * 1.20
print(f'Required no. of servers in 3 years: {required_servers_no}')
