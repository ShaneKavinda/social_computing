# Social Computing Exercise 2 tasks 7-9 (NoSQL-style solutions)
# @ Daniel Szabo 2025 <daniel.szabo@oulu.fi>
# University of Oulu

# SQLite library for python for reading, updating database
# See documentation at https://docs.python.org/3/library/sqlite3.html
import sqlite3

# Pandas library for data analysis and manipulation. Not specific for SQL, but provides support for importing data from SQLite database file.
# See documentation at https://pypi.org/project/pandas/
import pandas

# A very powerful visualisation library for python: https://pypi.org/project/matplotlib/
import matplotlib.pyplot as plt

# Optional for clearing the terminal at each run
import os
os.system('clear')

DB_FILE = "database.sqlite"
try:
    conn = sqlite3.connect(DB_FILE)
    
    # Use pandas as a quick way to load SQLite data into dataframes
    users = pandas.read_sql_query("SELECT * FROM users", conn)
    posts = pandas.read_sql_query("SELECT * FROM posts", conn)
    comments = pandas.read_sql_query("SELECT * FROM comments", conn)
    reactions = pandas.read_sql_query("SELECT * FROM reactions", conn)
    
    # Let's make sure our data is okay by quickly inspecting all the tables
    print(users)
    print(posts)
    print(comments)
    print(reactions)

except Exception as e:
    print(f"Uh oh '{e}'")
finally:
    if conn:
        conn.close()
        print("SQLite Database connection closed.")

# In-class Exercise, Task 7
# Join reactions with post authors' user_id for easy lookups below
reactions_with_posters = reactions.merge(
    posts[['id', 'user_id']],
    left_on='post_id', right_on='id',
    suffixes=('_reactor', '_poster')
)

# Drop self-reactions
reactions_with_posters = reactions_with_posters[
    reactions_with_posters['user_id_reactor'] != reactions_with_posters['user_id_poster']
]

# Count reactions of user A on user B's posts
reaction_counts = reactions_with_posters.groupby(
    ['user_id_reactor', 'user_id_poster']
).size().reset_index(name='reaction_count')

# Pairs with ≥5 reactions
potential_lurker_pairs = reaction_counts[reaction_counts['reaction_count'] >= 5]

# Find set of user pairs who where user A commented on posts of user B
comments_with_posters = comments.merge(
    posts[['id', 'user_id']],
    left_on='post_id', right_on='id',
    suffixes=('_commenter', '_poster')
)

commenting_pairs = comments_with_posters[['user_id_commenter', 'user_id_poster']].drop_duplicates()

# Final lurker pairs = in reactions but not in comments
lurkers = potential_lurker_pairs.merge(
    commenting_pairs,
    left_on=['user_id_reactor', 'user_id_poster'],
    right_on=['user_id_commenter', 'user_id_poster'],
    how='left', indicator=True
)

final_lurker_pairs = lurkers[lurkers['_merge'] == 'left_only'].drop(columns=['_merge', 'user_id_commenter'])

print(f'"lurker" pairs:\n{final_lurker_pairs}')

# In-class Exercise, Task 8 

# Group by post_id and count comments
counts = comments.groupby("post_id").size().reset_index(name="comment_count")
stats = {
    "min_comments": counts["comment_count"].min(),
    "avg_comments": f'{counts["comment_count"].mean():.2f}',
    "median_comments": counts["comment_count"].median(),
    "max_comments": counts["comment_count"].max()
}
print("Statistics for number of comments per post")
print(pandas.DataFrame([stats]))

# Identify post(s) with the most comments
top_posts = counts[counts["comment_count"] == counts["comment_count"].max()]["post_id"].tolist()
print(f"\nPost(s) with the most comments ({stats['max_comments']} comments)")
print(top_posts)

# In-class Exercise, Task 9 
# Find WildHorse's user_id from their username
wildhorse_row = users.loc[users['username'] == 'WildHorse']
wildhorse_id = wildhorse_row['id'].iloc[0]

# Get posts created by WildHorse
wildhorse_posts = posts.loc[posts['user_id'] == wildhorse_id, 'id']

# Filter comments on those posts
wh_comments = comments[comments['post_id'].isin(wildhorse_posts)].copy()

# Extract YYYY-MM
wh_comments['month'] = wh_comments['created_at'].str[:7]

# Count how many comments in each month
counts_per_month = wh_comments.groupby('month').size()

# Turn the result into a DataFrame with a proper column name
monthly_counts = counts_per_month.reset_index(name='monthly_comments')

# Sort by month (so the timeline is in order)
monthly_counts = monthly_counts.sort_values('month')

# Add a running total of all comments up to each month
monthly_counts['cumulative_comments'] = monthly_counts['monthly_comments'].cumsum()

print("Monthly and Cumulative Comments for @WildHorse")
print(monthly_counts)

# # Plot
# plt.plot(monthly_counts['month'], monthly_counts['cumulative_comments'])
# plt.title("Cumulative Comments on @WildHorse's Posts Over Time")
# plt.xlabel("Month")
# plt.ylabel("Total Cumulative Comments")
# plt.xticks(rotation=45)
# plt.grid(True)
# plt.show()

# Naive growth estimate
current_total = monthly_counts['cumulative_comments'].iloc[-1]
n_months = len(monthly_counts)
avg_growth = current_total / n_months
months_to_200 = (200 - current_total) / avg_growth

print(
    f"\nPrediction: Based on {avg_growth:.2f} comments/month historical growth, "
    f"it will take ≈{months_to_200:.2f} more months for total to reach 200."
)

# Let's refine our approach by removing outliers (following the 1.5 IQR standard)

# Compute Q1, Q3, and IQR
q1 = monthly_counts['monthly_comments'].quantile(0.25)
q3 = monthly_counts['monthly_comments'].quantile(0.75)
iqr = q3 - q1

# Define bounds for non-outliers
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

print(f"Outlier removal:\n\tLower bound: {lower_bound}\n\tUpper bound: {upper_bound}" )

# Filter monthly_counts to exclude outliers
filtered = monthly_counts[
    (monthly_counts['monthly_comments'] >= lower_bound) &
    (monthly_counts['monthly_comments'] <= upper_bound)
]

# Recalculate growth estimate without outliers
n_months = len(filtered)
filtered_total = filtered['monthly_comments'].sum()
avg_growth = filtered_total / n_months if n_months > 0 else 0
months_to_200 = (200 - current_total) / avg_growth if avg_growth > 0 else float('inf')

print(
    f"\nRefined Prediction (outliers removed): Based on {avg_growth:.2f} comments/month historical growth, "
    f"it will take ≈{months_to_200:.2f} more months for total to reach 200."
)

# Don't forget to close the database!!
conn.close()