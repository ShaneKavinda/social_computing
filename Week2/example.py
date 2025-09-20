import pandas as pd
import matplotlib.pyplot as plt

data = {
    'name' : ['Matti', 'Bob', 'Charlie', 'David'],
    'age' : [25, 32, 28, 45],
    'city' : ['Oulu', 'Paris', 'London', 'Tokyo']
}

df = pd.DataFrame(data)

df.info()

print(df.head())
print(df.tail(2))

ages = df['age']
name_and_city = df[['name','city']]

over_30 = df[df['age'] > 30]

df['age_next_year'] = df['age'] + 1

average_age = df['age'].mean()


data = {
'x_values': [0, 1, 2, 3, 4, 5],
'y_values': [0, 1, 4, 9, 16, 25]
}
df = pd.DataFrame(data)


plt.title('Y = X^2')
plt.xlabel('X')
plt.ylabel('Y')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
# plt.plot(df['x_values'], df['y_values'])

plt.scatter(df['x_values'], df['y_values'])
# plt.bar(df['x_values'], df['y_values'])

plt.show()