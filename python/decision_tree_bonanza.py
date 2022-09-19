from turtle import onclick
import pandas as pd
import numpy as np
from sklearn import tree
import streamlit as st

dtype = {
    'INST': int,
    'CORE': int,
    'RAM': int,
    'Time-q64-v2.4': np.float64,
    'Time-q70-v2.4': np.float64,
    'Time-q82-v2.4': np.float64,
    'Time-Avg': np.float64,
    'Cost-q64-v2.4': np.float64,
    'Cost-q70-v2.4': np.float64,
    'Cost-q82-v2.4': np.float64,
    'Cost-Avg': np.float64
}

# Read original CSV
df = pd.read_csv("F:\\Stuff\\Thesis\\Results.csv", delimiter=';', decimal=',', dtype=dtype)
# One hot representation for types of queries
df2 = pd.melt(df, id_vars=['INST', 'CORE', 'RAM'], value_vars=['Time-q64-v2.4', 'Time-q70-v2.4', 'Time-q82-v2.4', 'Time-Avg'], var_name='type', value_name='time')
df2 = pd.concat([df2, pd.get_dummies(df2['type'])], axis=1)
df2 = df2[df2.columns[~df2.columns.isin(['type'])]]
# Values for cost per time unit
vCPUsec = 1.461777E-5
GBsec = 0.160513888888E-5
# Reintroduce cost column
df2['cost'] = df2['time'] * (df2['INST'] * df2['CORE'] * vCPUsec + df2['RAM'] * GBsec)
# Copy dataframe in order to iteratively change column values later
fdf = df2.copy()

st.write('Configuration')
# Pick query or Avg
query = st.selectbox('Load Type', ['Balanced', 'Network Shuffle Heavy', 'CPU Heavy', 'I/O Heavy'])
if query == 'Balanced':
    query = 'Avg'
elif query == 'Network Shuffle Heavy':
    query = 'q64-v2.4'
elif query == 'CPU Heavy':
    query = 'q70-v2.4'
else:
    query = 'q82-v2.4'

# Scale = 10 for default dataset otherwise change to 100, 1000 and so on to get linear approximation
scale = st.select_slider('Dataset Size', options=[10, 100, 1000, 10000], format_func=str)

fdf['cost'] = df2['cost']*scale/10
fdf['time'] = df2['time']*scale/10

# Split to X and y
X = fdf.loc[fdf[f'Time-{query}'] == 1]
y = fdf.loc[fdf[f'Time-{query}'] == 1][fdf.columns[:3]]

# Declare three decision trees
regr = tree.DecisionTreeRegressor()
regr2 = tree.DecisionTreeRegressor()
regr3 = tree.DecisionTreeRegressor()

# Fit the trees to the data
time_to_settings = regr.fit(X['time'].array.reshape(-1,1), y)
money_to_settings = regr2.fit(X['cost'].array.reshape(-1,1), y)
settings_to_time = regr3.fit(y, X['time'].array.reshape(-1,1))

########################################################
def find_config():
    global rinput, x
    if rinput == 'Runtime (seconds)':
        i, c, r = time_to_settings.predict([[x]])[0]
    else:
        i, c, r = money_to_settings.predict([[x]])[0]
    return(f'Instances: {int(i)} Cores: {int(c)} RAM: {int(r)}')

st.write('Find Configuration')

rinput = st.radio('Input', ['Runtime (seconds)', 'Cost'])

x = st.number_input('Constraint Value')
st.write('Valid Configuration')
st.write(find_config())
########################################################
def find_time_cost():
    global w, cor, ra
    time = settings_to_time.predict([[w, cor, ra]])[0]
    return(f'Approximated Time: {time:.2f}s, Calculated Cost: {time*(w*cor*vCPUsec+ra*GBsec)}')
st.write('')
st.write('Calculate Time & Cost from Configuration')

w = st.select_slider('Workers', [1, 2, 3, 4])
cor = st.select_slider('Cores (per worker)', [1, 2, 3])
ra = st.select_slider('RAM (per worker)', [1, 2, 3])

st.button('Calculate', on_click=find_time_cost)
st.write(find_time_cost())