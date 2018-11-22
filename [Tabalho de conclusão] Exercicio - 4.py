
# coding: utf-8

# In[8]:


get_ipython().magic(u'matplotlib inline')
import warnings
warnings.filterwarnings('ignore')

# imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# In[9]:


# Ajustando o tamanho dos gráficos que serão gerados
plt.rcParams['figure.figsize'] = (12, 6)

# Definindo o estilo visual dos gráficos
plt.style.use('bmh')

# Create URL to JSON file (alternatively this can be a filepath)
url = '/home/nbuser/library/telemetry.json'

# Load the first sheet of the JSON file into a data frame
df = pd.read_json(url, lines=True)

# View the first ten rows
df.head(10)


# In[10]:



car3 = df.loc[df['Car'] == 3]
car3.head()


# In[11]:


car4 = df.loc[df['Car'] == 4]
car4.head()


# In[12]:


car3Telemetry = pd.DataFrame(item for item in car3['telemetry'])
car3Telemetry.head()


# In[15]:


car4Telemetry = pd.DataFrame(item for item in car4['telemetry'])
car4Telemetry.head()


# In[23]:


fig, ax = plt.subplots()
plt.plot(car3Telemetry['Speed'].values * 35/9)
plt.plot(car4Telemetry['Speed'].values * 35/9, 'r')
ax.set_ylabel('Speedy - Km/h')
ax.set_xlabel('time')
ax.set_title('Speed comparison between car 3 in blue & 4 in red')


# In[29]:


fig, ax = plt.subplots()
ax.set_title('Comparison between RPM and Gear of car 3')
plt.plot(car3Telemetry['RPM'].values)
ax.set_ylabel('RPM')
ax.set_xlabel('time')
ax2 = ax.twinx()
ax2.set_ylabel('Gear')
plt.plot(car3Telemetry['Gear'].values, 'r')


# In[30]:


fig, ax = plt.subplots()
ax.set_title('Avarage Lap time by car')
for i in df['Car'].unique():
    carx = df.loc[df['Car'] == i]
    carxTelemetry = pd.DataFrame(item for item in carx['telemetry'])
    rects1 = plt.bar(i, carxTelemetry['LapTime'].mean(), 0.35, color='y')
    
ax.set_ylabel('Avarage Lap time')
ax.set_xlabel('Car nº')
plt.show()

