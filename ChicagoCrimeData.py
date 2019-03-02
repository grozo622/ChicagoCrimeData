
# coding: utf-8

# Objectives:
# 
# *Use SQL to execute different queries to retrieve data from Chicago Crime dataset and Police statins dataset
# *Use Geospatial queries to locate police stations and gun related crimes (with arrest or no arrest) in every district on Choropleth map
# *Use Geospatial queries to provide descriptive stat for every district on Choropleth map
# *Use Geospatial queries to locate the Block that is the furthest (Maximum Distance) from the police station that has gun related crime resulted in arrest

# Chicago Crimes Dataset
# 
# The CSV file for crimes dataset for the city of Chicago is obtained from the data portal for the city of Chicago.
# 
# Here is the link for the city of Chicago data portal City of Chicago Data Portal.

# Loading the Dataset CSV file
# 
# Three datasets are needed...
# 
# *The Chicago police stations in every district
# *The Boundaries.geojson data for district boundries
# *The Crimes dataset 
# 
# Lets load the CSV file into a DataFrame object and see the nature of the data that we have.
# 
# Complete description of the dataset can be found on Chicago city data portal.

# *Plot on Choropleth map the districts and their Violent Crimes
# *Plot on Choropleth map the districts and their Gun related crimes
# *Which district is the crime capital of Chicago districts?
# *What the crime density per district?
# *Plot on Choropleth map those gun related crimes that resulted in arrests
# *Plot on Choropleth map the gun related crime that is in the farthest Block from the policy stattion for every district

# In[1]:


import folium
from folium import plugins
from folium.plugins import MarkerCluster
import psycopg2
import csv
import pandas as pd
import json
from area import area

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 


# In[ ]:



db_connection = psycopg2.connect(host='129.105.208.226',dbname="chicago_crimes", user="NetID" , password="password")


cursor = db_connection.cursor()


# In[ ]:


cursor.execute("SELECT district, count(district) from crimes GROUP BY district ORDER BY district ASC")
rows=cursor.fetchall()


# In[ ]:


#Query 1: calc total # of crimes in every district/plot
#on Choropleth map

crimes_per_district = pd.DataFrame(rows, columns=['dist_num','number_of_crimes'])
crimes_per_district['dist_num'] = crimes_per_district['dist_num'].astype(str)

crimes_per_district.head()


# In[ ]:


total_number_of_crimes_per_district_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)


# In[ ]:


total_number_of_crimes_per_district_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='OrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = crimes_per_district,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'number_of_crimes']
              )


# In[ ]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")
police_stations = cursor.fetchall()

for police_station in police_stations:
    police_station_location = (police_station[0],police_station[1])
    cursor.execute("SELECT district, count(district) from crimes where district= %s GROUP BY district",[police_station[2]])
    districts_crime_numbers = cursor.fetchall()
    for district in districts_crime_numbers:
        folium.Marker(location = police_station_location,popup=folium.Popup(html="District No : %s  has   Total Number of Crimes:%s" %district ,max_width=450)).add_to(total_number_of_crimes_per_district_map)

#total_number_of_crimes_per_district_map


# In[ ]:


#Query 2: calc total # of violent crimes in every district/
#plot in a table on Choropleth map

violent_crime_categories='THEFT','ASSAULT','ROBBERY','KIDNAPPING','CRIM SEXUAL ASSAULT','BATTERY','MURDER'

cursor.execute("SELECT district, count(district) from crimes where PRIMARY_TYPE in %s GROUP BY district",[violent_crime_categories])
rows=cursor.fetchall()
violent_crime_data=pd.DataFrame(rows, columns=['district_num','number_of_violent_crimes'])
violent_crime_data['district_num'] = violent_crime_data['district_num'].astype(str)
violent_crime_data


# In[ ]:


violent_crimes_per_district_map= folium.Map(location =(41.8781, -87.6298),zoom_start=11)
violent_crimes_per_district_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = violent_crime_data,
              key_on='feature.properties.dist_num',
              columns = ['district_num', 'number_of_violent_crimes'],
              legend_name="VOILENT CRIME MAP"
              )


# In[ ]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")
police_stations = cursor.fetchall()

for police_station in police_stations:
    police_station_location =(police_station[0],police_station[1])
    cursor.execute("SELECT PRIMARY_TYPE, count(PRIMARY_TYPE) from crimes where district =%s AND PRIMARY_TYPE in %s GROUP BY PRIMARY_TYPE",[police_station[2],violent_crime_categories])
    data = cursor.fetchall()
    violent_crimes_per_district_df = pd.DataFrame(data, columns=['Description', 'Number of Violent Crimes'])
    header = violent_crimes_per_district_df.to_html(classes='table table-striped table-hover table-condensed table-responsive')
    folium.Marker(location=police_station_location, popup=folium.Popup(html="District Number %s - Violent Crimes %s" %(police_station[2],header))).add_to(violent_crimes_per_district_map)

#violent_crimes_per_district_map


# In[ ]:


#Query 3: calc total # of gun related violent crimes
#in every district/plot in table on Choropleth map

#first create dataframe of gun crimes per district to
#get an idea of the # of gun crimes per district

gun='%GUN%'
cursor.execute("SELECT district, count(district) from crimes where DESCRIPTION::text LIKE %s GROUP BY district",[gun])
districts_gun_violent_crimes = cursor.fetchall()
districts_gun_violent_crimes_df = pd.DataFrame(districts_gun_violent_crimes, columns=['dist_num','gun_crimes'])
districts_gun_violent_crimes_df['dist_num'] = districts_gun_violent_crimes_df['dist_num'].astype(str)
districts_gun_violent_crimes_df


# In[ ]:


districts_gun_violent_crimes_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)
districts_gun_violent_crimes_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = districts_gun_violent_crimes_df,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'gun_crimes'],
              legend_name="GUN CRIME"
              )


# In[ ]:


#then, create dataframe of the different types of gun
#crimes for every district -> Choropleth map

cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")

gun='%GUN%'
police_stations = cursor.fetchall()

for police_station in police_stations:
    police_station_location = (police_station[0],police_station[1])
    cursor.execute("""SELECT DESCRIPTION, count(DESCRIPTION) from crimes where district=%s and DESCRIPTION::text LIKE %s GROUP BY DESCRIPTION""",[police_station[2],gun])
    district_gun_violent_crimes=cursor.fetchall()
    district_gun_violent_crimes_df=pd.DataFrame(district_gun_violent_crimes, columns=['Description', 'Number of Gun Crime'])
    header = district_gun_violent_crimes_df.to_html(classes='table table-striped table-hover table-condensed table-responsive')
    folium.Marker(location=police_station_location,popup=folium.Popup(html="District No: %s GUN_Crime: %s" %(police_station[2],header) )).add_to(districts_gun_violent_crimes_map)
    
#districts_gun_violent_crimes_map


# In[ ]:


#Query 4: calculate crime density per district

district=[]
tarea=[]

with open('Boundaries.geojson') as f:
    data = json.load(f)
    a = data['features']
    for i in range(len(a)):
        obj=a[i]['geometry']
        n= a[i]['properties']
        district.append(n['dist_num'])
        tarea.append(area(obj)/10000)

af=pd.DataFrame({'dist_num': district,'district_area_inHectares':tarea})
af['dist_num'] = af['dist_num'].astype(str)
final_data= pd.merge(af, crimes_per_district, on='dist_num', how='inner')
final_data['crime_density'] = round(final_data['number_of_crimes']/(final_data['district_area_inHectares']/100))
final_data


# In[ ]:


#one more example: Locate the block that has the highest
#number of gun crimes.

gun='%GUN%'
cursor.execute("SELECT block, district, count(block) from crimes where DESCRIPTION::text LIKE %s GROUP BY block, district",[gun])
districts_gun_violent_crimes = cursor.fetchall()
blockgun_df = pd.DataFrame(districts_gun_violent_crimes, columns=['block_num', 'dist_num','gun_crimes'])


blockgun_df['dist_num'] = blockgun_df['dist_num'].astype(str)

blockmax_df = blockgun_df.sort_values(['gun_crimes'], ascending=False).groupby('dist_num').head(1)

blockmax_df   #here, I show each district with their respective Block pertaining to the highest number of gun 
#crimes. District number 3 has the highest number of gun crimes (39) on the block_num shown below, and goes down
#in descending order of gun_crime highs per district. This is shown in the choropleth map lower, where you click in
#each district and it will show the highest descending in order per each block.


# In[ ]:


block_guncrimes_map = folium.Map(location =(41.8781, -87.6298),zoom_start=11)
block_guncrimes_map.choropleth(geo_data="Boundaries.geojson", 
              fill_color='YlOrRd', 
              fill_opacity=0.5, 
              line_opacity=1,
              data = blockmax_df,
              key_on='feature.properties.dist_num',
              columns = ['dist_num', 'gun_crimes'],
              legend_name="GUN CRIME per block"
              )


# In[ ]:


cursor.execute("""SELECT ST_X(ST_AsText(Where_IS)), ST_Y(ST_AsText(Where_IS)), district from police_stations where district!='Headquarters'""")

gun='%GUN%'
police_stations = cursor.fetchall()

for police_station in police_stations:
    police_station_location = (police_station[0],police_station[1])
   
    cursor.execute("""SELECT block, count(block) from crimes where district=%s and DESCRIPTION::text LIKE %s GROUP BY block ORDER BY count(block) DESC""",[police_station[2],gun])
    blockgun_crimes=cursor.fetchall()
    blockgun_crimes_df=pd.DataFrame(blockgun_crimes, columns=['block_num', 'Number of Gun Crimes'])


    header = blockgun_crimes_df.to_html(classes='table table-striped table-hover table-condensed table-responsive')
    folium.Marker(location=police_station_location,popup=folium.Popup(html="District No: %s GUN_Crime: %s" %(police_station[2],header) )).add_to(block_guncrimes_map)

#block_guncrimes_map    #Shows the highest gun crimes per block in descending order when you click on each district...

