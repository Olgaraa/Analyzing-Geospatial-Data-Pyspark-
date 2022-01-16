#create a pandas df from csv
import pandas as pd
url="https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv"
pandas_df=pd.read_csv(url)
print(pandas_df)

#write the whole file into ADLS
spark_df=spark.createDataFrame(pandas_df)
conf=dbutils.secrets.get (scope='mysqlKV', key='conf')
spark.conf.set(conf,dbutils.secrets.get(scope="mysqlKV",key="adls"))
destination_path="abfss://input@olgabadl.dfs.core.windows.net"
spark_df.write.format('csv').mode('overwrite').option('header', True).option('sep', ',').save(destination_path + '/Databricks_CSV')

#filter out the data
dbutils.library.installPyPI("geopandas")
dbutils.library.installPyPI("reverse_geocoder")
import reverse_geocoder
import pandas as pd
import geopandas

#changing the data type from string to float
pandas_df['latitude']=pandas_df['latitude'].astype(float)
pandas_df['longitude']=pandas_df['longitude'].astype(float)

#creating a new column 'country' which contains each row's country code
pickup_coords = pandas_df[['latitude', 'longitude']].apply(tuple, axis=1).tolist()
pickup_results = reverse_geocoder.search(pickup_coords, mode=2)
pandas_df['country'] = [x['cc'] for x in pickup_results]

#check which countries have the most occurences
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import desc
check_countries=spark.createDataFrame(pandas_df)
check_countries = check_countries.groupBy("country").count().orderBy(desc("count"))
check_countries.show(100)

#keeping the rows with AU (Australia) country code
pandas_df = pandas_df[pandas_df.country == 'AU']
print(pandas_df)

#write the output to SQL DB
password=dbutils.secrets.get (scope='mysqlKV', key='password')
username=dbutils.secrets.get (scope='mysqlKV', key='username')
url=dbutils.secrets.get (scope='mysqlKV', key='url')
spark_df=spark.createDataFrame(pandas_df)
spark_df.write.format("jdbc").option("url", url).mode("append").option("database", "olgab-DB").option("dbtable", "modis").option("user", username).option("password", password).save()

#display the filtered data
#creating a point from latitude and longitude
dbutils.library.installPyPI("shapely")
from shapely.geometry import Point
from geopandas import GeoDataFrame
geometry=[Point(xy) for xy in zip (pandas_df['longitude'], pandas_df['latitude'])]
gdf = GeoDataFrame(pandas_df, geometry=geometry)  

#World map
world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
gdf.plot(ax=world.plot (figsize=(25,10)), marker='.', color='pink')

#Australia's map
gdf.plot(ax=world[world.name == 'Australia'].plot (figsize=(25,10)), marker='.',color='pink')

#Interactive world map
dbutils.library.installPyPI("matplotlib")
dbutils.library.installPyPI("folium")
dbutils.library.installPyPI("mapclassify")
m = world.explore()
gdf.explore(m=m, color="pink")

#Interactive Australia's map
m = world[world['name'] =='Australia'].explore()
gdf.explore(m=m, color="pink")
