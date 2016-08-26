from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests

from datetime import datetime
from okmesonet import weather
from pymongo import MongoClient
mesonetDB = "mesonet"
appname = "osu_okmesonet"
#Default base directory 
#basedir="/data/static/"


#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result
@tasks()
def get_mesonet_data(site,start_data,end_date):
    mongo_uri ="mongodb://{0}:27017/".format(os.environ["{0}_MONGO_PORT_27017_TCP_ADDR".format(appname.upper())])
    db=MongoClient(mongo_uri)
    df=weather.get_mesonet_dataframe(datetime.strptime(start_date, '%Y-%m-%d'),datetime.strptime(end_date, '%Y-%m-%d'),site) 
    data=df.T.to_dict().values()
    result=db[mesonetDB][site].insert_many(data)
    return "{0} records recorded for {1} mesonet site".format(len(result.inserted_ids),site)   

