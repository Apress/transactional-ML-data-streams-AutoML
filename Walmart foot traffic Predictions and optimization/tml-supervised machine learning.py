# Developed by: Sebastian Maurice, PhD
# Company: OTICS Advanced Analytics Inc.
# Date: 2021-01-18 
# Toronto, Ontario Canada
# For help email: support@otics.ca 

#######################################################################################################################################

# This file will perform TML for Walmart Foot Traffic.  Before using this code you MUST have:
# 1) Downloaded and installed MAADS-VIPER from: https://github.com/smaurice101/transactionalmachinelearning

# 2) You have:
# a) VIPER listening for a connection on port IP: http://127.0.01 and PORT: 8000 (you can specify different IP and PORT
#    just change the  VIPERHOST="http://127.0.0.1" and VIPERPORT=8000)
# b) HPDE listening for a connection on port IP: http://127.0.01 and PORT: 8001 (you can specify different IP and PORT
#    just change the  hpdehost="http://127.0.0.1" and hpdeport=8001)

# 3) You have created a Kafka cluster in Confluent Cloud (https://confluent.cloud/)

# 4) You have updated the VIPER.ENV file in the following fields:
# a) KAFKA_CONNECT_BOOTSTRAP_SERVERS=[Enter the bootstrap server - this is the Kafka broker(s) - separate multiple brokers by a comma]
# b) KAFKA_ROOT=kafka
# c) SSL_CLIENT_CERT_FILE=[Enter the full path to client.cer.pem]
# d) SSL_CLIENT_KEY_FILE=[Enter the full path to client.key.pem]
# e) SSL_SERVER_CERT_FILE=[Enter the full path to server.cer.pem]

# f) CLOUD_USERNAME=[Enter the Cloud Username- this is the KEY]
# g) CLOUD_PASSWORD=[Enter the Cloud Password - this is the secret]

# NOTE: IF YOU GET STUCK WATCH THE YOUTUBE VIDEO: https://www.youtube.com/watch?v=b1fuIeC7d-8
# Or email support@otics.ca
#########################################################################################################################################

# Import the core libraries
import maadstml

# Uncomment IF using jupyter notebook
#import nest_asyncio

import json
import time

# Uncomment IF using jupyter notebook
#nest_asyncio.apply()

# Set Global variables for VIPER and HPDE - You can change IP and Port for your setup of 
# VIPER and HPDE
VIPERHOST="http://127.0.0.1"
VIPERPORT=8000
hpdehost="http://127.0.0.1"
hpdeport=8001

# Set Global variable for Viper confifuration file - change the folder path for your computer
viperconfigfile="C:/viperdemo/viper.env"

#############################################################################################################
#                                      STORE VIPER TOKEN
# Get the VIPERTOKEN from the file admin.tok - change folder location to admin.tok
# to your location of admin.tok
def getparams():
        
     with open("c:/viperdemo/admin.tok", "r") as f:
        VIPERTOKEN=f.read()
  
     return VIPERTOKEN

VIPERTOKEN=getparams()


def performSupervisedMachineLearning():

#############################################################################################################
#                                     JOIN DATA STREAMS 

      # Set personal data
      companyname="OTICS Advanced Analytics"
      myname="Sebastian"
      myemail="Sebastian.Maurice"
      mylocation="Toronto"

      # Joined topic name
      joinedtopic="otics-tmlbook-walmartretail-foottrafic-prediction-joinedtopics-input"
      # Replication factor for Kafka redundancy
      replication=3
      # Number of partitions for joined topic
      numpartitions=1
      # Enable SSL/TLS communication with Kafka
      enabletls=1
      # If brokerhost is empty then this function will use the brokerhost address in your
      # VIPER.ENV in the field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
      brokerhost=''
      # If this is -999 then this function uses the port address for Kafka in VIPER.ENV in the
      # field 'KAFKA_CONNECT_BOOTSTRAP_SERVERS'
      brokerport=-999
      # If you are using a reverse proxy to reach VIPER then you can put it here - otherwise if
      # empty then no reverse proxy is being used
      microserviceid=''

      description="Topic containing joined streams for Machine Learning training dataset"

      streamstojoin=["otics-tmlbook-walmartretail-foottrafic-prediction-foottrafficamount-input","otics-tmlbook-walmartretail-foottrafic-prediction-hourofday-input",
              "otics-tmlbook-walmartretail-foottrafic-prediction-monthofyear-input","otics-tmlbook-walmartretail-foottrafic-prediction-walmartlocationnumber-input"]

      streamstojoin=','.join(streamstojoin)
      # Call MAADS python function to create joined stream topic
      result=maadstml.vipercreatejointopicstreams(VIPERTOKEN,VIPERHOST,VIPERPORT,joinedtopic,
                          streamstojoin,companyname,myname,myemail,description,mylocation,
                          enabletls,brokerhost,brokerport,replication,numpartitions,microserviceid)

     
      #Load the JSON object and get producerid
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)

      producerid=y['ProducerId']
     
      # Subscribe consumer to the topic just created with some information about yourself
      # If subscribing to a group and add group id here
      groupid=''
      description="Topic contains joined data streams for Walmart example using transactional machine learning"
      result=maadstml.vipersubscribeconsumer(VIPERTOKEN,VIPERHOST,VIPERPORT,joinedtopic,companyname,
                                          myname,myemail,mylocation,description,
                                          brokerhost,brokerport,groupid,microserviceid)
      # Load the JSON object and extract the consumer id
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)

      consumerid=y['Consumerid']
      print(consumerid)


      #############################################################################################################
      #                                    PRODUCE TO TOPIC STREAM

      # Roll back each data stream by 50 offsets - change this to a larger number if you want more data
      # For supervised machine learning you need a minimum of 30 data points in each stream
      rollbackoffsets=70
      # Go to the last offset of each stream: If lastoffset=500, then this function will rollback the 
      # streams to offset=500-50=450
      startingoffset=-1
      # Max wait time for Kafka to response on milliseconds - you can increase this number if
      # Kafka takes longer to response.  Here we tell the functiont o wait 10 seconds
      delay=10000

      # Call the Python function to produce data from all the streams
      result=maadstml.viperproducetotopicstream(VIPERTOKEN,VIPERHOST,VIPERPORT,joinedtopic,producerid,
                                              startingoffset,rollbackoffsets,enabletls,
                                              delay,brokerhost,brokerport,microserviceid)
      
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)

      #get the partition
      for elements in y:
        try:
          if 'Partition' in elements:
             partition=elements['Partition'] 
        except Exception as e:
          continue

      #############################################################################################################
      #                           CREATE TOPIC TO SAVE TRAINING DATA SET FROM STREAM

      # Name the topic
      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-trainingdata-input"
      description="Topic containing the training dataset for TML"

      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                     myname,myemail,mylocation,description,enabletls,
                                     brokerhost,brokerport,numpartitions,replication,microserviceid)
      # Load the JSON and get the producer id 
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)
      producetotopic=y[0]['Topic']
      producerid=y[0]['ProducerId']

      #############################################################################################################
      #                           CREATE TRAINING DATA SET FROM JOINED STREAM TOPIC

      consumefrom=joinedtopic
      description="Subscribing to training dataset"
      result=maadstml.vipersubscribeconsumer(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,companyname,
                                          myname,myemail,mylocation,description,
                                          brokerhost,brokerport,groupid,microserviceid)
      print(result)
      # Load the JSON and extract the consumerid
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)

      consumerid=y['Consumerid']
      # Assign the dependent variable stream
      
      dependentvariable="otics-tmlbook-walmartretail-foottrafic-prediction-foottrafficamount-input"
      # Assign the independentvariable streams
      independentvariables=["otics-tmlbook-walmartretail-foottrafic-prediction-hourofday-input",
              "otics-tmlbook-walmartretail-foottrafic-prediction-monthofyear-input","otics-tmlbook-walmartretail-foottrafic-prediction-walmartlocationnumber-input"]
      independentvariables=','.join(independentvariables)
      #set the delay in milliseconds - or 60 seconds to wait for Kafka to respond 
      # before backing out - for large datasets or slow internet connection you may
      # need to adjust this variable
      delay=60000
      result=maadstml.vipercreatetrainingdata(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,producetotopic,
                                   dependentvariable,independentvariables, 
                                   consumerid,producerid,companyname,partition,
                                   enabletls,delay,brokerhost,brokerport,microserviceid)

      print(result)
      # Load the JSON object and extract the Kafka partition for the training dataset 
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)
      partition_training=y['Partition']
      print(partition_training)


      #############################################################################################################
      #                         SUBSCRIBE TO TRAINING DATA TOPIC  

      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-trainingdata-input"
      description="Subscribing to training dataset topic"
      result=maadstml.vipersubscribeconsumer(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                          myname,myemail,mylocation,description,
                                          brokerhost,brokerport,groupid,microserviceid)
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)
      consumeridtrainingdata2=y['Consumerid']

      #############################################################################################################
      #                         CREATE TOPIC TO STORE TRAINED PARAMS FROM ALGORITHM  

      consumefrom=producetotopic
      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-trained-params-input"

      description="Topic to store the trained machine learning parameters"
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                     myname,myemail,mylocation,description,enabletls,
                                     brokerhost,brokerport,numpartitions,replication,
                                     microserviceid='')
      # Load JSON data and extract the producer id
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)
      producetotopic=y[0]['Topic']
      producerid=y[0]['ProducerId']

      #############################################################################################################
      #                         VIPER CALLS HPDE TO PERFORM REAL_TIME MACHINE LEARNING ON TRAINING DATA 

      consumefrom="otics-tmlbook-walmartretail-foottrafic-prediction-trainingdata-input"
      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-trained-params-input"
      # deploy the algorithm to ./deploy folder - otherwise it will be in ./models folder
      deploy=1
      # number of models runs to find the best algorithm
      modelruns=20
      # Go to the last offset of the partition in partition_training variable
      offset=-1
      # If 0, this is not a logistic model where dependent variable is discreet
      islogistic=0
      # set network timeout for communication between VIPER and HPDE in seconds
      # increase this number if you timeout
      networktimeout=600

      # This parameter will attempt to fine tune the model search space - a number close to 0 means you will have lots of
      # models but their quality may be low.  A number close to 100 means you will have fewer models but their predictive
      # quality will be higher.
      modelsearchtuner=90
      
      result=maadstml.viperhpdetraining(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,producetotopic,
                                      companyname,consumeridtrainingdata2,producerid, hpdehost,
                                      viperconfigfile,enabletls,partition_training,
                                      deploy,modelruns,modelsearchtuner,hpdeport,offset,islogistic,
                                      brokerhost,brokerport,networktimeout,microserviceid)    
      print("Training Result=",result)
##########################################################################

# Change this to any number
numpredictions=10000

for j in range(numpredictions):
  try:
     # Re-train every 60 seconds- change to whatever number you wish
     performSupervisedMachineLearning()
     time.sleep(60)  
  except Exception as e:
    print(e)   
    continue   
