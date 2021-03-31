# Developed by: Sebastian Maurice, PhD
# Company: OTICS Advanced Analytics Inc.
# Date: 2021-01-18 
# Toronto, Ontario Canada
# For help email: support@otics.ca 

#######################################################################################################################################

# This file will predict and optimize Walmart Foot Traffic.  Before using this code you MUST have:
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


def performPredictionOptimization():

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


      print(result)    
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
      rollbackoffsets=20
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
      #                         CREATE TOPIC TO STORE PREDICTIONS FROM ALGORITHM  

      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-results-output"
      description="Topic to store the predictions"
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,
                                    microserviceid)
      #print(result)
      # Load the JSON and extract the producer id
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)
      produceridhyperprediction=y[0]['ProducerId']
      print(produceridhyperprediction)

      #############################################################################################################
      #                                     SUBSCRIBE TO STREAM TOPIC


      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-trained-params-input"
      description="Subscribing to trained machine learning parameters"
      result=maadstml.vipersubscribeconsumer(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                          myname,myemail,mylocation,description,
                                          brokerhost,brokerport,groupid,microserviceid)
      # Load the JSON and extract the consumer id
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)
      consumeridtraininedparams=y['Consumerid']

      #############################################################################################################
      #                                     START HYPER-PREDICTIONS FROM ESTIMATED PARAMETERS
      # name the topic
      producetotopic="otics-tmlbook-walmartretail-foottrafic-prediction-results-output"        
      # Use the topic created from function viperproducetotopicstream for new data for 
      # independent varibles
      inputdata=joinedtopic

      consumefrom="otics-tmlbook-walmartretail-foottrafic-prediction-trained-params-input"
      # if you know the algorithm key put it here - this will speed up the prediction
      mainalgokey=""
      # Offset=-1 means go to the last offset of hpdetraining_partition
      offset=-1
      # wait 60 seconds for Kafka - if exceeded then VIPER will backout
      delay=60000
      # use the deployed algorithm - must exist in ./deploy folder
      usedeploy=1
      # Network timeout
      networktimeout=120
      #Start predicting with new data streams
      result6=maadstml.viperhpdepredict(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,producetotopic,
                                     companyname,consumeridtraininedparams,
                                     produceridhyperprediction, hpdehost,inputdata,mainalgokey,
                                     -1,offset,enabletls,delay,hpdeport,
                                     brokerhost,brokerport,networktimeout,usedeploy,microserviceid)

      result=maadstml.vipersubscribeconsumer(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                          myname,myemail,mylocation,description,
                                          brokerhost,brokerport,groupid,microserviceid)
      
      print("****CONSUMER ID FOR TOPIC=otics-tmlbook-walmartretail-foottrafic-prediction-results-output:*****", result)

     ###########################################################################################################
     #                                  CREATE A CONSUMER GROUP
     #                       Use the Groupid in VIPERviz to consume from topic in parallel
     #                       across hundreds or thousands of consumers
     
      consumergrouptopic="otics-tmlbook-walmartretail-foottrafic-prediction-results-output"
      
      groupname="otics-tmlbook-walmartretail-foottrafic-prediction-results-retailgroup-output"
      
      result=maadstml.vipercreateconsumergroup(VIPERTOKEN,VIPERHOST,VIPERPORT,consumergrouptopic,groupname,
                                      companyname,myname,myemail, description,mylocation,enabletls)
      print(result) 
      y = json.loads(result)
      groupid=y['Groupid']
      print(groupid)

      #############################################################################################################
      #                         CREATE TOPIC TO STORE OPTIMAL PARAMETERS FROM ALGORITHM  

      producetotopic="otics-tmlbook-walmartretail-foottrafic-optimization-results-output"
      description="Topic for optimization results"
      result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,
                                    microserviceid='')
      print(result)
      # Load the JSON and extract the producer id
      try:
        y = json.loads(result,strict='False')
      except Exception as e:
        y = json.loads(result)
      producerid=y[0]['ProducerId']
      
      result=maadstml.vipersubscribeconsumer(VIPERTOKEN,VIPERHOST,VIPERPORT,producetotopic,companyname,
                                          myname,myemail,mylocation,description,
                                          brokerhost,brokerport,groupid,microserviceid)
      print("****CONSUMER ID FOR TOPIC=otics-tmlbook-walmartretail-foottrafic-optimization-results-output:*****", result)

     ###########################################################################################################
      #                                  CREATE A CONSUMER GROUP
      #                       Use the Groupid in VIPERviz to consume from topic in parallel
      #                       across hundreds or thousands of consumers 

      consumergrouptopic="otics-tmlbook-walmartretail-foottrafic-optimization-results-output"
      
      groupname="otics-tmlbook-walmartretail-foottrafic-optimization-results-storeoptimizationgroup-output"
      
      result=maadstml.vipercreateconsumergroup(VIPERTOKEN,VIPERHOST,VIPERPORT,consumergrouptopic,groupname,
                                      companyname,myname,myemail, description,mylocation,enabletls)
      print(result) 
      y = json.loads(result)
      groupid=y['Groupid']
      print(groupid)


      #############################################################################################################
      #                                     START MATHEMATICAL OPTIMIZATION FROM ALGORITHM
      consumefrom="otics-tmlbook-walmartretail-foottrafic-prediction-trained-params-input"
      delay=10000
      offset=-1
      # we are doing minimization if ismin=1, otherwise we are maximizing the objective function
      ismin=1
      # choosing constraints='best' will force HPDE to choose the constraints for you
      constraints='best'
      # We are going to expand the lower and upper bounds on the constraints by 20%
      stretchbounds=50
      # we are going to use MIN and MAX for the lower and upper bounds on the constraints
      constrainttype=1
      # We are going to see if there are 'better' optimal values around an epsilon distance (10%)
      # from the local optimal values found
      epsilon=20
      # network timeout in seconds between VIPER and HPDE
      timeout=120

      # Start the optimization
      result7=maadstml.viperhpdeoptimize(VIPERTOKEN,VIPERHOST,VIPERPORT,consumefrom,producetotopic,
                                      companyname,consumeridtraininedparams,
                                      producerid,hpdehost,-1,
                                      offset,enabletls,delay,hpdeport,usedeploy,ismin,
                                      constraints,stretchbounds,constrainttype,epsilon,
                                      brokerhost,brokerport,timeout,microserviceid)
      print(result7)

##########################################################################

# Change this to any number
numpredictions=1000000

for j in range(numpredictions):
  try:    
     performPredictionOptimization()
  except Exception as e:
    print(e)   
    continue   
