# Developed by: Sebastian Maurice, PhD
# Company: OTICS Advanced Analytics Inc.
# Date: 2021-01-18 
# Toronto, Ontario Canada
# For help email: support@otics.ca 

#######################################################################################################################################

# This file will produce data to a Kafka cluster for Walmart Foot Traffic Predictions and Optimization.  Before using this
# code you MUST have:
# 1) Downloaded and installed MAADS-VIPER from: https://github.com/smaurice101/transactionalmachinelearning

# 2) You have VIPER listening for a connection on port IP: http://127.0.01 and PORT: 9001 (you can specify different IP and PORT
#    just change the  VIPERHOST="http://127.0.0.1" and VIPERPORT=9001)

# 3) You have created a KAfka cluster in Confluent Cloud (https://confluent.cloud/)

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

# TML python library
import maadstml

# Uncomment IF using Jupyter notebook 
#import nest_asyncio

import json
import random
from joblib import Parallel, delayed
import sys

# Uncomment IF using Jupyter notebook
#nest_asyncio.apply()


# Set Global Host/Port for VIPER - You may change this to fit your configuration
VIPERHOST="http://127.0.0.1"
VIPERPORT=9001

#############################################################################################################
#                                      STORE VIPER TOKEN
# Get the VIPERTOKEN from the file admin.tok - change folder location to admin.tok
# to your location of admin.tok
def getparams():
        
     with open("c:/viperdemo/admin.tok", "r") as f:
        VIPERTOKEN=f.read()
  
     return VIPERTOKEN

VIPERTOKEN=getparams()


#############################################################################################################
#                                     CREATE TOPICS IN KAFKA

# Set personal data
def datasetup():
     companyname="OTICS Advanced Analytics"
     myname="Sebastian"
     myemail="Sebastian.Maurice"
     mylocation="Toronto"

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


     # The fields in the Walmart foot traffic prediction model - change these to any number field you wish
     streams=["otics-tmlbook-walmartretail-foottrafic-prediction-foottrafficamount-input","otics-tmlbook-walmartretail-foottrafic-prediction-hourofday-input",
              "otics-tmlbook-walmartretail-foottrafic-prediction-monthofyear-input","otics-tmlbook-walmartretail-foottrafic-prediction-walmartlocationnumber-input"]


     # Put streams in a comma separated list so we can create them all concurrently
     topicnames=','.join(streams)

     description="TML Book example prediction and optimization modeling"

     # Create the 4 topics in Kafka concurrently - it will return a JSON array
     result=maadstml.vipercreatetopic(VIPERTOKEN,VIPERHOST,VIPERPORT,topicnames,companyname,
                                    myname,myemail,mylocation,description,enabletls,
                                    brokerhost,brokerport,numpartitions,replication,
                                    microserviceid)
      
     # Load the JSON array in variable y
     try:
         y = json.loads(result,strict='False')
     except Exception as e:
         y = json.loads(result)


     producerids=[]
     topiclist=[]

     for p in y:  # Loop through the JSON ang grab the topic and producerids
         pid=p['ProducerId']
         tn=p['Topic']
         producerids.append(pid)
         topiclist.append(tn)
         
     return topiclist,producerids


def sendtransactiondata(topiclist,producerids,transactions,j):

     streams=["otics-tmlbook-walmartretail-foottrafic-prediction-foottrafficamount-input","otics-tmlbook-walmartretail-foottrafic-prediction-hourofday-input",
              "otics-tmlbook-walmartretail-foottrafic-prediction-monthofyear-input","otics-tmlbook-walmartretail-foottrafic-prediction-walmartlocationnumber-input"]

     inputbuf=""
     moy=random.randint(1, 12)
     hod=random.randint(1, 24)
     wml=random.randint(1, 11000)
     if moy>=12 and moy <=2:
           if hod>=7 and hod<=15:
               ft=random.randint(8000, 11000)
           else:
               ft=random.randint(0, 4000)
     elif moy>=4 and moy <=9:
           if hod>=7 and hod<=15:
               ft=random.randint(6000, 8000)
           else:
               ft=random.randint(0, 4000)
     else:       
           if hod>=7 and hod<=15:
               ft=random.randint(2000, 5000)
           else:
               ft=random.randint(0, 4000)

     inputbuf=str(ft) +"," + str(hod) + "," + str(moy) + "," + str(wml)
     
     topicbuf=','.join(topiclist)
     produceridbuf=','.join(producerids)


     # Add a 7000 millisecond maximum delay for VIPER to wait for Kafka to return confirmation message is received and written to topic 
     delay=7000
      
     try:
        result=maadstml.viperproducetotopic(VIPERTOKEN,VIPERHOST,VIPERPORT,topicbuf,produceridbuf,1,delay,'','', '',0,inputbuf)
     except Exception as e:
        print(e)


#############################################################################################################
#                                     SETUP THE TOPIC DATA STREAMS FOR WALMART EXAMPLE

topics,producerids=datasetup()
#print(topics)

#############################################################################################################
#                                     SEND DATA TO DATA STREAMS IN PARALLEL USING SSL/TLS FOR WALMART EXAMPLE

transactions=10000000
# n_jobs=-1 means use all cores in your computer
element_run = Parallel(n_jobs=-1)(delayed(sendtransactiondata)(topics,producerids,transactions,k) for k in range(transactions))


