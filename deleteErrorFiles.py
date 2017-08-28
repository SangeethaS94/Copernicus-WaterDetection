# -*- coding: utf-8 -*-
# --------------------------------------------------------------------------------------------------------------------------------------------------------------
# COPERNICUS STUDY PROJECT #
# SUMMER SEMESTER 2017 #
# CREATED BY TEAM B - Cristhian Eduardo Murcia Galeano, Diego Armando Morales Cepeda, Jeison Orlando Londoño Espinosa, Mina Karamesouti and Sangeetha Shankar  #
# INSTITUTE FOR GEOINFORMATICS (IFGI) #
# UNIVERSITY OF MUENSTER, GERMANY #
# LAST UPDATED ON 28 AUGUST 2017 #
# --------------------------------------------------------------------------------------------------------------------------------------------------------------


# Import necessary modules

# modules to connect with AWS S3 buckets
import boto3
import botocore

import codecs

from AWSFunctions import *
            
configuration = getDefaultConfigurationFile()
mybucket = configuration["BUCKET_NAME_PROCESSED_IMAGES"]
FolderForInput = configuration["outputFilePath"]
FolderForOutput = configuration["outputFilePath"]

deleteErrorFiles(mybucket)
ResetProcessingStatus(mybucket)
