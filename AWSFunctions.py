# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------------------------------------------------------------------------------------
# COPERNICUS STUDY PROJECT #
# SUMMER SEMESTER 2017 #
# CREATED BY TEAM B - Cristhian Eduardo Murcia Galeano, Diego Armando Morales Cepeda, Jeison Orlando Londoño Espinosa, Mina Karamesouti and Sangeetha Shankar#
# INSTITUTE FOR GEOINFORMATICS (IFGI) #
# UNIVERSITY OF MUENSTER, GERMANY #
# LAST UPDATED ON 28 AUGUST 2017 #
# ------------------------------------------------------------------------------------------------------------------------------------------------------------

import json
import os
import subprocess
import sys
import traceback

import boto3
import time


def connect():
    configuration = getDefaultConfigurationFile()

    os.system("aws configure set AWS_ACCESS_KEY_ID " + configuration["AWS_ACCESS_KEY_ID"])
    os.system("aws configure set AWS_SECRET_ACCESS_KEY " + configuration["AWS_SECRET_ACCESS_KEY"])
    os.system("aws configure set default.region " + configuration["default.region"])


def getDefaultConfigurationFile():
    return getConfigurationFile("C:/grpB_scripts/configuration.json")


def getConfigurationFile(jsonPath):
    with open(jsonPath, 'r') as outfile:
        conf = json.load(outfile)
    return conf


def getNotificationIDAndResourceName():
    sqs = boto3.client('sqs')

    queue_url = 'https://sqs.eu-central-1.amazonaws.com/837005286527/detect_water_bodies_queue'
    #water-detection-image-queue

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    records = json.loads(message["Body"])["Records"]

    record = records[0]
    filename = record["s3"]["object"]["key"]

    attempts = 0

    if "attempts" in record["s3"]["object"]:
        attempts = int(record["s3"]["object"]["attempts"])
    return receipt_handle, filename, attempts

# this function is used while converting Img files (zipped) to Tif files
def getNextFilename(bucketName):
    configuration = getDefaultConfigurationFile()
    META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]

    s3 = boto3.resource("s3")

    bucket = s3.Bucket(bucketName)

    for key in bucket.objects.all():
        newKey=key.key
        foldername = newKey.split('/',1)[0]
        if (newKey.split('/',1)[1]!="" and foldername=="preprocessed-images"):
            file = s3.Object(bucketName, key.key)
            if ('meta_data_status_key' not in file.metadata and key.size<5368709120):
                # We skip files larger than 5 GigaBytes since metadata of these files cannot be updated due to some unknown reasons (the script crashes)
                return file.key

    return None

# this function is used in the water detection process
def getNextTifFilename(bucketName):
    configuration = getDefaultConfigurationFile()
    META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]

    s3 = boto3.resource("s3")

    bucket = s3.Bucket(bucketName)

    for key in bucket.objects.all():
        newKey=key.key
        foldername = newKey.split('/',1)[0]
        if (newKey.split('/',1)[1]!="" and foldername=="preprocessed-images-tif"):
            file = s3.Object(bucketName, key.key)
            META = file.metadata
            if ('meta_data_status_key' not in file.metadata or META['meta_data_status_key']=='Retry'):
                return file.key

    return None


def deleteMessage(receipt_handle):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.eu-central-1.amazonaws.com/837005286527/detect_water_bodies_queue'

    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


def downloadZipFile(bucketName, FolderForInput, NameOfZipFile):
    s3 = boto3.resource('s3')
    mybucket = s3.Bucket(bucketName)
    for i in mybucket.objects.all():
        if i.key.startswith('preprocessed-images'):
                filename = i.key.rsplit('/',1)[1]
                if filename==NameOfZipFile:
                    print(NameOfZipFile)
                    mybucket.download_file(i.key,FolderForInput+NameOfZipFile)


def downloadTifFile(bucketName, FolderForInput, NameOfTifFile):
    s3 = boto3.resource('s3')
    mybucket = s3.Bucket(bucketName)
    downloadedFilesList = []
    for i in mybucket.objects.all():
        if i.key.startswith('preprocessed-images-tif/'+NameOfTifFile):
                filename = i.key.rsplit('/',1)[1]
                mybucket.download_file(i.key,FolderForInput+filename)
                downloadedFilesList.append(i.key)
    return downloadedFilesList


def uploadProcessedData(FolderForOutput,bucketName):
    s3 = boto3.resource('s3')
    mybucket = s3.Bucket(bucketName)
    filesInFolder = os.listdir(FolderForOutput)
    filesInFolder = [FolderForOutput+i for i in filesInFolder]
    for i in filesInFolder:
        filename = i.rsplit('/',1)[1]
        if(filename.startswith('p_') and not filename.endswith('lock')):
                mybucket.upload_file(i, "processed-images/"+filename)

def uploadConvertedData(FolderName,bucketName):
    s3 = boto3.resource('s3')
    mybucket = s3.Bucket(bucketName)

    filesInFolder = os.listdir(FolderName+"VH")
    filesInFolder = [FolderName+"VH/"+i for i in filesInFolder]
    for i in filesInFolder:
        filename = i.rsplit('/',1)[1]
        mybucket.upload_file(i, "preprocessed-images-tif/"+filename)

    filesInFolder = os.listdir(FolderName+"VV")
    filesInFolder = [FolderName+"VV/"+i for i in filesInFolder]
    for i in filesInFolder:
        filename = i.rsplit('/',1)[1]
        mybucket.upload_file(i, "preprocessed-images-tif/"+filename)


def getFileMetadata(bucketName, filename, key):
    s3 = boto3.resource("s3")

    file = s3.Object(bucketName, filename)

    if key is not file.metadata:
        return None

    return file.metadata[key]


def updateFileMetadata(bucketName, filename, metadata={}):
    s3 = boto3.resource("s3")

    file = s3.Object(bucketName, filename)

    file.metadata.update(metadata)
    file.copy_from(CopySource={"Bucket": bucketName, "Key": filename}, Metadata=file.metadata,
                   MetadataDirective="REPLACE")

def deleteErrorFiles(bucketName):
    configuration = getDefaultConfigurationFile()
    META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]

    s3 = boto3.resource("s3")

    bucket = s3.Bucket(bucketName)

    ZipFileToDelete = []
    
    for key in bucket.objects.all():
        newKey=key.key
        foldername = newKey.split('/',1)[0]
        if (newKey.split('/',1)[1]!="" and foldername=="preprocessed-images-tif"):
            file = s3.Object(bucketName, newKey)
            META = file.metadata
            
            if 'meta_data_status_key' in file.metadata:
                if META['meta_data_status_key'] == 'Out of AOI':
                    valueToBeAdded = ((newKey.split('/',1)[1]).split('.',1)[0]).rsplit('_',1)[0]
                    valueToBeAdded = "preprocessed-images/"+valueToBeAdded+".zip"
                    ZipFileToDelete.append(valueToBeAdded)
                    bucket.delete_objects(Delete={'Objects': [{'Key': newKey}]})
                    print("\nDeleted "+str(newKey))
    ZipFileToDelete = set(ZipFileToDelete)
    for i in ZipFileToDelete:
        bucket.delete_objects(Delete={'Objects': [{'Key': i}]})
        print("\nDeleted "+str(i))
    return None

def ResetProcessingStatus(bucketName):
    configuration = getDefaultConfigurationFile()
    META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]

    s3 = boto3.resource("s3")

    bucket = s3.Bucket(bucketName)

    for key in bucket.objects.all():
        newKey=key.key
        foldername = newKey.split('/',1)[0]
        if (newKey.split('/',1)[1]!="" and foldername=="preprocessed-images-tif"):
            file = s3.Object(bucketName, newKey)
            META = file.metadata
            
            if 'meta_data_status_key' in file.metadata:
                if META['meta_data_status_key'] == 'Processing':
                    print(newKey)
                    updateFileMetadata(bucketName, newKey, {"meta_data_status_key": "Retry"})
    return None


def getNumberOfFiles(bucketName):
    configuration = getDefaultConfigurationFile()
    META_DATA_STATUS_KEY = configuration["META_DATA_STATUS_KEY"]
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucketName)

    preprocessedZipCount = 0
    preprocessedTifCount = 0
    waterDetectedTifCount = 0

    BigFiles = 0
    
    print("\nYet to be processed : \n")
     
    for key in bucket.objects.all():
        newKey=key.key
        
        foldername = newKey.split('/',1)[0]
        
        if (newKey.split('/',1)[1]!="" and foldername=="preprocessed-images"):
            preprocessedZipCount = preprocessedZipCount + 1
            file = s3.Object(bucketName, newKey)
            if 'meta_data_status_key' not in file.metadata:
                print(newKey)
            if key.size>5368709120:
                BigFiles = BigFiles + 1
        if (newKey.split('/',1)[1]!="" and foldername=="preprocessed-images-tif"):
            preprocessedTifCount = preprocessedTifCount + 1
            file = s3.Object(bucketName, newKey)
            if 'meta_data_status_key' not in file.metadata:
                print(newKey)
        if (newKey.split('/',1)[1]!="" and foldername=="processed-images"):
            waterDetectedTifCount = waterDetectedTifCount + 1

    print("\nNumber of Pre-processed Zip Files : "+str(preprocessedZipCount))
    print("\nNumber of Pre-processed Tif Files : "+str(preprocessedTifCount/6))
    print("\nNumber of Processed Tif Files : "+str(waterDetectedTifCount/5))
    print("\nNumber of BigFiles : "+ str(BigFiles))
            
    return None

def extractDateFromFileName(key):
   return key[37:45]

def extractDateFromTifFileName(key):
   return key[41:49]
