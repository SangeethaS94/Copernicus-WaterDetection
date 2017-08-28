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
# modules to use ArcGIS functionalities
import arcpy
from arcpy.sa import *
# module for scientific calculations
import numpy as np
# modules to connect with AWS S3 buckets
import boto3
import botocore
# module to use the functionalities of the OS
import os
import shutil

import codecs

from AWSFunctions import *
from Water_detection_functions import *

from datetime import datetime

def processData(inputRasterPath):

        #-----------READ THE RASTER-----------------#
        inputRaster = arcpy.Raster(inputRasterPath)

        #-----------AUTOMATIC THRESHOLDING--------------#
        print("\nPerforming automatic thresholding...")
        reclassField = "VALUE"
        mapmin = float(arcpy.GetRasterProperties_management(inputRaster,"MINIMUM").getOutput(0))
        mapmax = float(arcpy.GetRasterProperties_management(inputRaster,"MAXIMUM").getOutput(0))
        print("\nMapmin : "+str(mapmin))
        print("\nMapmax : "+str(mapmax))

        ###find threshold values
        thresholdValueSet = findThresholdSet (inputRaster, 5000, mapmin)
        print("\nMean : "+str(thresholdValueSet["mean"]))
        print("\nMin : "+str(thresholdValueSet["minT"]))
        print("\nMax : "+str(thresholdValueSet["maxT"]))
        thresholdValue = float(thresholdValueSet["maxT"])
        waterBodiesByThreshold = createSeedPixels(inputRaster, reclassField, thresholdValue, mapmin, mapmax)
        #similarityThreshold = 0.05  #5% similarity threshold

        print("\nAutomatic thresholding completed at "+str(datetime.now()))

        ###grow water bodies
        #waterBodiesByThreshold = growWaterBodies(inputRaster,waterBodiesByThreshold, similarityThreshold, mapmin, mapmax)

        #print("\nGrow waterbodies completed at "+str(datetime.now()))

        #------------ISOCLUSTERING-----------------#
        print("\nPerforming isoclustering...")
        classes = 40
        ResultIsocluster = IsoClusterUnsupervisedClassification(inputRaster, classes)
        ##waterBodies = IsoClusterUnsupervisedClassification(inputRaster, classes)
        print("\nIsoclustering completed at "+str(datetime.now()))

        #-----------COMBINE RESULTS----------------#
        print("\nMerging results...")
        waterBodies = Con((waterBodiesByThreshold == 100) | (ResultIsocluster == 1), 100,0)
        print("\nResults combined successfully at "+str(datetime.now()))

        return waterBodies


#----------SETUP------------#

try:
        print("\nProcessing started at "+str(datetime.now()))
        #config details
        print("\nGetting configuration details...")
        #messageID, NameOfZipFile, attempts = getNotificationIDAndResourceName()

        configuration = getDefaultConfigurationFile()
        mybucket = configuration["BUCKET_NAME_PROCESSED_IMAGES"]
        FolderForInput = configuration["outputFilePath"]
        FolderForOutput = configuration["outputFilePath"]

        NewFilename = getNextTifFilename(mybucket)

        if NewFilename!=None:
                NameOfTifFile = NewFilename.split('/',1)[1]
                NameOfTifFile = NameOfTifFile.split('.',1)[0]
                NameOfTifFile = NameOfTifFile.rsplit('_',1)[0]
                print("\nName of new file : "+str(NameOfTifFile))
                print("\nSuccessfully obtained configuration details at "+str(datetime.now()))

                #Empty the folder before begining the process
                print("\nCreating folder to store data...")
                if os.path.isdir(FolderForInput):
                        shutil.rmtree(FolderForInput)
                if os.path.isdir("C:/Users/Administrator/AppData/Local/ESRI/Desktop10.5/SpatialAnalyst/"):
                        shutil.rmtree("C:/Users/Administrator/AppData/Local/ESRI/Desktop10.5/SpatialAnalyst/")
                os.makedirs(FolderForInput)
                os.makedirs("C:/Users/Administrator/AppData/Local/ESRI/Desktop10.5/SpatialAnalyst/")

                #set input and output rasters
                inputRasterPathVH = FolderForInput+NameOfTifFile+"_VH.tif"
                inputRasterPathVV = FolderForInput+NameOfTifFile+"_VV.tif"
                outRaster = FolderForOutput+"p_"+NameOfTifFile+".tif"

                print("\nSetup completed at "+str(datetime.now()))

                #----------DOWNLOAD DATA---------------#
                print("\nDownloading preprocessed data...")
                downloadedFilesList = downloadTifFile(mybucket, FolderForInput, NameOfTifFile)
                print("\nDownload successful at "+str(datetime.now()))

                #Update metadata
                for i in downloadedFilesList:
                        updateFileMetadata(mybucket, i, {"meta_data_status_key": "Processing"})
                print("\nMetadata successfully updated at "+str(datetime.now()))

                if(len(downloadedFilesList) == 6):
                        arcpy.CheckOutExtension("Spatial")

                        #--------------------PROCESSING VV BAND--------------------------#
                        print("\nProcessing VV band...")
                        waterbodiesVV = processData(inputRasterPathVV)

                        #--------------------PROCESSING VH BAND--------------------------#
                        print("\nProcessing VH band...")
                        waterbodiesVH = processData(inputRasterPathVH)

                        #-----------COMBINE RESULTS OF BOTH BANDS--------#

                        waterbodies =  Con((waterbodiesVH == 100) & (waterbodiesVV == 100), 100,0)

                        #----------COMBINE WITH PERMANENT WATER BODIES-----------------#
                        print("\nMerging with permanent water bodies...")
                        perm_water = Raster("C:/grpB_scripts/water_NRW_new.tif.vat/water_NRW_new.tif")
                        waterbodies = Con((waterbodies == 100) | (perm_water == 50), 100,0)
                        print("\nCombined with permanent water bodies at "+str(datetime.now()))

                        #----------REMOVE FALSE POSITIVES---------------#
                        print("\nRemoving false positives...")
                        inMaskData = Raster("C:/grpB_scripts/FP.tif")
                        waterbodies = Con((waterbodies==100) & (inMaskData==1),0,waterbodies)
                        print("\nFalse positive removal successful at "+str(datetime.now()))

                        #----------SAVE AND UPLOAD RESULTS--------------#
                        print("\nSaving results...")
                        #save output to folder
                        waterbodies.save(outRaster)
                        print("\nResult saved to folder at "+str(datetime.now()))

                        #upload processes files to the s3 bucket
                        print("\nUploading data to bucket...")
                        uploadProcessedData(FolderForInput,mybucket)
                        print("\nUploading successful at "+str(datetime.now()))

                        #Update metadata
                        for i in downloadedFilesList:
                                updateFileMetadata(mybucket, i, {"meta_data_status_key": "Processed"})
                        #print("\nMetadata successfully updated at "+str(datetime.now()))

                        print("\nSuccess!!!")
                        

        else:
                print("\nNo new file found")

except Exception, e:
       print(str(e))
       for i in downloadedFilesList:
                        updateFileMetadata(mybucket, i, {"meta_data_status_key": "Out of AOI"})

###-----END OF CODE-----------#
