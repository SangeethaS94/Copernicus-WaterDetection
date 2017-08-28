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
import zipfile

from AWSFunctions import *
from Water_detection_functions import *

from datetime import datetime

try:
        print("\nProcessing started at "+str(datetime.now()))
        #config details
        print("\nGetting configuration details...")
        configuration = getDefaultConfigurationFile()
        mybucket = configuration["BUCKET_NAME_PROCESSED_IMAGES"]
        FolderForInput = configuration["FolderForDataConversion"]
        FolderForOutput = configuration["FolderForDataConversion"]

        NameOfZipFile = getNextFilename(mybucket)

        if NameOfZipFile!=None:
                NameOfZipFile = NameOfZipFile.split('/',1)[1]
                print("\nSuccessfully obtained configutration details at "+str(datetime.now()))

                #Empty the folder before begining the process
                print("\nCreating folder to store data...")
                if os.path.isdir(FolderForInput):
                        shutil.rmtree(FolderForInput)
                if os.path.isdir("C:/Users/Administrator/AppData/Local/ESRI/Desktop10.5/SpatialAnalyst/"):
                        shutil.rmtree("C:/Users/Administrator/AppData/Local/ESRI/Desktop10.5/SpatialAnalyst/")
                os.makedirs(FolderForInput)
                os.makedirs(FolderForInput+"temp/")
                os.makedirs(FolderForInput+"VH/")
                os.makedirs(FolderForInput+"VV/")
                os.makedirs("C:/Users/Administrator/AppData/Local/ESRI/Desktop10.5/SpatialAnalyst/")


                #define the path to the img file
                NameWithoutZip = NameOfZipFile.rsplit('.',1)[0]
                inputRasterImgVH = FolderForInput+NameWithoutZip+"/"+NameWithoutZip+".data/Sigma0_VH_db.img"
                inputRasterImgVV = FolderForInput+NameWithoutZip+"/"+NameWithoutZip+".data/Sigma0_VV_db.img"
                inputRasterName = NameWithoutZip


                #set input and output rasters
                inputRasterRawVH = FolderForInput+"temp/"+inputRasterName+"_VH.tif"
                inputRasterRawVV = FolderForInput+"temp/"+inputRasterName+"_VV.tif"

                inputRasterRawVHtemp = FolderForInput+"temp/"+inputRasterName+"_VH_temp.tif"
                inputRasterRawVVtemp = FolderForInput+"temp/"+inputRasterName+"_VV_temp.tif"
                
                inputRasterPathVH = FolderForInput+"VH/"+inputRasterName+"_VH.tif"
                inputRasterPathVV = FolderForInput+"VV/"+inputRasterName+"_VV.tif"

                print("\nSetup completed at "+str(datetime.now()))

                #-------------------------------------------------DOWNLOAD DATA----------------------------------------------#
                print("\nDownloading preprocessed data...")
                downloadZipFile(mybucket, FolderForInput, NameOfZipFile)
                print("\nDownload successful at "+str(datetime.now()))

                #Update metadata
                updateFileMetadata(mybucket, "preprocessed-images/"+NameOfZipFile, {"meta_data_status_key": "Processing"})
                print("\nMetadata successfully updated at "+str(datetime.now()))

                #-------------------------------------------------UNZIP THE FILE---------------------------------------------#
                print("\nUnzipping data...")
                zip_ref = zipfile.ZipFile(FolderForInput+NameOfZipFile, 'r')
                zip_ref.extractall(FolderForInput)
                zip_ref.close()
                os.remove(FolderForInput+NameOfZipFile)
                print("\nUnzip successful at "+str(datetime.now()))


                arcpy.CheckOutExtension("Spatial")

                #-----------------------------------------------PROCESSING VV BAND-------------------------------------------#
                print("\nProcessing VV band...")

                #---------------------------------------CONVERT IMG TO TIFF AND REPROJECT------------------------------------#
                print("\nConverting data to TIFF...")
                arcpy.CopyRaster_management(inputRasterImgVV, inputRasterRawVVtemp, "", "", "-3,402823e+038", "NONE", "NONE", "", "NONE", "NONE", "TIFF", "NONE")
                print("\nTif conversion completed at "+str(datetime.now()))
                arcpy.ProjectRaster_management(inputRasterRawVVtemp, inputRasterRawVV, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", "NEAREST", "8.98315284119522E-05 8.98315284119523E-05", "", "", "GEOGCS['GCS_WGS84_DD',DATUM['D_WGS84',SPHEROID['WGS84',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]")
                print("\nReprojection completed at "+str(datetime.now()))
                
                #-------------------------------------------REMOVE OUT OF BOUND VALUES---------------------------------------#
                print("\nRemoving out-of-bound values...")
                inputRasterRawVV = Raster(inputRasterRawVV)
                inputRasterFilteredVV = Con((inputRasterRawVV < -30),0,inputRasterRawVV)
                inputRasterFilteredVV.save(inputRasterPathVV)
                print("\nRemove out-of-bounds successful at "+str(datetime.now()))

                #-----------------------------------------------PROCESSING VH BAND-------------------------------------------#
                print("\nProcessing VH band...")
                #-----------------------------------------------CONVERT IMG TO TIFF------------------------------------------#
                print("\nConverting data to TIFF...")
                arcpy.CopyRaster_management(inputRasterImgVH, inputRasterRawVHtemp, "", "", "-3,402823e+038", "NONE", "NONE", "", "NONE", "NONE", "TIFF", "NONE")
                print("\nTif conversion completed at "+str(datetime.now()))
                arcpy.ProjectRaster_management(inputRasterRawVHtemp, inputRasterRawVH, "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", "NEAREST", "8.98315284119522E-05 8.98315284119523E-05", "", "", "GEOGCS['GCS_WGS84_DD',DATUM['D_WGS84',SPHEROID['WGS84',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]")
                print("\nReprojection completed at "+str(datetime.now()))

                #-------------------------------------------REMOVE OUT OF BOUND VALUES---------------------------------------#
                print("\nRemoving out-of-bound values...")
                inputRasterRawVH = Raster(inputRasterRawVH)
                inputRasterFilteredVH = Con((inputRasterRawVH < -30),0,inputRasterRawVH)
                inputRasterFilteredVH.save(inputRasterPathVH)
                print("\nRemove out-of-bounds successful at "+str(datetime.now()))

                #---------------------------------------------SAVE AND UPLOAD RESULTS----------------------------------------#
                print("\nSaving results...")

                #upload processes files to the s3 bucket
                print("\nUploading data to bucket...")
                uploadConvertedData(FolderForOutput,mybucket)
                print("\nUploading successful at "+str(datetime.now()))

                #Update metadata
                updateFileMetadata(mybucket, "preprocessed-images/"+NameOfZipFile, {"meta_data_status_key": "Processed"})
                print("\nMetadata successfully updated at "+str(datetime.now()))

                print("\nSuccess!!!")

        else:
                print("\nNo new file found")

except Exception, e:
       print(str(e))

###------------------------------------------------------------------------END OF CODE-----------------------------------------------------------------------###
