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
import math

# Function to identify the threshold value from image histogram
def findThreshold(valuesArray, mapmin=-40):

    """
    param valuesArray: Subset of the original image is a numpy array
    return: Returns a local threshold which is a float number
    """
    
    #This function receives a block or image subset which is an object of the type np array
    valuesArray = valuesArray.reshape(valuesArray.shape[0] * valuesArray.shape[1])
    valuesArray = valuesArray[(valuesArray >= mapmin) & (valuesArray != 0.0000000)]
    threshVal = -9999
    lengthOfArray = len(valuesArray)

    #Don't run thresholding if the number of non-zero values is small
    if lengthOfArray>10000:
        histo = np.histogram(valuesArray, 300)
        mode_index = np.argmax(histo[0])
        mode = histo[1][mode_index]

        temp = mode
        cutoff = np.array([9999] * 5)
        i = mode_index
        loopVal = 1
            
        if(mode>-17):
            #mode is on the second peak #move left on the histogram
            while loopVal <= 5:
                while temp < cutoff[loopVal - 1]:
                    temp = histo[0][i - loopVal]
                    cutoff[loopVal - 1] = histo[0][i]
                    i = i - loopVal
                cutoff[loopVal - 1] = i + loopVal
                loopVal = loopVal + 1

        else:
            #mode is on the first peak #move right in the histogram
            while loopVal <= 5:
                while temp > cutoff[loopVal - 1]:
                    temp = histo[0][i + loopVal]
                    cutoff[loopVal - 1] = histo[0][i]
                    i = i + loopVal
                cutoff[loopVal - 1] = i - loopVal
                loopVal = loopVal + 1

        cutoff[cutoff < 0] = 500
        mincutoff = min(cutoff)
        if(mincutoff!=500):
            threshVal = histo[1][mincutoff]

    return threshVal



def findThresholdSet (inRaster, blocksize = 5000, mapmin=-40):

    """    
    param inRaster:  Path where the image in tiff format is located (I expect a large image)
    param blocksize: size of the window that will explore or go through the large image the function  
    findThreshold will be executed on small blocks which have this size by default is 5000
    return: A dictionary called tDict that contains the min, max, average threshold values and the set of thresholds
    that were computed over the windows (for exploration purposes) , we will test with sanghetha which of those is more suitable
    """

    myRaster = inRaster
    thresholds = []
    tDict = {}
    c = 0
    t = 0
    for x in range(0, myRaster.width, blocksize):
        for y in range(0, myRaster.height, blocksize):

            # Lower left coordinate of block (in map units)
            mx = myRaster.extent.XMin + x * myRaster.meanCellWidth
            my = myRaster.extent.YMin + y * myRaster.meanCellHeight
            # Upper right coordinate of block (in cells)
            lx = min([x + blocksize, myRaster.width])
            ly = min([y + blocksize, myRaster.height])

            # Extract data block
            block = arcpy.RasterToNumPyArray(myRaster, arcpy.Point(mx, my), lx-x, ly-y)
            c += 1

            #Somtimes the findThreshold function fails that is why I added the try - except structuture
            try :
                t = findThreshold(block,mapmin)
                print("Succeeded in block number " + str(c))
                print("threshold = "+str(t))
            except :
                print("Failed in block number "+str(c))
            #Values below -40 are excluded since they correspond to the black strips that surround the image.
            if (t!=0.00 and t>= -30 and t!=-9999 and t<=-15):
                thresholds.append(t)

    meanT = sum(thresholds)/len(thresholds)
    minT = min(thresholds)
    maxT = max(thresholds)
    tDict["mean"] = meanT
    tDict["minT"] = minT
    tDict["maxT"] = maxT
    tDict["Set"] = thresholds

    print (str(c) + " Blocks were evaluated")
    return tDict

    
# function to create seed pixels using the threshhold value
def createSeedPixels(inRaster, reclassField, cutoff, mapmin, mapmax):
    #creates seed pixels by thresholding    #threshold determined by histogram analysis
    
    remap = RemapRange([[mapmin,cutoff,100],[cutoff,mapmax,0]])

    # Check out the ArcGIS Spatial Analyst extension license
    arcpy.CheckOutExtension("Spatial")

    # Execute Reclassify    #Simple thresholding
    outReclassify = Reclassify(inRaster, reclassField, remap, 0)

    return outReclassify


# function to grow the regions returned by createSeedPixels function
def growWaterBodies(inRaster,seedPixels, simThresh, mapmin, mapmax, reclassField="VALUE", neighborhood=NbrRectangle(9, 9, "CELL")):
    #checks if the cells around the seed pixels are similar; if similar they are classified as water body
    
    print("\nGrowing region..")
    #Group cells into regions
    outRegionGrp = RegionGroup(seedPixels, "EIGHT", "WITHIN","NO_LINK")
    #Middle value of regions
    outZonalStatistics = (ZonalStatistics(outRegionGrp, "Value", inRaster, "MAXIMUM", "NODATA")+ZonalStatistics(outRegionGrp, "Value", inRaster, "MINIMUM", "NODATA"))/2
    #Expand once
    outFocalStatistics = FocalStatistics(outZonalStatistics, neighborhood, "MAXIMUM","")

    nullCells = IsNull(outFocalStatistics)
    minVal = outFocalStatistics - (mapmax-mapmin)*simThresh 
    maxVal = outFocalStatistics + (mapmax-mapmin)*simThresh

    #check and grow water region    
    seedPixels = Con((nullCells == 0) & (inRaster <= maxVal) & (inRaster >= minVal), 100, seedPixels)

    seedPixels = removeSmallAreas(seedPixels)
    remap = RemapValue([[100,100],["NODATA",0]])
    seedPixels = Reclassify(seedPixels, reclassField, remap, "NODATA")
    return seedPixels

# Function to remove very small waterbodies, possibly false positives
def removeSmallAreas(inRaster):
    #remove small areas based on number of cells in the area
    
    outRegionGrp = RegionGroup(inRaster, "EIGHT", "WITHIN","NO_LINK")
    #number of cells
    outZonalStatistics = ZonalStatistics(outRegionGrp, "Value", inRaster, "SUM", "NODATA")/100
    #check neighbourhood
    nbr = NbrRectangle(11, 11, "CELL")
    outFocalStatistics = FocalStatistics(inRaster, nbr, "SUM","")/100
    
    #remove small areas #contains less than 5 pixels (area less than 500msq.) and doesnt contain any other water pixels in a (square) neighbourhood of 50m
    inRaster = Con((outZonalStatistics <= 5) & (outFocalStatistics == outZonalStatistics),0,inRaster)
    inRaster = Reclassify(inRaster, "Value", "0 NODATA;100 100", "NODATA")
    
    return inRaster
