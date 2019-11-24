#Python Script for Querying of MS4 NPDES Geodatabase
#Organization: Maryland Department of the Environment
#Author: Rory Maymon
#File Name: automate_query.py
#Purpose: Automate the process of allowing users to query out desired varibles and summarize
#quantitative data.


# ---------------------------------------------------------------------------
#Imports Python packages in order to perform some actions in script.
from __future__ import print_function
import os
import sys
import re
import arcpy
import numpy as np
import math
import pandas as pd

#Defines environment variables

tempenvironment0 = arcpy.env.scratchWorkspace
arcpy.env.scratchWorkspace = arcpy.GetParameterAsText(0)
tempenvironment1 = arcpy.env.workspace
arcpy.env.workspace = arcpy.GetParameterAsText(1)
arcpy.env.scratchWorkspace = tempenvironment0
arcpy.env.workspace = tempenvironment1

#_______________________________________________________________________________

#Defines query workspace and the output table geodatabase. Also defines desired jurisdictions.

QUERIES = arcpy.GetParameterAsText(2)
SUMMARY_GDB = arcpy.GetParameterAsText(3)
JURIS_BORD = arcpy.GetParameterAsText(4)
JURIS_CHOICE = arcpy.GetParameterAsText(5)
if ";" in JURIS_CHOICE:
    JURISLIST = JURIS_CHOICE.split(";")
else:
    JURISLIST = []
    JURISLIST.append(JURIS_CHOICE)
JURISSTRING = ''
for i in JURISLIST:
    JURISSTRING = JURISSTRING + i + ','
JURISSTRING = JURISSTRING[:-1]

#_______________________________________________________________________________

#Defines the alternative BMP line shapefile and passes along user-selected field names.

ALTBMPLINE = arcpy.GetParameterAsText(6)
if ALTBMPLINE != "":
    ALTBMPLINE_Fields = arcpy.GetParameterAsText(7)
    ALTBMPLINE_FIELDLIST = ALTBMPLINE_Fields.split(";")
ALTBMPTABLE = "ALTBMPTABLE.dbf"
ALTBMPTABLE_ = QUERIES + "\\ALTBMPTABLE.dbf"

#_______________________________________________________________________________

#Defines the alternative BMP polygon shapefile and passes along user-selected field names.

ALTBMPPOLY = arcpy.GetParameterAsText(8)
if ALTBMPPOLY != "":
    ALTBMPPOLY_Fields = arcpy.GetParameterAsText(9)
    ALTBMPPOLY_FIELDLIST = ALTBMPPOLY_Fields.split(";")
ALTBMPPTABLE = "ALTBMPPTABLE.dbf"
ALTBMPPTABLE_ = QUERIES + "\\ALTBMPPTABLE.dbf"

#_______________________________________________________________________________

#Defines the alternative BMP point shapefile and passes along user-selected field names.

ALTBMPPOINT = arcpy.GetParameterAsText(10)
if ALTBMPPOINT != "":
    ALTBMPPOINT_Fields = arcpy.GetParameterAsText(11)
    ALTBMPPOINT_FIELDLIST = ALTBMPPOINT_Fields.split(";")
ALTPTTBL_ = QUERIES + "\\ALTPTTBL.dbf"

#_______________________________________________________________________________

#Defines the removal rate table, the BMP table, and the BMPPOI table.
#Also passes along user-selected field names.

BMP_TABLE = arcpy.GetParameterAsText(12)
if BMP_TABLE != "":
    BMP_TABLE_Fields = arcpy.GetParameterAsText(13)
    BMP_TABLE_FIELDLIST = BMP_TABLE_Fields.split(";")
BMP_POI = arcpy.GetParameterAsText(14)
BMP_POI_TBL = "BMPPOI.dbf"
BMPPOITBL_ = QUERIES + "\\BMPPOI.dbf"
BMPTABLE = "BMPTABLE.dbf"
BMPTABLE_ = QUERIES + "\\BMPTABLE.dbf"
BMPTABLE2 = "BMPTABLE2.dbf"
BMPTABLE2_ = QUERIES + "\\BMPTABLE2.dbf"
BMPRATEJOIN = QUERIES + "\\BMPRATEJOIN.dbf"
REMOVE_RATE = arcpy.GetParameterAsText(15)

#_______________________________________________________________________________

#Defines the BMP drainage area shapefile and passes along user-selected field names.

BMPDRAIN = arcpy.GetParameterAsText(16)
if BMPDRAIN != "":
    BMPDRAIN_fields = arcpy.GetParameterAsText(17)
    BMPDRAINFIELDLIST = BMPDRAIN_fields.split(";")
#print(BMPDRAINFIELDLIST)
DRAINTBL = "DRAINTBL.dbf"
DRAINTBL_ = QUERIES + "\\DRAINTBL.dbf"
DRAINJOIN = "DRAINJOIN.dbf"
DRAINJOIN_ = QUERIES + "\\DRAINJOIN.dbf"

#_______________________________________________________________________________

#Defines the countywide stormwater watershed assessment table and passes along user-selected field names.

CO_SWA = arcpy.GetParameterAsText(18)
if CO_SWA != "":
    CO_SWA_fields = arcpy.GetParameterAsText(19)
    CO_SWA_FIELDLIST = CO_SWA_fields.split(";")
CO_ST_WA = "CO_ST_WA.dbf"
CO_ST_WA_ = QUERIES + "\\CO_ST_WA.dbf"

#_______________________________________________________________________________

#Defines the erosion and sediment control table and passes along user-selected field names.

ESC = arcpy.GetParameterAsText(20)
if ESC != "":
    ESC_Fields = arcpy.GetParameterAsText(21)
    ESC_FIELDLIST = ESC_Fields.split(";")
ESCTBL = "ESCTBL.dbf"
ESCTBL_ = QUERIES + "\\ESCTBL.dbf"

#_______________________________________________________________________________

#Defines the fiscal analyses info table and passes along user-selected field names.

FISCALANALYSES = arcpy.GetParameterAsText(22)
if FISCALANALYSES != "":
    FISCALANALYSES_Fields = arcpy.GetParameterAsText(23)
    FISCAN_FIELDLIST = FISCALANALYSES_Fields.split(";")
FISCTBL = "FISCTBL.dbf"
FISCTBL_ = QUERIES + "\\FISCTBL.dbf"

#_______________________________________________________________________________

#Defines the illicit dischange detection and elimination program and passes along user-selected field names.

IDDE = arcpy.GetParameterAsText(24)
if IDDE != "":
    IDDE_Fields = arcpy.GetParameterAsText(25)
    IDDE_FIELDLIST = IDDE_Fields.split(";")
IDDETBL = "IDDETBL.dbf"
IDDETBL_ = QUERIES + "\\IDDETBL.dbf"
IDDEEDIT_ = QUERIES + "\\IDDEEDIT.dbf"

#_______________________________________________________________________________

#Defines the impervious surface table and passes along user-selected field names.

IMPERV = arcpy.GetParameterAsText(26)
if IMPERV != "":
    IMPERV_Fields = arcpy.GetParameterAsText(27)
    IMPERV_FIELDLIST = IMPERV_Fields.split(";")
IMPSTBL = "IMPSTBL.dbf"
IMPSTBL_ = QUERIES + "\\IMPSTBL.dbf"
field = "IMPPCT"

#_______________________________________________________________________________

#Defines the local stormwater watershed assessment table and passes along user-selected field names.

LOSWA = arcpy.GetParameterAsText(28)
if LOSWA != "":
    LSWA_Fields = arcpy.GetParameterAsText(29)
    LSWA_FIELDLIST = LSWA_Fields.split(";")
LOCSTTABLE = "LOCSTTABLE.dbf"
LOCSTTABLE_ = QUERIES + "\\LOCSTTABLE.dbf"

#_______________________________________________________________________________

#Defines the municipal facitlies shapefile and passes along user-selected field names.

MUNICIPAL_FACILITIES = arcpy.GetParameterAsText(30)
if MUNICIPAL_FACILITIES != "":
    MUNICIPAL_FACILITIES_Fields = arcpy.GetParameterAsText(31)
    MUNFACFIELDLIST = MUNICIPAL_FACILITIES_Fields.split(";")
MUN_FAC_TBL_ = QUERIES + "\\MUN_FAC_TBL.dbf"

#_______________________________________________________________________________

#Defines the narrative files table and passes along user-selected field names.

NARRATIVEFILES = arcpy.GetParameterAsText(32)
if NARRATIVEFILES != "":
    NARRATIVEFILES_Fields = arcpy.GetParameterAsText(33)
    NARRFILE_FIELDLIST = NARRATIVEFILES_Fields.split(";")
NARRTBL = "NARRTBL.dbf"
NARRTBL_ = QUERIES + "\\NARRTBL.dbf"

#_______________________________________________________________________________

#Defines the permit info table and passes along user-selected field names.

PERMITINFO = arcpy.GetParameterAsText(34)
if PERMITINFO != "":
    PERMITINFO_Fields = arcpy.GetParameterAsText(35)
    PERMITINFO_FIELDLIST = PERMITINFO_Fields.split(";")
PERMTBL = "PERMTBL.dbf"
PERMTBL_ = QUERIES + "\\PERMTBL.dbf"

#_______________________________________________________________________________

#Defines both the quarterly grading permit info table and the quarterly grading permit shapefile.
#Also passes along field names that are selected by the user.

QUARTERLYGRADINGPMTINFO = arcpy.GetParameterAsText(36)
QUARTERLYGRADINGPERMITS = arcpy.GetParameterAsText(37)
if QUARTERLYGRADINGPMTINFO != "":
    QGPI_Field = arcpy.GetParameterAsText(38)
    QGPI_FIELDLIST = QGPI_Field.split(";")
if QUARTERLYGRADINGPERMITS != "":
    QGP_Field = arcpy.GetParameterAsText(39)
    QGP_FIELDLIST = QGP_Field.split(";")
QUGRINFO = "QGPQUERY"
QUGR = "QUGR.dbf"
QUGR_ = QUERIES + "\\QUGR.dbf"
QUGRPERM = "QUGRPERM.dbf"
QUGRPERM_ = QUERIES + "\\QUGRPERM.dbf"
QUGRJOIN = "QUGRJOIN.dbf"
QUGRJOIN_ = QUERIES + "\\QUGRJOIN.dbf"

#_______________________________________________________________________________

#Defines the restoration BMP shapefile and passes along user-selected field names.

RESTBMP = arcpy.GetParameterAsText(40)
if RESTBMP != "":
    RESTBMP_Fields = arcpy.GetParameterAsText(41)
    RESTBMP_FIELDLIST = RESTBMP_Fields.split(";")
RESTTBL = "RESTTBL.dbf"
RESTTBL_ = QUERIES + "\\" + "RESTTBL.dbf"
RESTJOIN = QUERIES + "\\" + "RESTJOIN.shp"
RESTJOIN_ = QUERIES + "\\" + "RESTJOIN_.shp"
RESTJOINTBL = QUERIES + "\\RESTJOINTBL.dbf"

#_______________________________________________________________________________

#Defines the stormwater management table and passes along user-selected field names.

SWM = arcpy.GetParameterAsText(42)
if SWM != "":
    SWM_Fields = arcpy.GetParameterAsText(43)
    SWM_FIELDLIST = SWM_Fields.split(";")
SWMTBL = "SWMTBL.dbf"
SWMTBL_ = QUERIES + "\\SWMTBL.dbf"

#_______________________________________________________________________________

#Defines the output string files and summary file names and filepath.

ALTBMPPOLY_GDB = QUERIES + "\\ALTBMPPOLYSTR.dbf"
ALTBMPLINESTR_GDB = QUERIES + "\\ALTBMPLINESTR.dbf"
ALTBMPPTSTR_GDB = QUERIES + "\\ALTBMPPTSTR.dbf"
IMPERVSTR_GDB = QUERIES + "\\IMPSTR.dbf"
COSWASTR_GDB = QUERIES + "\\COSWASTR.dbf"
LOSWASTR_GDB = QUERIES + "\\LOSWASTR.dbf"
BMPSTR_GDB = QUERIES + "\\BMPSTR.dbf"
RESTSTR_GDB = QUERIES + "\\RESTSTR.dbf"
DRAINSTR_GDB = QUERIES + "\\DRAINSTR.dbf"
MUNFACSTR_GDB = QUERIES + "\\MUNFACSTR.dbf"
SWMSTR_GDB = QUERIES + "\\SWMSTR.dbf"
IDDESTR_GDB = QUERIES + "\\IDDESTR.dbf"

BMPRATESUM = SUMMARY_GDB + "\\BMPSUM"
IMPSUM = SUMMARY_GDB + "\\IMPSUM"
ALTPOLYSUM = SUMMARY_GDB + "\\ALTPOLYSUM"
ALTLINESUM = SUMMARY_GDB + "\\ALTLINESUM"
ALTPOINTSUM = SUMMARY_GDB + "\\ALTPOINTSUM"
LOC_SWA_SUM = SUMMARY_GDB + "\\LOC_SWA_SUM"
CO_SWA_SUM = SUMMARY_GDB + "\\CO_SWA_SUM"
DRAINSUM = SUMMARY_GDB + "\\DRAINSUM"
RESTSUM = SUMMARY_GDB + "\\RESTSUM"
MUNFACSUM = SUMMARY_GDB + "\\MUNFACSUM"
SWMSUM = SUMMARY_GDB + "\\SWMSUM1"
IDDESUM = SUMMARY_GDB + "\\IDDESUM"

#_______________________________________________________________________________

#Defines class for calculation portion of the script.

class CALCULATIONS:

        #Function that performs calculations and queries of alternative BMP point shapefile.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def ALTBMPPOLY(self, ALTBMPPOLY, ALTBMPPOLY_FIELDLIST, ALTBMPPTABLE, ALTBMPPTABLE_, ALTPOLYSUM, QUERIES):
            #Creates lists containing field names and field lengths.
                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(ALTBMPPOLY) if f.name in ALTBMPPOLY_FIELDLIST]
                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(ALTBMPPOLY) if f.name in ALTBMPPOLY_FIELDLIST]
                FIELD_TYPE1 = arcpy.ListFields(ALTBMPPOLY)
                FIELDVAL = []
                for field in FIELD_TYPE1:
                    if field.name in ALTBMPPOLY_FIELDLIST:
                        if str(field.type) == 'String':
                            FIELDVAL.append('Text')
                        else:
                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPPOLY = ''

                FIELDMAPPOLYSTRING = "'''"

                FIELDMAPPOLY = FIELDMAPPOLY + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + ALTBMPPOLY + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):
                    FIELDMAPPOLY = FIELDMAPPOLY + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + ALTBMPPOLY + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPPOLY2 = FIELDMAPPOLY[:-1]

                FIELDMAPPOLYSTRING = FIELDMAPPOLYSTRING + FIELDMAPPOLY2 + FIELDMAPPOLYSTRING

                arcpy.TableToTable_conversion(ALTBMPPOLY, QUERIES, ALTBMPPTABLE, "", FIELDMAPPOLYSTRING, "#")

                arcpy.AddField_management(ALTBMPPTABLE_, "FIELDTEST", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                #If "TSS_REDUCTION" is in the list of field names, perform calculations.

                if "TSS_REDUCTION" in ALTBMPPOLY_FIELDLIST:

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "FIELDTEST", "condition( !TSS_REDUCT! )", "PYTHON_9.3", "def condition(a):\\n         if a > 0:\\n             a = 1\\n         elif a == 0:\\n             a = 0\\n         elif a == None:\\n             a = 0\\n         return a\\n\\n")

                    arcpy.AddField_management(ALTBMPPTABLE_, "TSSREDVAL", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TSSREDVAL", "tssCALC( !FIELDTEST! , !TSS_REDUCT! )", "PYTHON_9.3", "def tssCALC(a, b):\\n      if a == 1:\\n             a = b\\n      elif a == 0:\\n             a = 0\\n      return a")

                    arcpy.AddField_management(ALTBMPPTABLE_, "REDTSSP_EQ", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "REDTSSP_EQ", "tssefficperc( !ALTBMP_TYP!, !REDTSSP_EQ!)", "PYTHON_9.3", "def tssefficperc(a, b):\\n          if a == 'MSS':\\n               b = .10\\n          elif a == 'VSS':\\n               b = .25\\n          elif a == 'IMPP':\\n               b = .57\\n          elif a == 'IMPF':\\n               b = .84\\n          elif a == 'FPU':\\n               b = .93\\n	  return b\\n")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "REDTSSP_EQ", "!REDTSSP_EQ! * !EQU_IMP_AC!", "PYTHON_9.3", "")

                    arcpy.AddField_management(ALTBMPPTABLE_, "TSSPERACRE", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TSSPERACRE", "catchtssCALC( !EQU_IMP_AC!, !TSSREDVAL! , !TSSPERACRE!)", "PYTHON_9.3", "def catchtssCALC(a, b, c):\\n          if a == 0:\\n                c = 0\\n          elif a != 0:\\n                c = b / a \\n          return c")

                    arcpy.AddField_management(ALTBMPPTABLE_, "TON_TSS", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TON_TSS", "!TSSPERACRE! * .000454", "PYTHON_9.3", "")

                #If "TP_REDUCTION" is in the list of field names, perform calculations.

                if "TP_REDUCTION" in ALTBMPPOLY_FIELDLIST:

                    arcpy.AddField_management(ALTBMPPTABLE_, "TPREDVAL", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TPREDVAL", "tpCALC( !FIELDTEST! , !TP_REDUCTI! )", "PYTHON_9.3", "def tpCALC(a, b):\\n      if a == 1:\\n             a = b\\n      elif a == 0:\\n             a = 0\\n      return a")

                    arcpy.AddField_management(ALTBMPPTABLE_, "REDTPP_EQ", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "REDTPP_EQ", "tpefficperc( !ALTBMP_TYP!, !REDTPP_EQ!)", "PYTHON_9.3", "def tpefficperc(a, b):\\n          if a == 'MSS':\\n               b = .04\\n          elif a == 'VSS':\\n               b = .06\\n          elif a == 'IMPP':\\n               b = .77\\n          elif a == 'IMPF':\\n               b = .72\\n          elif a == 'FPU':\\n               b = .94\\n	  return b\\n")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "REDTPP_EQ", "!REDTPP_EQ! * !EQU_IMP_AC!", "PYTHON_9.3", "")

                    arcpy.AddField_management(ALTBMPPTABLE_, "TPPERACRE", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TPPERACRE", "catchtpCALC( !EQU_IMP_AC!, !TPREDVAL! , !TPPERACRE!)", "PYTHON_9.3", "def catchtpCALC(a, b, c):\\n          if a == 0:\\n                c = 0\\n          elif a != 0:\\n                c = b / a \\n          return c")

                    arcpy.AddField_management(ALTBMPPTABLE_, "TON_TP", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TON_TP", "!TPPERACRE! * .000454", "PYTHON_9.3", "")

                #If "TSS_REDUCTION" is in the list of field names, perform calculations.

                if "TN_REDUCTION" in ALTBMPPOLY_FIELDLIST:

                    arcpy.AddField_management(ALTBMPPTABLE_, "TNREDVAL", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TNREDVAL", "tnCALC( !FIELDTEST! , !TN_REDUCTI! )", "PYTHON_9.3", "def tnCALC(a, b):\\n      if a == 1:\\n             a = b\\n      elif a == 0:\\n             a = 0\\n      return a")

                    arcpy.AddField_management(ALTBMPPTABLE_, "REDTNP_EQ", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "REDTNP_EQ", "tnefficperc( !ALTBMP_TYP!, !REDTNP_EQ!)", "PYTHON_9.3", "def tnefficperc(a, b):\\n          if a == 'MSS':\\n               b = .04\\n          elif a == 'VSS':\\n               b = .05\\n          elif a == 'IMPP':\\n               b = .66\\n          elif a == 'IMPF':\\n               b = .13\\n          elif a == 'FPU':\\n               b = .71\\n	  return b\\n")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "REDTNP_EQ", "!REDTNP_EQ! * !EQU_IMP_AC!", "PYTHON_9.3", "")

                    arcpy.AddField_management(ALTBMPPTABLE_, "TNPERACRE", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TNPERACRE", "catchtnCALC( !EQU_IMP_AC!, !TNREDVAL! , !TNPERACRE!)", "PYTHON_9.3", "def catchtnCALC(a, b, c):\\n          if a == 0:\\n                c = 0\\n          elif a != 0:\\n                c = b / a \\n          return c")

                    arcpy.AddField_management(ALTBMPPTABLE_, "TON_TN", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "TON_TN", "!TNPERACRE! * .000454", "PYTHON_9.3", "")

                #If "ALTBMP_PY_ID" is in the list of field names, perform calculations.

                if "ALTBMP_PY_ID" in ALTBMPPOLY_FIELDLIST:

                    arcpy.AddField_management(ALTBMPPTABLE_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPPTABLE_, "JURIS","!ALTBMP_PY_![0:2]", "PYTHON_9.3", "")

                    if "PERMIT_NUM" in ALTBMPPOLY_FIELDLIST:

                        arcpy.CalculateField_management(ALTBMPPTABLE_, "PERMIT_NUM", "PERMNUM( !JURIS!, !PERMIT_NUM!)", "PYTHON_9.3", "def PERMNUM(a, b):\\n        if a == 'MO':\\n             b = '06-DP-3320'\\n        return b\\n")

                    if "EQU_IMP_ACR" in ALTBMPPOLY_FIELDLIST:
                        arcpy.CalculateField_management(ALTBMPPTABLE_, "EQU_IMP_AC", "IMPacre( !JURIS!, !EQU_IMP_AC!)", "PYTHON_9.3", "def IMPacre(a,b):\\n       if a == 'MO':\\n             b = 0\\n       return b")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(ALTBMPPTABLE_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(ALTBMPPTABLE_)]
                FIELD_TYPE2 = arcpy.ListFields(ALTBMPPTABLE_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                    if str(FIELD_NAMES2[i]) == 'ALTBMP_TYP':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPPOLYSTR = ''
                FIELDMAPPOLYSTRING2 = "'''"
                FIELDMAPPOLYINT = ''
                FIELDMAPPOLYSTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPPOLYSTR = FIELDMAPPOLYSTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + ALTBMPPTABLE_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPPOLYSTR2 = FIELDMAPPOLYSTR[:-1]

                FIELDMAPPOLYSTRING2 = FIELDMAPPOLYSTRING2 + FIELDMAPPOLYSTR + FIELDMAPPOLYSTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(ALTBMPPTABLE_, QUERIES, "ALTBMPPOLYSTR", "", FIELDMAPPOLYSTRING2, "")

                arcpy.TableToGeodatabase_conversion(QUERIES + "\\" + "ALTBMPPOLYSTR.dbf", SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPPOLYINT = FIELDMAPPOLYINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + ALTBMPPTABLE_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPPOLYINT2 = FIELDMAPPOLYINT[:-1]

                FIELDMAPPOLYSTRING3 = FIELDMAPPOLYSTRING3 + FIELDMAPPOLYINT + FIELDMAPPOLYSTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(ALTBMPPTABLE_, SUMMARY_GDB, "ALTBMPPOLYINT", "", FIELDMAPPOLYSTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                arcpy.Statistics_analysis(ALTBMPPTABLE_, ALTPOLYSUM, statNames_ , "JURIS;ALTBMP_TYP")


#_______________________________________________________________________________

        #Function that performs calculations and queries of alternative BMP line shapefile.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def ALTBMPLINECALC(self, ALTBMPLINE, ALTBMPLINE_FIELDLIST, ALTBMPTABLE,ALTBMPTABLE_,ALTLINESUM, QUERIES):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES2 = [f.name for f in arcpy.ListFields(ALTBMPLINE) if f.name in ALTBMPLINE_FIELDLIST]

                FIELD_LENGTHS2 = [f.length for f in arcpy.ListFields(ALTBMPLINE) if f.name in ALTBMPLINE_FIELDLIST]

                FIELD_TYPE2 = arcpy.ListFields(ALTBMPLINE)

                FIELDVAL2 = []

                for field in FIELD_TYPE2:
                    if field.name in ALTBMPLINE_FIELDLIST:
                        if str(field.type) == 'string':
                            FIELDVAL2.append('Text')
                        else:
                            FIELDVAL2.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPLINE = ''

                FIELDMAPLINESTRING = "'''"

                FIELDMAPLINE = FIELDMAPLINE + FIELD_NAMES2[0] + ' ' + '"' + FIELD_NAMES2[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS2[0]) + ' ' + FIELDVAL2[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + ALTBMPLINE + ',' + ' ' + FIELD_NAMES2[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES2)):

                    FIELDMAPLINE = FIELDMAPLINE + FIELD_NAMES2[i] + ' ' + '"' + FIELD_NAMES2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + ALTBMPLINE + ',' + ' ' + FIELD_NAMES2[i] + ',-1,-1' + ';'

                FIELDMAPPOLY2 = FIELDMAPLINE[:-1]

                FIELDMAPLINESTRING = FIELDMAPLINESTRING + FIELDMAPPOLY2 + FIELDMAPLINESTRING

                arcpy.TableToTable_conversion(ALTBMPLINE, QUERIES, "ALTBMPTABLE.dbf", "", FIELDMAPLINESTRING, "")

                #If "TSS_LOAD" and then if "TSS_REDUCTION" is in the list of field names, perform calculations.

                if "TSS_LOAD" in ALTBMPLINE_FIELDLIST:

                    arcpy.AddField_management(ALTBMPTABLE_, "TSS_BOOL", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "TSS_BOOL", "condition( !TSS_LOAD! )", "PYTHON_9.3", "def condition(a):\\n         if (a > 0) and (a != 999):\\n             a = 1\\n         elif a == 0 or a == 999:\\n             a = 0\\n         elif a != 0 or a == None:\\n             a = 0\\n         return a\\n\\n")

                    if "TSS_REDUCTION" in ALTBMPLINE_FIELDLIST:

                        arcpy.AddField_management(ALTBMPTABLE_, "TSSREDUCP", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(ALTBMPTABLE_, "TSSREDUCP", "tssCALC( !TSS_BOOL! , !TSS_LOAD!, !TSS_REDUCT!)", "PYTHON_9.3", "def tssCALC(a, b, c):\\n      if a == 1:\\n             a = (a * (c / b)) * 100\\n      elif a == 0:\\n             a = 0\\n      return a")

                        arcpy.CalculateField_management(ALTBMPTABLE_, "TSS_REDUCT", "reducEDIT( !TSS_REDUCT! )", "PYTHON_9.3", "def reducEDIT(a):\\n      if a == -999:\\n            a = 0\\n      return a")

                #If "TP_LOAD" and then if "TP_REDUCTION" is in the list of field names, perform calculations.

                if "TP_LOAD" in ALTBMPLINE_FIELDLIST:

                    arcpy.AddField_management(ALTBMPTABLE_, "TP_BOOL", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "TP_BOOL", "condition( !TP_LOAD! )", "PYTHON_9.3", "def condition(a):\\n         if (a > 0) and (a!=999):\\n             a = 1\\n         elif a == 0 or a == 999:\\n             a = 0\\n         elif a != 0 or a == None:\\n             a = 0\\n         return a\\n\\n")

                    if "TP_REDUCTION" in ALTBMPLINE_FIELDLIST:

                        arcpy.AddField_management(ALTBMPTABLE_, "TPREDUCP", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(ALTBMPTABLE_, "TPREDUCP", "tpCALC( !TP_BOOL! , !TP_LOAD!, !TP_REDUCTI!)", "PYTHON_9.3", "def tpCALC(a, b, c):\\n      if a == 1:\\n             a = (a * (c / b)) * 100\\n      elif a == 0:\\n             a = 0\\n      return a")

                        arcpy.CalculateField_management(ALTBMPTABLE_, "TP_REDUCTI", "reducEDIT( !TP_REDUCTI! )", "PYTHON_9.3", "def reducEDIT(a):\\n      if a == -999:\\n            a = 0\\n      return a")

                #If "TN_LOAD" and then if "TN_REDUCTION" is in the list of field names, perform calculations.

                if "TN_LOAD" in ALTBMPLINE_FIELDLIST:

                    arcpy.AddField_management(ALTBMPTABLE_, "TN_BOOL", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "TN_BOOL", "condition( !TN_LOAD! )", "PYTHON_9.3", "def condition(a):\\n         if (a > 0) and (a!=999):\\n             a = 1\\n         elif (a == 0):\\n             a = 0\\n         elif a != 0 or a == None:\\n             a = 0\\n         elif a == 999:\\n             a = 0\\n         return a\\n\\n")

                    if "TN_REDUCTION" in ALTBMPLINE_FIELDLIST:

                        arcpy.AddField_management(ALTBMPTABLE_, "TNREDUCP", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(ALTBMPTABLE_, "TNREDUCP", "tnCALC( !TN_BOOL! , !TN_LOAD!, !TN_REDUCTI!)", "PYTHON_9.3", "def tnCALC(a, b, c):\\n      if a == 1:\\n             a = (a * (c / b)) * 100\\n      elif a == 0:\\n             a = 0\\n      return a")

                #If "PERMIT_NUM" is in the list of field names, perform calculations.

                if "PERMIT_NUM" in ALTBMPLINE_FIELDLIST:

                    arcpy.CalculateField_management(ALTBMPTABLE_, "PERMIT_NUM", "ALTBMPEDIT( !PERMIT_NUM!)", "PYTHON_9.3", "def ALTBMPEDIT(a):\\n       if a == \" \":\\n             a = \"06-DP-3320\"\\n       return a\\n             ")

                #If "ALTBMP_LN_ID" is in the list of field names, perform calculations.

                if "ALTBMP_LN_ID" in ALTBMPLINE_FIELDLIST:

                    arcpy.AddField_management(ALTBMPTABLE_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "JURIS", "!ALTBMP_LN_![0:2]", "PYTHON_9.3", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "JURIS", "ALTlcoEDIT( !PERMIT_NUM!, !JURIS!)", "PYTHON_9.3", "def ALTlcoEDIT(a, b):\\n       if a == \"06-DP-3320\":\\n             b = \"MO\"\\n       return b\\n")

                #If "ALTBMP_TYPE" is in the list of field names, perform calculations.

                if "ALTBMP_TYPE" in ALTBMPLINE_FIELDLIST:

                    arcpy.AddField_management(ALTBMPTABLE_, "STRRESTTN", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "STRRESTTN", "tnreducperc( !ALTBMP_TYP!, !STRRESTTN!)", "PYTHON_9.3", "def tnreducperc(a, b):\\n          if a == 'STRE':\\n                  b = 0.075\\n          elif a != 'STRE': \\n                  b =0\\n          return b")

                    arcpy.AddField_management(ALTBMPTABLE_, "STRTPREDUC", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "STRTPREDUC", "tpreducperc( !ALTBMP_TYP!, !STRTPREDUC!)", "PYTHON_9.3", "def tpreducperc(a, b):\\n          if a == 'STRE':\\n                  b = 0.068\\n          elif a != 'STRE': \\n                  b =0\\n          return b")

                    arcpy.AddField_management(ALTBMPTABLE_, "NONCOAST", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "NONCOAST", "tssreducp( !ALTBMP_TYP!, !NONCOAST!)", "PYTHON_9.3", "def tssreducp(a, b):\\n          if a == 'STRE':\\n                  b = 15.1\\n          elif a != 'STRE': \\n                  b =0\\n          return b")

                    arcpy.AddField_management(ALTBMPTABLE_, "COASTRED", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "COASTRED", "tssreducp( !ALTBMP_TYP!, !COASTRED! )", "PYTHON_9.3", "def tssreducp(a, b):\\n          if a == 'STRE':\\n                  b = 44.9\\n          elif a != 'STRE': \\n                  b =0\\n          return b")

                    arcpy.AddField_management(ALTBMPTABLE_, "SH_TNP", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "SH_TNP", "shoretnCALC( !ALTBMP_TYP!, !SH_TNP!)", "PYTHON_9.3", "def shoretnCALC(a, b):\\n           if a == 'SHST':\\n                 b = 0.075\\n           elif a != 'SHST':\\n                 b = 0\\n           return b")

                    arcpy.AddField_management(ALTBMPTABLE_, "SH_TPP", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "SH_TPP", "shoretpCALC( !ALTBMP_TYP!, !SH_TPP!)", "PYTHON_9.3", "def shoretpCALC(a, b):\\n           if a == 'SHST':\\n                 b = 0.068\\n           elif a != 'SHST':\\n                 b = 0\\n           return b")

                    arcpy.AddField_management(ALTBMPTABLE_, "SHTSSP", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTBMPTABLE_, "SHTSSP", "shoretssCALC( !ALTBMP_TYP!, !SHTSSP!)", "PYTHON_9.3", "def shoretssCALC(a, b):\\n           if a == 'SHST':\\n                 b = 137\\n           elif a != 'SHST':\\n                 b = 0\\n           return b")

                #If "EQU_IMP_ACR" is in the list of field names, perform calculations.

                if "EQU_IMP_ACR" in ALTBMPLINE_FIELDLIST:

                    arcpy.CalculateField_management(ALTBMPTABLE_, "EQU_IMP_AC", "ifnull( !EQU_IMP_AC! )", "PYTHON_9.3", "def ifnull(a):\\n      if a == -999:\\n           a = 0\\n      return a")

                #If "PERCENT_IMPERVIOUS" is in the list of field names, perform calculations.

                if "PERCENT_IMPERVIOUS" in ALTBMPLINE_FIELDLIST:

                    arcpy.CalculateField_management(ALTBMPTABLE_, "PERCENT_IM", "ifnull(!PERCENT_IM!)", "PYTHON_9.3", "def ifnull(a):\\n     if a == -999:\\n           a = 0\\n     return a")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(ALTBMPTABLE_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(ALTBMPTABLE_)]
                FIELD_TYPE2 = arcpy.ListFields(ALTBMPTABLE_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                    if str(FIELD_NAMES2[i]) == 'ALTBMP_TYP':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPLINESTR = ''
                FIELDMAPLINESTRING2 = "'''"
                FIELDMAPLINEINT = ''
                FIELDMAPLINESTRING3 = "'''"

                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPLINESTR = FIELDMAPLINESTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + ALTBMPTABLE_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPLINESTR2 = FIELDMAPLINESTR[:-1]

                FIELDMAPLINESTRING2 = FIELDMAPLINESTRING2 + FIELDMAPLINESTR2 + FIELDMAPLINESTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(ALTBMPTABLE_, QUERIES, "ALTBMPLINESTR", "", FIELDMAPLINESTRING2, "")

                arcpy.TableToGeodatabase_conversion(ALTBMPLINESTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPLINEINT = FIELDMAPLINEINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + ALTBMPTABLE_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPLINEINT2 = FIELDMAPLINEINT[:-1]

                FIELDMAPLINESTRING3 = FIELDMAPLINESTRING3 + FIELDMAPLINEINT2 + FIELDMAPLINESTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(ALTBMPTABLE_, SUMMARY_GDB, "ALTBMPLINEINT", "", FIELDMAPLINESTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                arcpy.Statistics_analysis(ALTBMPTABLE_, ALTLINESUM, statNames_, "JURIS;ALTBMP_TYP")


#_______________________________________________________________________________

        #Function that performs calculations and queries of alternative BMP point shapefile.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def ALTBMPPOINT(self, ALTBMPPOINT, ALTBMPPOINT_FIELDLIST, QUERIES, ALTPTTBL_, ALTPOINTSUM):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(ALTBMPPOINT) if f.name in ALTBMPPOINT_FIELDLIST]
                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(ALTBMPPOINT) if f.name in ALTBMPPOINT_FIELDLIST]
                FIELD_TYPE1 = arcpy.ListFields(ALTBMPPOINT)
                FIELDVAL = []
                for field in FIELD_TYPE1:
                    if field.name in ALTBMPPOINT_FIELDLIST:
                        if str(field.type) == 'string':
                            FIELDVAL.append('Text')
                        else:
                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPPOINT = ''

                FIELDMAPPOINTSTRING = "'''"

                FIELDMAPPOINT = FIELDMAPPOINT + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + ALTBMPPOINT + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPPOINT = FIELDMAPPOINT + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + ALTBMPPOINT + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPPOINT2 = FIELDMAPPOINT[:-1]

                FIELDMAPPOINTSTRING = FIELDMAPPOINTSTRING + FIELDMAPPOINT2 + FIELDMAPPOINTSTRING

                arcpy.TableToTable_conversion(ALTBMPPOINT, QUERIES, "ALTPTTBL.dbf", "", FIELDMAPPOINTSTRING, "")

                #If "ALTBMP_PT_ID" is in the list of field names, perform calculations.

                if "ALTBMP_PT_ID" in ALTBMPPOINT_FIELDLIST:

                    arcpy.AddField_management(ALTPTTBL_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(ALTPTTBL_, "JURIS", "!ALTBMP_PT_![0:2]", "PYTHON_9.3", "")

                #If "EQU_IMP_ACR" is in the list of field names, perform calculations.

                if "EQU_IMP_ACR" in ALTBMPPOINT_FIELDLIST:

                    arcpy.CalculateField_management(ALTPTTBL_, "EQU_IMP_AC", "ifnull( !EQU_IMP_AC!)", "PYTHON_9.3", "def ifnull(a):\\n     if a == -999:\\n           a = 0\\n     return a")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(ALTPTTBL_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(ALTPTTBL_)]
                FIELD_TYPE2 = arcpy.ListFields(ALTPTTBL_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                    if str(FIELD_NAMES2[i]) == 'ALTBMP_TYP':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPPTSTR = ''
                FIELDMAPPTSTRING2 = "'''"
                FIELDMAPPTINT = ''
                FIELDMAPPTSTRING3 = "'''"

                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPPTSTR = FIELDMAPPTSTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + ALTPTTBL_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPPTSTR2 = FIELDMAPPTSTR[:-1]

                FIELDMAPPTSTRING2 = FIELDMAPPTSTRING2 + FIELDMAPPTSTR2 + FIELDMAPPTSTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(ALTPTTBL_, QUERIES, "ALTBMPPTSTR", "", FIELDMAPPTSTRING2, "")

                arcpy.TableToGeodatabase_conversion(ALTBMPPTSTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPPTINT = FIELDMAPPTINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + ALTPTTBL_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPPTINT2 = FIELDMAPPTINT[:-1]

                FIELDMAPPTSTRING3 = FIELDMAPPTSTRING3 + FIELDMAPPTINT2 + FIELDMAPPTSTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(ALTPTTBL_, SUMMARY_GDB, "ALTBMPPTINT", "", FIELDMAPPTSTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                if "ALTBMP_PT_ID" in ALTBMPPOINT_FIELDLIST and "ALTBMP_TYPE" in ALTBMPPOINT_FIELDLIST:

                    arcpy.Statistics_analysis(ALTPTTBL_, ALTPOINTSUM, statNames_, "JURIS;ALTBMP_TYP")
                else:
                    pass

#_______________________________________________________________________________

        #Function that performs calculations and queries of impervious surface table.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def IMPERVCALC(self, IMPERV, IMPSTBL, IMPSTBL_, IMPERV_FIELDLIST, field, IMPSUM, QUERIES):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(IMPERV) if f.name in IMPERV_FIELDLIST]
                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(IMPERV) if f.name in IMPERV_FIELDLIST]
                FIELD_TYPE1 = arcpy.ListFields(IMPERV)
                FIELDVAL = []
                for field in FIELD_TYPE1:
                    if field.name in IMPERV_FIELDLIST:
                        if str(field.type) == 'string':
                            FIELDVAL.append('Text')
                        else:
                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPIMPERV = ''

                FIELDMAPIMPERVSTRING = "'''"

                FIELDMAPIMPERV = FIELDMAPIMPERV + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + IMPERV + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):
                    FIELDMAPIMPERV = FIELDMAPIMPERV + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + IMPERV + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPIMPERV2 = FIELDMAPIMPERV[:-1]

                FIELDMAPIMPERVSTRING = FIELDMAPIMPERVSTRING + FIELDMAPIMPERV2 + FIELDMAPIMPERVSTRING

                arcpy.TableToTable_conversion(IMPERV, QUERIES, "IMPSTBL.dbf", "", FIELDMAPIMPERVSTRING, "")

                #If "IMP_ACRES" and then if "CONTROLLED_ACRES" or if "PLANNED_ACRES" or if "UNDER_CONST" or if "COMPLETED" is in the list of field names, perform calculations.

                if "IMP_ACRES" in IMPERV_FIELDLIST:

                    if "CONTROLLED_ACRES" in IMPERV_FIELDLIST:

                        arcpy.AddField_management(IMPSTBL_,"PERC_CONTR","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                        arcpy.CalculateField_management(IMPSTBL_, "PERC_CONTR", "100 * !CONTROLLED! / !IMP_ACRES!", "PYTHON_9.3", "")

                        arcpy.AddField_management(IMPSTBL_,"PERC_UNCON","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                        arcpy.CalculateField_management(IMPSTBL_, "PERC_UNCON", "100 * (1-(!CONTROLLED! / !IMP_ACRES!))", "PYTHON_9.3", "")


                    if "PLANNED_ACRES" in IMPERV_FIELDLIST:

                        arcpy.AddField_management(IMPSTBL_,"PROP_REST","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                        arcpy.CalculateField_management(IMPSTBL_, "PROP_REST", "100 * (!PLANNED_AC! / !IMP_ACRES!)", "PYTHON_9.3", "")


                    if "UNDER_CONST" in IMPERV_FIELDLIST:

                        arcpy.AddField_management(IMPSTBL_,"RE_UN_CON","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                        arcpy.CalculateField_management(IMPSTBL_, "RE_UN_CON", "100 * (!UNDER_CONS! / !IMP_ACRES!)", "PYTHON_9.3", "")


                    if "COMPLETED" in IMPERV_FIELDLIST:

                        arcpy.AddField_management(IMPSTBL_,"REST_COMP","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                        arcpy.CalculateField_management(IMPSTBL_, "REST_COMP", "100 * (!COMPLETED! / !IMP_ACRES!)", "PYTHON_9.3", "")

                #If "BASELINE_ACRES" is in the list of field names, perform calculations.

                if "BASELINE_ACRES" in IMPERV_FIELDLIST:

                    arcpy.AddField_management(IMPSTBL_,"RETRO_ACR","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(IMPSTBL_, "RETRO_ACR", "!BASELINE_A! / 5", "PYTHON_9.3", "")

                #If "BASELINE_ACRES" is in the list of field names, perform calculations.

                if "IMPERV_ID" in IMPERV_FIELDLISTL:

                    arcpy.AddField_management(IMPSTBL_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(IMPSTBL_, "JURIS", "!IMPERV_ID![0:2]", "PYTHON_9.3", "")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(IMPSTBL_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(IMPSTBL_)]
                FIELD_TYPE2 = arcpy.ListFields(IMPSTBL_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPIMPSTR = ''
                FIELDMAPIMPSTRING2 = "'''"
                FIELDMAPIMPINT = ''
                FIELDMAPIMPSTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPIMPSTR = FIELDMAPIMPSTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + IMPSTBL_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPIMPSTR2 = FIELDMAPIMPSTR[:-1]

                FIELDMAPIMPSTRING2 = FIELDMAPIMPSTRING2 + FIELDMAPIMPSTR2 + FIELDMAPIMPSTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(IMPSTBL_, QUERIES, "IMPSTR", "", FIELDMAPIMPSTRING2, "")

                arcpy.TableToGeodatabase_conversion(IMPERVSTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPIMPINT = FIELDMAPIMPINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + IMPSTBL_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPIMPINT2 = FIELDMAPIMPINT[:-1]

                FIELDMAPIMPSTRING3 = FIELDMAPIMPSTRING3 + FIELDMAPIMPINT2 + FIELDMAPIMPSTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(IMPSTBL_, SUMMARY_GDB, "IMPINT", "", FIELDMAPIMPSTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                statNames_ = statNames[:-1]

                #Perform summary statistics function in order to summarize numerical data.

                if "IMPERV_ID" in IMPERV_FIELDLIST:
                    arcpy.Statistics_analysis(IMPSTBL_, IMPSUM, statNames_, "JURIS")
                else:
                    pass

#_______________________________________________________________________________

        #Function that performs calculations and queries of countywide stormwater watershed assessment table.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def COSWACALC(self, CO_SWA,CO_ST_WA, CO_ST_WA_, CO_SWA_SUM , CO_SWA_FIELDLIST, QUERIES):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(CO_SWA) if f.name in CO_SWA_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(CO_SWA) if f.name in CO_SWA_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(CO_SWA)

                FIELDVAL = []

                for field in FIELD_TYPE1:
                    if field.name in CO_SWA_FIELDLIST:
                        if str(field.type) == 'string':
                            FIELDVAL.append('Text')
                        else:
                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPCO_SWA = ''

                FIELDMAPCO_SWASTRING = "'''"

                FIELDMAPCO_SWA = FIELDMAPCO_SWA + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + CO_SWA + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):
                    FIELDMAPCO_SWA = FIELDMAPCO_SWA + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + CO_SWA + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPCO_SWA2 = FIELDMAPCO_SWA[:-1]

                FIELDMAPCO_SWASTRING = FIELDMAPCO_SWASTRING + FIELDMAPCO_SWA2 + FIELDMAPCO_SWASTRING

                arcpy.TableToTable_conversion(CO_SWA, QUERIES, "CO_ST_WA.dbf", "", FIELDMAPCO_SWASTRING, "")

                #If "CSW_ID" is in the list of field names, perform calculations.

                if "CSW_ID" in CO_SWA_FIELDLIST:
                    arcpy.AddField_management(CO_ST_WA_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(CO_ST_WA_, "JURIS", "!CSW_ID![0:2]", "PYTHON_9.3", "")

                #If "PERMIT_LOAD" and if "CURRENT_LOAD" is in the list of field names, perform calculations.

                if "PERMIT_LOAD" in CO_SWA_FIELDLIST and "CURRENT_LOAD" in CO_SWA_FIELDLIST:

                    arcpy.AddField_management(CO_ST_WA_, "POLL_REDUC", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(CO_ST_WA_, "POLL_REDUC", "!PERMIT_LOA! - !CURRENT_LO!", "PYTHON_9.3", "")

                    arcpy.AddField_management(CO_ST_WA_, "REDUCPERC", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(CO_ST_WA_, "REDUCPERC", "!CURRENT_LO! / !PERMIT_LOA!", "PYTHON_9.3", "")

                    arcpy.AddField_management(CO_ST_WA_, "SUMEREDUC", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(CO_ST_WA_, "SUMEREDUC", "!POLL_REDUC!", "PYTHON_9.3", "")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(CO_ST_WA_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(CO_ST_WA_)]
                FIELD_TYPE2 = arcpy.ListFields(CO_ST_WA_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS' or str(FIELD_NAMES2[i]) == 'POLLUTANT':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPCOSWASTR = ''
                FIELDMAPCOSWASTRING2 = "'''"
                FIELDMAPCOSWAINT = ''
                FIELDMAPCOSWASTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPCOSWASTR = FIELDMAPCOSWASTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + CO_ST_WA_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPCOSWASTR2 = FIELDMAPCOSWASTR[:-1]

                FIELDMAPCOSWASTRING2 = FIELDMAPCOSWASTRING2 + FIELDMAPCOSWASTR2 + FIELDMAPCOSWASTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(CO_ST_WA_, QUERIES, "COSWASTR", "", FIELDMAPCOSWASTRING2, "")

                arcpy.TableToGeodatabase_conversion(COSWASTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPCOSWAINT = FIELDMAPCOSWAINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + CO_ST_WA_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPCOSWAINT2 = FIELDMAPCOSWAINT[:-1]

                FIELDMAPCOSWASTRING3 = FIELDMAPCOSWASTRING3 + FIELDMAPCOSWAINT2 + FIELDMAPCOSWASTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(CO_ST_WA_, SUMMARY_GDB, "COSWAINT", "", FIELDMAPCOSWASTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                if "CSW_ID" in CO_SWA_FIELDLIST and "POLLUTANT" in CO_SWA_FIELDLIST:
                    arcpy.Statistics_analysis(CO_ST_WA_, CO_SWA_SUM, statNames_, "JURIS;POLLUTANT")
                else:
                    pass
#_______________________________________________________________________________

        #Function that performs calculations and queries of local stormwater watershed assessment table.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def LOCSTCALC(self, LOSWA, LOCSTTABLE, LSWA_FIELDLIST, LOCSTTABLE_, LOC_SWA_SUM, QUERIES):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(LOSWA) if f.name in LSWA_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(LOSWA) if f.name in LSWA_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(LOSWA)

                FIELDVAL = []

                for field in FIELD_TYPE1:
                    if field.name in LSWA_FIELDLIST:
                        if str(field.type) == 'string':
                            FIELDVAL.append('Text')
                        else:
                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPLO_SWA = ''

                FIELDMAPLO_SWASTRING = "'''"

                FIELDMAPLO_SWA = FIELDMAPLO_SWA + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + LOSWA + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPLO_SWA = FIELDMAPLO_SWA + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + LOSWA + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPLO_SWA2 = FIELDMAPLO_SWA[:-1]

                FIELDMAPLO_SWASTRING = FIELDMAPLO_SWASTRING + FIELDMAPLO_SWA2 + FIELDMAPLO_SWASTRING

                arcpy.TableToTable_conversion(LOSWA, QUERIES, LOCSTTABLE, "", FIELDMAPLO_SWASTRING, "")

                #If "LSW_ID" is in the list of field names, perform calculations.

                if "LSW_ID" in LSWA_FIELDLIST:

                    arcpy.AddField_management(LOCSTTABLE_, "YEAR", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(LOCSTTABLE_, "YEAR", "!LSW_ID![2:4]", "PYTHON_9.3", "")

                    arcpy.AddField_management(LOCSTTABLE_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(LOCSTTABLE_, "JURIS","!LSW_ID![0:2]", "PYTHON_9.3", "")

                #If "PERMIT_NUM" and if "PERMIT_LOAD" is in the list of field names, perform calculations.

                if "PERMIT_NUM" in LSWA_FIELDLIST and "CURRENT_LOAD" in LSWA_FIELDLIST:

                    arcpy.AddField_management(LOCSTTABLE_, "POLLREDUC", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(LOCSTTABLE_, "POLLREDUC", "LOCperccond( !PERMIT_LOA!, !CURRENT_LO!, !pollreduc!)", "PYTHON_9.3", "def LOCperccond(a, b, c):\\n          if a == 0:\\n                c = 0\\n          elif a == b:\\n                c = 0\\n          else:\\n                c = a - b\\n          return c")

                    if "PERMIT_LOAD" in LSWA_FIELDLIST:
                        arcpy.AddField_management(LOCSTTABLE_, "POLLPCTRED", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(LOCSTTABLE_, "POLLPCTRED", "LOCperccheck( !POLLREDUC!, !POLLPCTRED!, !PERMIT_LOA!, !CURRENT_LO!)", "PYTHON_9.3", "def LOCperccheck(a, b, c, d):\\n       if a == 0:\\n           b = 0\\n       elif a != 0:\\n           b = 1-(d/c)\\n       return b")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(LOCSTTABLE_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(LOCSTTABLE_)]
                FIELD_TYPE2 = arcpy.ListFields(LOCSTTABLE_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS' or str(FIELD_NAMES2[i]) == 'POLLUTANT':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPLOSWASTR = ''
                FIELDMAPLOSWASTRING2 = "'''"
                FIELDMAPLOSWAINT = ''
                FIELDMAPLOSWASTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPLOSWASTR = FIELDMAPLOSWASTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + LOCSTTABLE_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPLOSWASTR2 = FIELDMAPLOSWASTR[:-1]

                FIELDMAPLOSWASTRING2 = FIELDMAPLOSWASTRING2 + FIELDMAPLOSWASTR2 + FIELDMAPLOSWASTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(LOCSTTABLE_, QUERIES, "LOSWASTR", "", FIELDMAPLOSWASTRING2, "")

                arcpy.TableToGeodatabase_conversion(LOSWASTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPLOSWAINT = FIELDMAPLOSWAINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + LOCSTTABLE_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPLOSWAINT2 = FIELDMAPLOSWAINT[:-1]

                FIELDMAPLOSWASTRING3 = FIELDMAPLOSWASTRING3 + FIELDMAPLOSWAINT2 + FIELDMAPLOSWASTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(LOCSTTABLE_, SUMMARY_GDB, "LOSWAINT", "", FIELDMAPLOSWASTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                if "LSW_ID" in LSWA_FIELDLIST and "POLLUTANT" in LSWA_FIELDLIST:
                    arcpy.Statistics_analysis(LOCSTTABLE_, LOC_SWA_SUM, statNames_, "JURIS;POLLUTANT")
                else:
                    pass

#_______________________________________________________________________________

        #Function that performs calculations and queries of BMP table.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def BMPCALC(self, QUERIES, BMP_POI, BMP_POI_TBL, BMP_TABLE, BMP_TABLE_FIELDLIST, BMPTABLE, BMPTABLE_, BMPTABLE2, BMPTABLE2_, REMOVE_RATE, BMPRATEJOIN):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(BMP_TABLE) if f.name in BMP_TABLE_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(BMP_TABLE) if f.name in BMP_TABLE_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(BMP_TABLE)

                FIELDVAL = []

                for field in FIELD_TYPE1:
                    if field.name in BMP_TABLE_FIELDLIST:
                        if str(field.type) == 'string':
                            FIELDVAL.append('Text')
                        else:
                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPBMP = ''

                FIELDMAPBMPSTRING = "'''"

                FIELDMAPBMP = FIELDMAPBMP + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + BMP_TABLE + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPBMP = FIELDMAPBMP + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + BMP_TABLE + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPBMP2 = FIELDMAPBMP[:-1]

                FIELDMAPBMPSTRING = FIELDMAPBMPSTRING + FIELDMAPBMP2 + FIELDMAPBMPSTRING

                arcpy.TableToTable_conversion(BMP_TABLE, QUERIES, BMPTABLE, "#", FIELDMAPBMPSTRING, "#")

                #If "BMP_ID", "BMP_CLASS", "PE_ADR", and/or "IMP_ACRES are in the list of field names, perform calculations.

                if "BMP_ID" in BMP_TABLE_FIELDLIST:

                    arcpy.AddField_management(BMPTABLE_,"JURIS","TEXT","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.AddField_management(BMPTABLE_,"YEAR","TEXT","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(BMPTABLE_,"JURIS","!BMP_ID![0:2]","PYTHON_9.3","#")

                    arcpy.CalculateField_management(BMPTABLE_,"YEAR","!BMP_ID![2:4]","PYTHON_9.3","#")

                    arcpy.TableToTable_conversion(BMP_POI, QUERIES, BMP_POI_TBL, "#", "#", "#")

                    if "BMP_CLASS" in BMP_TABLE_FIELDLIST and "BMP_TYPE" in BMP_TABLE_FIELDLIST:

                        arcpy.AddField_management(BMPTABLE_, "RR", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.AddField_management(BMPTABLE_, "ST", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.AddField_management(BMPTABLE_, "OTH", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(BMPTABLE_, "OTH", "AorU(!BMP_CLASS!, !OTH!)", "PYTHON_9.3", "def AorU(a, b):\\n     if a== 'A':\\n          b = 'OTH'\\n     elif a == 'U':\\n          b = 'OTH'\\n     elif a == '':\\n          b = 'OTH'\\n     return b ")

                        arcpy.CalculateField_management(BMPTABLE_, "RR", "RRorst( !BMP_CLASS!, !BMP_TYPE!, !RR!)", "PYTHON_9.3", "def RRorst(a, b, c):\\n      if a == 'U':\\n            c = ''\\n      elif (a == 'ESD') or (b == 'IBAS') or (b == 'ITRN') or (b == 'FBIO') or (b =='ODSW'):\\n            c = 'RR'\\n      elif a == '':\\n           c = ''\\n      return c")

                        arcpy.CalculateField_management(BMPTABLE_, "ST", "ifst( !OTH!, !RR!, !ST!)", "PYTHON_9.3", "def ifst(a, b, c):\\n      if a == 'OTH':\\n            c = \"\"\\n      elif a == 'U':\\n            c = \"\"\\n      elif a == 'A':\\n            c = \"\"\\n      elif a == '':\\n            c = \"\"\\n      elif b != 'RR':\\n            c = \"st\"\\n      return c")

                        arcpy.AddField_management(BMPTABLE_, "CONC_RRST", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(BMPTABLE_, "CONC_RRST", "concat( !RR!, !ST!, !OTH!, !CONC_RRST!)", "PYTHON_9.3", "def concat(a, b, c, d):\\n      d = a + b + c\\n      return d")

                        arcpy.AddField_management(BMPTABLE_, "CONC_RRST_", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(BMPTABLE_, "CONC_RRST_", "concat( !CONC_RRST!, !CONC_RRST_!)", "PYTHON_9.3", "def concat(a, b):\\n      if a == 'RR':\\n           b = 'RR'\\n      elif a == '  st':\\n           b = 'st'\\n      elif a == '  OTH':\\n           b = 'OTH'\\n      return b\\n ")

                    if "BMPPOI_ID" in BMP_TABLE_FIELDLIST:

                        arcpy.JoinField_management(BMPTABLE_, "BMPPOI_ID", BMPPOITBL_, "BMPPOI_ID", "#")

                    arcpy.TableToTable_conversion(BMPTABLE_, QUERIES, "BMPTABLE2.dbf", "#", "#", "#")
                    if "PE_ADR" in BMP_TABLE_FIELDLIST and "BMP_CLASS" in BMP_TABLE_FIELDLIST:
                        arcpy.AddField_management(BMPTABLE2_, "BMP_CLASS", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(BMPTABLE2_, "BMP_CLASS", "cond_rate( !PE_ADR!, !CONC_RRST!, !BMP_CLASS!)", "PYTHON_9.3", "def cond_rate( a, b, c):\\n          if b == ' ':\\n                c = 23\\n          elif b !=' ':\\n                 if a == 0:\\n                        if b == 'RR':\\n                               c = 1\\n                        elif b == ' st':\\n                               c = 2\\n                 elif 0 < a <= .25:\\n                        if b == 'RR':\\n                               c = 3\\n                        elif b == ' st':\\n                               c = 4\\n                 elif .25 < a <= .5: \\n                        if b == 'RR':\\n                               c = 5\\n                        elif b == ' st':\\n                               c = 6\\n                 elif .5 < a <= .75:\\n                        if b == 'RR':\\n                               c = 7\\n                        elif b == ' st':\\n                               c = 8\\n                 elif .75 < a <= 1.00:\\n                        if b == 'RR':\\n                               c = 9\\n                        elif b == ' st':\\n                               c = 10\\n                 elif 1.00 < a <= 1.25:\\n                        if b == 'RR':\\n                               c = 11\\n                        elif b ==' st':\\n                               c = 12\\n                 elif 1.25 < a <= 1.5:\\n                        if b == 'RR':\\n                               c = 13\\n                        elif b == ' st':\\n                               c = 14\\n                 elif 1.5 < a <=1.75:\\n                        if b == 'RR':\\n                               c = 15\\n                        elif b == ' st':\\n                               c = 16\\n                 elif 1.75 < a <= 2.00:\\n                        if b == 'RR':\\n                               c = 17\\n                        elif b == ' st':\\n                               c = 18\\n                 elif 2.00 < a <= 2.25:\\n                        if b == 'RR':\\n                               c = 19\\n                        elif b == ' st':\\n                               c = 20\\n                 elif 2.25 < a <= 2.5:\\n                         if b == 'RR':\\n                               c = 21\\n                         elif b == ' st':\\n                               c =22\\n          return c")
                    if "BMP_CLASS" in BMP_TABLE_FIELDLIST:

                        arcpy.JoinField_management(BMPTABLE2_, "BMP_CLASS", REMOVE_RATE, "KEY_")

                    arcpy.TableToTable_conversion(BMPTABLE2_, QUERIES, "BMPRATEJOIN.dbf", "#", "#", "#")

                    if "BMP_ID" in BMP_TABLE_FIELDLIST:

                        arcpy.AddField_management(BMPRATEJOIN, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(BMPRATEJOIN, "JURIS", "!BMP_ID![0:2]", "PYTHON_9.3", "")

                    arcpy.CalculateField_management(BMPRATEJOIN, "IMP_ACRES", "ifnull( !IMP_ACRES!)", "PYTHON_9.3", "def ifnull(a):\\n     if a == -999:\\n          a = 0\\n     elif a == 77777:\\n          a = 0\\n     return a")

                    arcpy.CalculateField_management(BMPRATEJOIN, "BMP_DRAIN1", "ifnull( !BMP_DRAIN1!)", "PYTHON_9.3", "def ifnull(a):\\n    if a == -999:\\n         a = 0\\n    elif a == 77777:\\n         a = 0\\n    return a")

                    arcpy.CalculateField_management(BMPRATEJOIN, "PE_REQ", "ifnull( !PE_REQ!)", "PYTHON_9.3", "def ifnull(a):\\n    if a == -999:\\n        a = 0\\n    elif a == 77777:\\n        a = 0\\n    return a")

                    arcpy.CalculateField_management(BMPRATEJOIN, "PE_ADR", "ifnull( !PE_ADR!)", "PYTHON_9.3", "def ifnull(a):\\n     if a == -999:\\n          a = 0\\n     elif a == 77777:\\n          a = 0\\n     return a")

                    arcpy.AddField_management(BMPRATEJOIN,"RRorST","TEXT","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"RRorST","!TSS_Type!","PYTHON_9.3","#")

                    arcpy.AddField_management(BMPRATEJOIN,"PE_REQ_","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"PE_REQ_","!PE_REQ!","PYTHON_9.3","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"PE_REQ_","round( !PE_REQ_!, 2)","PYTHON_9.3","#")

                    arcpy.AddField_management(BMPRATEJOIN,"PE_ADR_","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"PE_ADR_","!PE_ADR!","PYTHON_9.3","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"PE_ADR_","round( !PE_ADR_!, 2)","PYTHON_9.3","#")

                    if "IMP_ACRES" in BMP_TABLE_FIELDLIST:

                        arcpy.AddField_management(BMPRATEJOIN,"IMP_ACRES_","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                        arcpy.CalculateField_management(BMPRATEJOIN,"IMP_ACRES_","!IMP_ACRES!","PYTHON_9.3","#")

                        arcpy.CalculateField_management(BMPRATEJOIN,"IMP_ACRES_","round( !IMP_ACRES_!, 2)","PYTHON_9.3","#")

                    arcpy.AddField_management(BMPRATEJOIN,"TSS_","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"TSS_","!TSS!","PYTHON_9.3","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"TSS_","round( !TSS_!, 2)","PYTHON_9.3","#")

                    arcpy.AddField_management(BMPRATEJOIN,"TP_","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"TP_","!TP!","PYTHON_9.3","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"TP_","round( !TP_!, 2)","PYTHON_9.3","#")

                    arcpy.AddField_management(BMPRATEJOIN,"TN_","DOUBLE","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"TN_","!TN!","PYTHON_9.3","#")

                    arcpy.CalculateField_management(BMPRATEJOIN,"TN_","round( !TN_!, 2)","PYTHON_9.3","#")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(BMPRATEJOIN)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(BMPRATEJOIN)]
                FIELD_TYPE2 = arcpy.ListFields(BMPRATEJOIN)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS' or str(FIELD_NAMES2[i]) == 'RRorST':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPBMPSTR = ''
                FIELDMAPBMPSTRING2 = "'''"
                FIELDMAPBMPINT = ''
                FIELDMAPBMPSTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPBMPSTR = FIELDMAPBMPSTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + BMPRATEJOIN + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPBMPSTR2 = FIELDMAPBMPSTR[:-1]

                FIELDMAPBMPSTRING2 = FIELDMAPBMPSTRING2 + FIELDMAPBMPSTR2 + FIELDMAPBMPSTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(BMPRATEJOIN, QUERIES, "BMPSTR", "", FIELDMAPBMPSTRING2, "")

                arcpy.TableToGeodatabase_conversion(BMPSTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPBMPINT = FIELDMAPBMPINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + BMPRATEJOIN + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPBMPINT2 = FIELDMAPBMPINT[:-1]

                FIELDMAPBMPSTRING3 = FIELDMAPBMPSTRING3 + FIELDMAPBMPINT2 + FIELDMAPBMPSTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(BMPRATEJOIN, SUMMARY_GDB, "BMPINT", "", FIELDMAPBMPSTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.
                if "BMP_ID" in BMP_TABLE_FIELDLIST:
                    arcpy.Statistics_analysis(BMPRATEJOIN, BMPRATESUM, statNames_, "JURIS;RRorST")
                else:
                    pass


#_______________________________________________________________________________
        #Imports removal rate table and performs calculations.

        def rem_rate(REMOVE_RATE):


                arcpy.AddField_management(REMOVE_RATE, "KEY_", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")


                arcpy.CalculateField_management(REMOVE_RATE, "KEY_", "cond_rate( !Runoff_Dep!, !TSS_Type!, !KEY_!)", "PYTHON_9.3", "def cond_rate( a, b, c):\\n          if b == ' ':\\n                c = 23\\n          elif b !=' ':\\n                 if a == 0:\\n                        if b == 'RR':\\n                               c = 1\\n                        elif b == 'st':\\n                               c = 2\\n                 elif 0 < a <= .25:\\n                        if b == 'RR':\\n                               c = 3\\n                        elif b == 'st':\\n                               c = 4\\n                 elif .25 < a <= .5: \\n                        if b == 'RR':\\n                               c = 5\\n                        elif b == 'st':\\n                               c = 6\\n                 elif .5 < a <= .75:\\n                        if b == 'RR':\\n                               c = 7\\n                        elif b == 'st':\\n                               c = 8\\n                 elif .75 < a <= 1.00:\\n                        if b == 'RR':\\n                               c = 9\\n                        elif b == 'st':\\n                               c = 10\\n                 elif 1.00 < a <= 1.25:\\n                        if b == 'RR':\\n                               c = 11\\n                        elif b =='st':\\n                               c = 12\\n                 elif 1.25 < a <= 1.5:\\n                        if b == 'RR':\\n                               c = 13\\n                        elif b == 'st':\\n                               c = 14\\n                 elif 1.5 < a <=1.75:\\n                        if b == 'RR':\\n                               c = 15\\n                        elif b == 'st':\\n                               c = 16\\n                 elif 1.75 < a <= 2.00:\\n                        if b == 'RR':\\n                               c = 17\\n                        elif b == 'st':\\n                               c = 18\\n                 elif 2.00 < a <= 2.25:\\n                        if b == 'RR':\\n                               c = 19\\n                        elif b == 'st':\\n                               c = 20\\n                 elif 2.25 < a <= 2.5:\\n                         if b == 'RR':\\n                               c = 21\\n                         elif b == 'st':\\n                               c =22\\n          return c")

#_______________________________________________________________________________

        #Function for performing calculations and querying the restoration BMP shapefile.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def RESTBMPCALC(self, RESTBMP, RESTTBL, RESTTBL_, RESTBMP_FIELDLIST, QUERIES, JURIS_CHOICE, RESTJOIN, RESTJOIN_, RESTJOINTBL, JURIS_BORD, RESTSUM):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(RESTBMP) if f.name in RESTBMP_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(RESTBMP) if f.name in RESTBMP_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(RESTBMP)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in RESTBMP_FIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPREST = ''

                FIELDMAPRESTSTRING = "'''"

                FIELDMAPREST = FIELDMAPREST + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + RESTBMP + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPREST = FIELDMAPREST + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + RESTBMP + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPREST2 = FIELDMAPREST[:-1]

                FIELDMAPRESTSTRING = FIELDMAPRESTSTRING + FIELDMAPREST2 + FIELDMAPRESTSTRING

                JURISSPLIT = JURIS_CHOICE.split(";")

                JURISLIST = []

                JURIFIELDLIST = ''

                for i in JURISSPLIT:
                    if "s'" in i:
                        i = i.replace("'", '')
                        i = i.replace("s", "'s")
                        i = '"' + i + '"'
                    else:
                        i = i.replace("'", '')
                        i = '"' + i + '"'
                    JURISLIST.append(i)
                for i in range(0, len(JURISLIST)):
                    if len(JURISLIST) == 1:
                        JURISFIELD = "[JURIS]" + "=" + JURISLIST[0]
                        break
                    elif len(JURISLIST) == 1:
                        JURISFIELD = "(" + "[JURIS]" + " " + "=" + " " + JURISLIST[i] + ")"
                    elif len(JURISLIST) > 1:
                        JURISFIELD = "(" + "[JURIS]" + " " + "=" + " " + JURISLIST[i]  + ")"
                        if (i+1) < len(JURISLIST):
                            JURISFIELD = JURISFIELD  + " "+ "AND" + " "
                    JURIFIELDLIST  = JURIFIELDLIST + JURISFIELD

                arcpy.CalculateField_management(RESTBMP, "GEN_COMMENTS", "!GEN_COMMENTS![0:254]", "PYTHON_9.3", "")

                arcpy.TableToTable_conversion(RESTBMP, QUERIES, "RESTTBL.dbf","#", FIELDMAPRESTSTRING,"#")

                arcpy.FeatureClassToFeatureClass_conversion(JURIS_BORD, QUERIES,"JURISBORD.shp","#","#","#")

                arcpy.SpatialJoin_analysis(RESTBMP, QUERIES + "\\" + "JURISBORD.shp", RESTJOIN, "JOIN_ONE_TO_MANY", "KEEP_ALL", "#", "WITHIN", "", "")

                FIELD_NAMES_JOIN = [g.name for g in arcpy.ListFields(RESTJOIN)]

                FIELD_LENGTHS_JOIN = [g.length for g in arcpy.ListFields(RESTJOIN)]

                FIELD_TYPE_JOIN = arcpy.ListFields(RESTJOIN)

                FIELDVAL2 = []

                for fields in FIELD_TYPE_JOIN:
                    if str(fields.type) == 'string':
                        FIELDVAL2.append('Text')
                    else:
                        FIELDVAL2.append(str(fields.type))
                FIELDMAPJOIN = ''

                FIELDMAPJOINSTRING = "'''"

                FIELDMAPJOIN = FIELDMAPJOIN + FIELD_NAMES_JOIN[0] + ' ' + '"' + FIELD_NAMES_JOIN[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS_JOIN[0]) + ' ' + FIELDVAL2[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + RESTJOIN + ',' + ' ' + FIELD_NAMES_JOIN[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELDVAL2)):

                    FIELDMAPJOIN = FIELDMAPJOIN + FIELD_NAMES_JOIN[i] + ' ' + '"' + FIELD_NAMES_JOIN[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS_JOIN[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + RESTJOIN + ',' + ' ' + FIELD_NAMES_JOIN[i] + ',-1,-1' + ';'

                FIELDMAPJOIN2 = FIELDMAPJOIN[:-1]

                FIELDMAPJOINSTRING = FIELDMAPJOINSTRING + FIELDMAPJOIN2 + FIELDMAPJOINSTRING

                arcpy.TableToTable_conversion(RESTJOIN, QUERIES,"RESTJOINTBL.dbf", "#", "#", "#")

                #If "IMP_ACRES" is in the list of field names, perform calculations.

                if "IMP_ACRES" in RESTBMP_FIELDLIST:

                    arcpy.CalculateField_management(RESTJOINTBL, "IMP_ACRES", "ifnull( !IMP_ACRES!)", "PYTHON_9.3", "def ifnull(a):\\n      if a == -999:\\n           a = 0\\n      elif a == 77777:\\n           a = 0\\n      return a")

                #If "PE_REQ" is in the list of field names, perform calculations.

                if "PE_REQ" in RESTBMP_FIELDLIST:

                    arcpy.CalculateField_management(RESTJOINTBL, "PE_REQ", "ifnull( !PE_REQ! )", "PYTHON_9.3", "def ifnull(a):\\n    if a == -999:\\n         a = 0\\n    return a")

                #If "PE_ADR" is in the list of field names, perform calculations.

                if "PE_ADR" in RESTBMP_FIELDLIST:

                    arcpy.CalculateField_management(RESTJOINTBL, "PE_ADR", "ifnull( !PE_ADR!)", "PYTHON_9.3", "def ifnull(a):\\n     if a == -999:\\n         a = 0\\n     return a")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(RESTJOINTBL)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(RESTJOINTBL)]
                FIELD_TYPE2 = arcpy.ListFields(RESTJOINTBL)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[fields].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPRESTSTR = ''
                FIELDMAPRESTSTRING2 = "'''"
                FIELDMAPRESTINT = ''
                FIELDMAPRESTSTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPRESTSTR = FIELDMAPRESTSTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + RESTJOINTBL + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPRESTSTR2 = FIELDMAPRESTSTR[:-1]

                FIELDMAPRESTSTRING2 = FIELDMAPRESTSTRING2 + FIELDMAPRESTSTR2 + FIELDMAPRESTSTRING2

                #If "LAST_CHANGE" is in the list of field names, perform calculations.

                if "LAST_CHANGE" in RESTBMP_FIELDLIST:

                    arcpy.AddField_management(RESTJOINTBL,"LASTCHANGE","TEXT","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(RESTJOINTBL, "LASTCHANGE", "str(!LAST_CHANG!)", "PYTHON_9.3", "")

                    arcpy.DeleteField_management(RESTJOINTBL,"LAST_CHANG")

                #If "APPR_DATE" is in the list of field names, perform calculations.

                if "APPR_DATE" in RESTBMP_FIELDLIST:

                    arcpy.AddField_management(RESTJOINTBL, "APPRDATE", "TEXT","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(RESTJOINTBL, "APPRDATE", "str(!APPRDATE!)", "PYTHON_9.3", "")

                    arcpy.DeleteField_management(RESTJOINTBL,"APPR_DATE")

                #If "BUILT_DATE" is in the list of field names, perform calculations.

                if "BUILT_DATE" in RESTBMP_FIELDLIST:

                    arcpy.AddField_management(RESTJOINTBL,"BUILTDATE","TEXT","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(RESTJOINTBL,"BUILTDATE","str(!BUILT_DATE!)","PYTHON_9.3","#")

                    arcpy.DeleteField_management(RESTJOINTBL,"BUILT_DATE")

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(RESTJOINTBL, QUERIES, "RESTSTR", "", FIELDMAPRESTSTRING2, "")

                arcpy.TableToGeodatabase_conversion(RESTSTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPRESTINT = FIELDMAPRESTINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + RESTJOINTBL + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPRESTINT2 = FIELDMAPRESTINT[:-1]

                FIELDMAPRESTSTRING3 = FIELDMAPRESTSTRING3 + FIELDMAPRESTINT2 + FIELDMAPRESTSTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(RESTJOINTBL, SUMMARY_GDB, "RESTINT", "", FIELDMAPRESTSTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                arcpy.Statistics_analysis(RESTJOINTBL, RESTSUM, statNames_, "county")

#_______________________________________________________________________________

        #Function for performing calculations, querying the BMP drainage area shapefile.
        #Outputs tabular string and numerical summary data to a geodatabase.

        def BMPDRAINCALC(self, BMPDRAIN, BMPDRAINFIELDLIST, QUERIES, DRAINTBL, DRAINTBL_, BMPRATEJOIN, RESTTBL_, DRAINJOIN):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(BMPDRAIN) if f.name in BMPDRAINFIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(BMPDRAIN) if f.name in BMPDRAINFIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(BMPDRAIN)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in BMPDRAINFIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPDRAIN = ''

                FIELDMAPDRAINSTRING = "'''"

                FIELDMAPDRAIN = FIELDMAPDRAIN + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + BMPDRAIN + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPDRAIN = FIELDMAPDRAIN + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + BMPDRAIN + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPDRAIN2 = FIELDMAPDRAIN[:-1]

                FIELDMAPDRAINSTRING = FIELDMAPDRAINSTRING + FIELDMAPDRAIN2 + FIELDMAPDRAINSTRING

                arcpy.TableToTable_conversion(BMPDRAIN, QUERIES, DRAINTBL, "", FIELDMAPDRAINSTRING, "")

                #If "BMP_DRAIN_ID" is in the list of field names, perform calculations.

                if "BMP_DRAIN_ID" in BMPDRAINFIELDLIST:

                    arcpy.AddField_management(DRAINTBL_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(DRAINTBL_, "JURIS", "!BMP_DRAIN_![0:2]", "PYTHON_9.3", "")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(DRAINTBL_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(DRAINTBL_)]
                FIELD_TYPE2 = arcpy.ListFields(DRAINTBL_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []
                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPDRAINSTR = ''
                FIELDMAPDRAINSTRING2 = "'''"
                FIELDMAPDRAININT = ''
                FIELDMAPDRAINSTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPDRAINSTR = FIELDMAPDRAINSTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + BMPRATEJOIN + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPDRAINSTR2 = FIELDMAPDRAINSTR[:-1]

                FIELDMAPDRAINSTRING2 = FIELDMAPDRAINSTRING2 + FIELDMAPDRAINSTR2 + FIELDMAPDRAINSTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(DRAINTBL_, QUERIES, "DRAINSTR", "", FIELDMAPDRAINSTRING2, "")

                arcpy.TableToGeodatabase_conversion(DRAINSTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPDRAININT = FIELDMAPDRAININT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + BMPRATEJOIN + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPDRAININT2 = FIELDMAPDRAININT[:-1]

                FIELDMAPDRAINSTRING3 = FIELDMAPDRAINSTRING3 + FIELDMAPDRAININT2 + FIELDMAPDRAINSTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(BMPRATEJOIN, SUMMARY_GDB, "DRAININT", "", FIELDMAPDRAINSTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                if "BMP_DRAIN_ID" in BMPDRAINFIELDLIST:
                    arcpy.Statistics_analysis(BMPRATEJOIN, DRAINSUM, statNames_, "JURIS")
                else:
                    pass

#_______________________________________________________________________________

        #Function for performing calculations and querying the municipal facitlies shapefile.

        def MUNICIPALFACILITY(self, MUNICIPAL_FACILITIES, MUNFACFIELDLIST, QUERIES, MUN_FAC_TBL_, MUNFACSUM):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(MUNICIPAL_FACILITIES) if f.name in MUNFACFIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(MUNICIPAL_FACILITIES) if f.name in MUNFACFIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(MUNICIPAL_FACILITIES)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in MUNFACFIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPMUNFAC = ''

                FIELDMAPMUNFACSTRING = "'''"

                FIELDMAPMUNFAC = FIELDMAPMUNFAC + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + MUNICIPAL_FACILITIES + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPMUNFAC = FIELDMAPMUNFAC + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + MUNICIPAL_FACILITIES + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPMUNFAC2 = FIELDMAPMUNFAC[:-1]

                FIELDMAPMUNFACSTRING = FIELDMAPMUNFACSTRING + FIELDMAPMUNFAC2 + FIELDMAPMUNFACSTRING

                arcpy.TableToTable_conversion(MUNICIPAL_FACILITIES, QUERIES, "MUN_FAC_TBL.dbf", "", FIELDMAPMUNFACSTRING, "")

                #If "MUNI_FACILITIES_ID" is in the list of field names, perform calculations.

                if "MUNI_FACILITIES_ID" in MUNFACFIELDLIST:

                    arcpy.AddField_management(MUN_FAC_TBL_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(MUN_FAC_TBL_, "JURIS", "!MUNI_FACIL![0:2]", "PYTHON_9.3", "")

                    arcpy.AddField_management(MUN_FAC_TBL_, "YEAR", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(MUN_FAC_TBL_, "YEAR", "!MUNI_FACIL![2:4]", "PYTHON_9.3", "")

                #If "QRT_INSP" is in the list of field names, perform calculations.

                if "QRT_INSP" in MUNFACFIELDLIST:

                    arcpy.CalculateField_management(MUN_FAC_TBL_, "QRT_INSP", "ifyes( !QRT_INSP! )", "PYTHON_9.3", "def ifyes(a):\\n     if a == 'Yes':\\n           a = 'Y'\\n     return a ")

                    arcpy.AddField_management(MUN_FAC_TBL_, "INSPBIN", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(MUN_FAC_TBL_, "INSPBIN", "ifinspect( !QRT_INSP! , !INSPBIN! )", "PYTHON_9.3", "def  ifinspect(a, b):\\n     if a == 'Y':\\n          b = 1\\n     elif a != 'Y':\\n          b = 0 \\n     return b")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(MUN_FAC_TBL_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(MUN_FAC_TBL_)]
                FIELD_TYPE2 = arcpy.ListFields(MUN_FAC_TBL_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []

                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPMUNFACSTR = ''
                FIELDMAPMUNFACSTRING2 = "'''"
                FIELDMAPMUNFACINT = ''
                FIELDMAPMUNFACSTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPMUNFACSTR = FIELDMAPMUNFACSTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + MUN_FAC_TBL_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPMUNFACSTR2 = FIELDMAPMUNFACSTR[:-1]

                FIELDMAPMUNFACSTRING2 = FIELDMAPMUNFACSTRING2 + FIELDMAPMUNFACSTR2 + FIELDMAPMUNFACSTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(MUN_FAC_TBL_, QUERIES, "MUNFACSTR", "", FIELDMAPMUNFACSTRING2, "")

                arcpy.TableToGeodatabase_conversion(MUNFACSTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPMUNFACINT = FIELDMAPMUNFACINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + MUN_FAC_TBL_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPMUNFACINT2 = FIELDMAPMUNFACINT[:-1]

                FIELDMAPMUNFACSTRING3 = FIELDMAPMUNFACSTRING3 + FIELDMAPMUNFACINT2 + FIELDMAPMUNFACSTRING3

                #Perform arcpy function table-to-table on list of numerical fields.

                arcpy.TableToTable_conversion(MUN_FAC_TBL_, SUMMARY_GDB, "MUNFACINT", "", FIELDMAPMUNFACSTRING3, "")


#_______________________________________________________________________________
class QUERYtoGDB:

        #Function that queries fiscal analyses table and outputs tabular string and numerical summary data to a geodatabase.

        def FISCQUERY(self, FISCALANALYSES, FISCTBL, FISCAN_FIELDLIST, FISCTBL_, QUERIES):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(FISCALANALYSES) if f.name in FISCAN_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(FISCALANALYSES) if f.name in FISCAN_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(FISCALANALYSES)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in FISCAN_FIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPFISCAN = ''

                FIELDMAPFISCANSTRING = "'''"


                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPFISCAN = FIELDMAPFISCAN + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + FISCTBL + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'


                FIELDMAPFISCANSTRING = FIELDMAPFISCANSTRING + FIELDMAPFISCAN + FIELDMAPFISCANSTRING

                #Performs arcpy table-to-table function in order to develop new copy of the original table.

                arcpy.TableToTable_conversion(FISCALANALYSES, QUERIES, FISCTBL, "", FIELDMAPFISCANSTRING, "")

                if "FISCAL_ID" in FISCAN_FIELDLIST:

                    arcpy.AddField_management(FISCTBL_, "JURIS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(FISCTBL_, "JURIS", "!FISCAL_ID![0:2]", "PYTHON_9.3", "")

                #Peforms arcpy table-to-geodatabase function in order to send a copy of the table above to a desired geodatabase.
                arcpy.TableToGeodatabase_conversion(FISCTBL_,SUMMARY_GDB)

#_______________________________________________________________________________

        #Function that queries the quarterly grading permit info table and quarterly grading permit shapefile while also outputting summary data to a geodatabase.

        def QUGRPERM(self, QUARTERLYGRADINGPMTINFO, QUARTERLYGRADINGPERMITS, QUGRINFO, QUGR, QGPI_FIELDLIST, QGP_FIELDLIST, QUGR_, QUGRPERM, QUGRPERM_, QUERIES, QUGRJOIN):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(QUARTERLYGRADINGPMTINFO) if f.name in QGPI_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(QUARTERLYGRADINGPMTINFO) if f.name in QGPI_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(QUARTERLYGRADINGPMTINFO)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in QGPI_FIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPQGPI = ''

                FIELDMAPQGPISTRING = "'''"

                FIELDMAPQGPI = FIELDMAPQGPI + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + QUARTERLYGRADINGPMTINFO + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPQGPI = FIELDMAPQGPI + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + QUARTERLYGRADINGPMTINFO + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPQGPI2 = FIELDMAPQGPI[:-1]

                FIELDMAPQGPISTRING = FIELDMAPQGPISTRING + FIELDMAPQGPI2 + FIELDMAPQGPISTRING

                #Performs arcpy table-to-table function in order to develop new copy of the original table.

                arcpy.TableToTable_conversion(QUARTERLYGRADINGPMTINFO, QUERIES, QUGR, "", FIELDMAPQGPISTRING, "")

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(QUARTERLYGRADINGPERMITS) if g.name in QGP_FIELDLIST]

                FIELD_LENGTHS2 = [g.length for g in arcpy.ListFields(QUARTERLYGRADINGPERMITS) if g.name in QGP_FIELDLIST]

                FIELD_TYPE2 = arcpy.ListFields(QUARTERLYGRADINGPERMITS)

                FIELDVAL2 = []

                for field2 in FIELD_TYPE2:

                    if field2.name in QGP_FIELDLIST:

                        if str(field2.type) == 'string':

                            FIELDVAL2.append('Text')

                        else:

                            FIELDVAL2.append(str(field2.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPQGP = ''

                FIELDMAPQGPSTRING = "'''"

                FIELDMAPQGP = FIELDMAPQGP + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + QUARTERLYGRADINGPERMITS + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES2)):

                    FIELDMAPQGP = FIELDMAPQGP + FIELD_NAMES2[i] + ' ' + '"' + FIELD_NAMES2[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS2[i]) +  ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + QUARTERLYGRADINGPERMITS + ',' + ' ' + FIELD_NAMES2[i] + ',-1,-1' + ';'

                FIELDMAPQGP2 = FIELDMAPQGP[:-1]

                FIELDMAPQGPSTRING = FIELDMAPQGPSTRING + FIELDMAPQGP2 + FIELDMAPQGPSTRING

                #Perform arcpy functions' table-to-table and table-to-geodatabase on table that is a join of the output quarterly grading permit info table and the quarterly grading permit shapefile.

                arcpy.TableToTable_conversion(QUARTERLYGRADINGPERMITS, QUERIES, "QUGRPERM.dbf", "", FIELDMAPQGPSTRING, "")

                arcpy.JoinField_management(QUGR_, "QGP_ID", QUGRPERM_, "QGP_ID")

                arcpy.TableToTable_conversion(QUGR_, QUERIES, QUGRJOIN, "#", "#", "#")

                arcpy.TableToGeodatabase_conversion(QUERIES + "\\" + "QUGRJOIN.dbf", SUMMARY_GDB)


#_______________________________________________________________________________

        #Function queries user-selected variables from permit info table and exports it to a geodatabase.

        def PERMITQUERY(self, PERMITINFO, PERMITINFO_FIELDLIST, QUERIES):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(PERMITINFO) if f.name in PERMITINFO_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(PERMITINFO) if f.name in PERMITINFO_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(PERMITINFO)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in PERMITINFO_FIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPPERMIN = ''

                FIELDMAPPERMINSTRING = "'''"

                FIELDMAPPERMIN = FIELDMAPPERMIN + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + PERMITINFO + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPPERMIN = FIELDMAPPERMIN + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + PERMITINFO + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPPERMIN2 = FIELDMAPPERMIN[:-1]

                FIELDMAPPERMINSTRING = FIELDMAPPERMINSTRING + FIELDMAPPERMIN2 + FIELDMAPPERMINSTRING

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(PERMITINFO, QUERIES, PERMTBL, "", FIELDMAPPERMINSTRING, "")

                arcpy.CalculateField_management(PERMTBL_, "CONTACT_TI", "ifnull( !CONTACT_TI! )", "PYTHON_9.3", "def ifnull(a):\\n    if a == ' ':\\n         a = \"N/A\"\\n    return a")

                arcpy.TableToGeodatabase_conversion(PERMTBL_,SUMMARY_GDB)

#_______________________________________________________________________________

        #Function queries user-selected variables from ESC table and exports it to a geodatabase.

        def ESCQUERY(self, ESC, QUERIES, ESC_FIELDLIST, ESCTBL):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(ESC) if f.name in ESC_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(ESC) if f.name in ESC_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(ESC)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in ESC_FIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPESC = ''

                FIELDMAPESCSTRING = "'''"

                FIELDMAPESC = FIELDMAPESC + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + ESC + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPESC = FIELDMAPESC + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + ESC + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPESC2 = FIELDMAPESC[:-1]

                FIELDMAPESCSTRING = FIELDMAPESCSTRING + FIELDMAPESC2 + FIELDMAPESCSTRING

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(ESC, QUERIES, ESCTBL, "", FIELDMAPESCSTRING, "")

                if "ESC_ID" in ESC_FIELDLIST:

                    arcpy.AddField_management(ESCTBL_,"JURIS","TEXT","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(ESCTBL_,"JURIS","!ESC_ID![0:2]","PYTHON_9.3","#")

                arcpy.TableToGeodatabase_conversion(ESCTBL_, SUMMARY_GDB)

#_______________________________________________________________________________

        #Function queries user-selected variables from SWM table and exports it to a geodatabase.

        def SWMCALC(self, SWM, SWM_FIELDLIST, QUERIES, SWMSUM):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(SWM) if f.name in SWM_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(SWM) if f.name in SWM_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(SWM)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in SWM_FIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPSWM = ''

                FIELDMAPSWMSTRING = "'''"

                FIELDMAPSWM = FIELDMAPSWM + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + SWM + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPSWM = FIELDMAPSWM + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + SWM + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPSWM2 = FIELDMAPSWM[:-2]

                FIELDMAPSWMSTRING = FIELDMAPSWMSTRING + FIELDMAPSWM2 + FIELDMAPSWMSTRING

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(SWM, QUERIES, SWMTBL, "", FIELDMAPSWMSTRING, "")

                #If "SWM_ID" is in the list of field names, perform calculations.

                if "SWM_ID" in SWM_FIELDLIST:

                    arcpy.AddField_management(SWMTBL_,"JURIS","TEXT","#","#","#","#","NULLABLE","NON_REQUIRED","#")

                    arcpy.CalculateField_management(SWMTBL_,"JURIS","STRINGJURIS( !SWM_ID!, !JURIS! )","PYTHON_9.3","def STRINGJURIS(a, b):\\n       b = a[:2]\\n       return b")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(SWMTBL_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(SWMTBL_)]
                FIELD_TYPE2 = arcpy.ListFields(SWMTBL_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []

                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'JURIS':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPSWMSTR = ''
                FIELDMAPSWMSTRING2 = "'''"
                FIELDMAPSWMINT = ''
                FIELDMAPSWMSTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPSWMSTR = FIELDMAPSWMSTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + SWMTBL_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPSWMSTR2 = FIELDMAPSWMSTR[:-1]

                FIELDMAPSWMSTRING2 = FIELDMAPSWMSTRING2 + FIELDMAPSWMSTR2 + FIELDMAPSWMSTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(SWMTBL_, QUERIES, "SWMSTR", "", FIELDMAPSWMSTRING2, "")

                arcpy.TableToGeodatabase_conversion(SWMSTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):

                    FIELDMAPSWMINT = FIELDMAPSWMINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + SWMTBL_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPSWMINT2 = FIELDMAPSWMINT[:-1]

                FIELDMAPSWMSTRING3 = FIELDMAPSWMSTRING3 + FIELDMAPSWMINT2 + FIELDMAPSWMSTRING3

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(SWMTBL_, SUMMARY_GDB, "SWMINT", "", FIELDMAPSWMSTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                if "SWM_ID" in SWM_FIELDLIST:
                    arcpy.Statistics_analysis(SWMTBL_,SWMSUM,statNames_,"JURIS")
                else:
                    pass


#_______________________________________________________________________________

        #Function queries user-selected variables from IDDE table and exports it to a geodatabase.

        def IDDECALC(self, IDDE, IDDE_FIELDLIST, QUERIES):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(IDDE) if f.name in IDDE_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(IDDE) if f.name in IDDE_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(IDDE)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in IDDE_FIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPIDDE = ''

                FIELDMAPIDDESTRING = "'''"

                FIELDMAPIDDE = FIELDMAPIDDE + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + IDDE + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPIDDE = FIELDMAPIDDE + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + IDDE + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPIDDE2 = FIELDMAPIDDE[:-1]

                FIELDMAPIDDESTRING = FIELDMAPIDDESTRING + FIELDMAPIDDE2 + FIELDMAPIDDESTRING

                if "GEN_COMMENTS" in IDDE_FIELDLIST:

                    arcpy.CalculateField_management(IDDE,"GEN_COMMENTS","!GEN_COMMENTS![0:254]","PYTHON_9.3","#")

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(IDDE, QUERIES, IDDETBL, "", FIELDMAPIDDESTRING, "")

                #If "SCREEN_DATE" and then if "LAST_RAIN" are in the list of field names, perform calculations.

                if "SCREEN_DATE" in IDDE_FIELDLIST:

                    arcpy.AddField_management(IDDETBL_, "YEAR", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(IDDETBL_,"YEAR","YEAR( [SCREEN_DAT] )","VB","#")

                    arcpy.CalculateField_management(IDDETBL_,"YEAR","ifnot1900( !YEAR! )","PYTHON_9.3","def ifnot1900(a):\\n    if a < 1900:\\n          a = 1900\\n    return a")

                    arcpy.AddField_management(IDDETBL_, "MONTH", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(IDDETBL_,"MONTH","MONTH( [SCREEN_DAT] )","VB","#")

                    arcpy.AddField_management(IDDETBL_, "DAY", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(IDDETBL_,"DAY","DAY( [SCREEN_DAT] )","VB","#")

                    arcpy.AddField_management(IDDETBL_, "DOFY_S", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(IDDETBL_, "DOFY_S", "NUMDAYs( !YEAR! , !MONTH! , !DAY!, !DOFY_S! )", "PYTHON_9.3", "import datetime\\ndef NUMDAYs(a, b, c, d):\\n     e = datetime.datetime(int(a), int(b), int(c))\\n     f = e.strftime(\"%j\")\\n     d = int(f)\\n     return d")

                    if "LAST_RAIN" in IDDE_FIELDLIST:

                        arcpy.AddField_management(IDDETBL_, "YEAR2", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(IDDETBL_, "YEAR2", "YEAR([LAST_RAIN])", "VB", "")

                        arcpy.CalculateField_management(IDDETBL_,"YEAR2","ifnot1900( !YEAR2! )","PYTHON_9.3","def ifnot1900(a):\\n    if a < 1900:\\n          a = 1900\\n    return a")

                        arcpy.AddField_management(IDDETBL_, "MONTH2", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(IDDETBL_, "MONTH2", "MONTH( [LAST_RAIN] )", "VB", "")

                        arcpy.AddField_management(IDDETBL_, "DAY2", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(IDDETBL_, "DAY2", "DAY([LAST_RAIN])", "VB", "")

                        arcpy.AddField_management(IDDETBL_, "DOFY_R", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(IDDETBL_, "DOFY_R", "NUMDAYs( !YEAR2! , !MONTH2! , !DAY2!, !DOFY_R! )", "PYTHON_9.3", "import datetime\\ndef NUMDAYs(a, b, c, d):\\n     if c != 0 and a != 1900:\\n             e = datetime.datetime(int(a), int(b), int(c))\\n             f = e.strftime(\"%j\")\\n             d = int(f)\\n     return d")

                        arcpy.AddField_management(IDDETBL_, "DIFFDATE", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(IDDETBL_, "DIFFDATE", "DIFFDATE( !DOFY_S!, !DOFY_R!, !DIFFDATE!)", "PYTHON_9.3", "def DIFFDATE(b, c, d):\\n     if c == 0:\\n           d = -999\\n     elif c != 0:\\n           d = b - c\\n     return d")

                        arcpy.AddField_management(IDDETBL_, "DRYFLOW", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(IDDETBL_, "DRYFLOW", "dryweatherflow( !DIFFDATE!, !DRYFLOW!)", "PYTHON_9.3", "def dryweatherflow(a, b):\\n         if a <= 7:\\n               b = 'N'\\n         elif a > 7:\\n               b = 'Y'\\n         return b")

                        arcpy.AddField_management(IDDETBL_, "NUMDRYFLOW", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(IDDETBL_, "NUMDRYFLOW", "drybinary( !DRYFLOW!, !NUMDRYFLOW!)", "PYTHON_9.3", "def drybinary(a, b):\\n       if a == 'Y':\\n            b = 1\\n       elif a != 'Y':\\n            b = 0 \\n       return b")

                #If "IDDE_ID" is in the list of field names, perform calculations.

                if "IDDE_ID" in IDDE_FIELDLIST:

                    arcpy.AddField_management(IDDETBL_, "JURIS_", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(IDDETBL_, "JURIS_", "!IDDE_ID![0:2]", "PYTHON_9.3", "")

                #If "CHEM_TEST" is in the list of field names, perform calculations.

                if "CHEM_TEST" in IDDE_FIELDLIST:

                    arcpy.AddField_management(IDDETBL_, "CHEMTEST", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(IDDETBL_, "CHEMTEST", "CHEMTESTNUM( !CHEM_TEST!, !CHEMTEST!)", "PYTHON_9.3", "def CHEMTESTNUM(a, b):\\n         if a == 'Y':\\n              b = 1\\n         elif a != \"Y\":\\n              b = 0\\n         return b")

                #If "ILLICIT_Q" and then if "ILLICIT_ELIM" are in the list of field names, perform calculations.

                if "ILLICIT_Q" in IDDE_FIELDLIST:

                    arcpy.AddField_management(IDDETBL_, "ILL_DIS", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                    arcpy.CalculateField_management(IDDETBL_, "ILL_DIS", "illdischargeNUM( !ILLICIT_Q!, !ILL_DIS!)", "PYTHON_9.3", "def illdischargeNUM(a, b):\\n         if a == 'Y':\\n              b = 1\\n         elif a != \"Y\":\\n              b = 0\\n         return b")

                    if "ILLICIT_ELIM" in IDDE_FIELDLIST:
                        arcpy.AddField_management(IDDETBL_, "ILL_STAT", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

                        arcpy.CalculateField_management(IDDETBL_, "ILL_STAT", "illdis_status( !ILL_DIS!, !ILLICIT_EL!, !ILL_STAT!)", "PYTHON_9.3", "def illdis_status(a, b, c):\\n         if a == 1:\\n              if b == 'E':\\n                   c = 1\\n              elif b == 'N':\\n                   c = 0\\n              elif b == 'C':\\n                   c = 0 \\n              elif b == 'I':\\n                   c = 0\\n         return c")

                #Perform similar actions as above but with separating out the string fields from the numerical fields.

                FIELD_NAMES2 = [g.name for g in arcpy.ListFields(IDDETBL_)]
                FIELD_LENGTHS2 = [h.length for h in arcpy.ListFields(IDDETBL_)]
                FIELD_TYPE2 = arcpy.ListFields(IDDETBL_)
                FIELDVAL2 = []
                FIELDNAME2 = []
                FIELDLENGTH2 = []
                FIELDVAL3 = []
                FIELDNAME3 = []
                FIELDLENGTH3 = []

                for fields in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[fields].type) == 'String'or str(FIELD_TYPE2[i].type) == 'Date':
                        FIELDVAL2.append('Text')
                        FIELDNAME2.append(str(FIELD_NAMES2[fields]))
                        FIELDLENGTH2.append(FIELD_LENGTHS2[fields])
                for i in range(0, len(FIELD_TYPE2)):
                    if str(FIELD_TYPE2[i].type) != 'String' and str(FIELD_TYPE2[i].type) != 'Date':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])
                for i in range(0, len(FIELD_NAMES2)):
                    if str(FIELD_NAMES2[i]) == 'PERMIT_NUM':
                        FIELDVAL3.append(str(FIELD_TYPE2[i].type))
                        FIELDNAME3.append(str(FIELD_NAMES2[i]))
                        FIELDLENGTH3.append(FIELD_LENGTHS2[i])

                FIELDVAL3_ = FIELDVAL3[2:]
                FIELDNAME3_ = FIELDNAME3[2:]
                FIELDLENGTH3_ = FIELDLENGTH3[2:]
                FIELDMAPIDDESTR = ''
                FIELDMAPIDDESTRING2 = "'''"
                FIELDMAPIDDEINT = ''
                FIELDMAPIDDESTRING3 = "'''"
                for i in range(0, len(FIELDNAME2)):
                    FIELDMAPIDDESTR = FIELDMAPIDDESTR + FIELDNAME2[i] + ' ' + '"' + FIELDNAME2[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH2[i]) + ' ' + FIELDVAL2[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + IDDETBL_ + ',' + ' ' + FIELDNAME2[i] + ',-1,-1' + ';'

                FIELDMAPIDDESTR2 = FIELDMAPIDDESTR[:-1]

                FIELDMAPIDDESTRING2 = FIELDMAPIDDESTRING2 + FIELDMAPIDDESTR2 + FIELDMAPIDDESTRING2

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(IDDETBL_, QUERIES, "IDDESTR", "", FIELDMAPIDDESTRING2, "")

                arcpy.TableToGeodatabase_conversion(IDDESTR_GDB, SUMMARY_GDB)

                for i in range(0, len(FIELDNAME3_)):
                    FIELDMAPIDDEINT = FIELDMAPIDDEINT + FIELDNAME3_[i] + ' ' + '"' + FIELDNAME3_[i] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELDLENGTH3_[i]) + ' ' + FIELDVAL3_[i] + ' ' + '0' + ' ' + '0' + ',First,#,' + ' ' + IDDETBL_ + ',' + ' ' + FIELDNAME3_[i] + ',-1,-1' + ';'

                FIELDMAPIDDEINT2 = FIELDMAPIDDEINT[:-1]

                FIELDMAPIDDESTRING3 = FIELDMAPIDDESTRING3 + FIELDMAPIDDEINT2 + FIELDMAPIDDESTRING3

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(IDDETBL_, SUMMARY_GDB, "IDDEINT", "", FIELDMAPIDDESTRING3, "")

                statNames = ""

                for i in range(0, len(FIELDNAME3_) - 2):
                    statNames = statNames + FIELDNAME3_[i] + " MEAN; "
                    statNames = statNames + FIELDNAME3_[i] + " SUM; "
                statNames_ = statNames[:-2]

                #Perform summary statistics function in order to summarize numerical data.

                if "IDDE_ID" in IDDE_FIELDLIST:
                    arcpy.Statistics_analysis(IDDETBL_, IDDESUM, statNames_, "PERMIT_NUM")
                else:
                    pass

#_______________________________________________________________________________
        #Function that queries out user-selected variables and creates a query table in a geodatabase.
        def NARRQUERY(self, NARRATIVEFILES, NARRFILE_FIELDLIST, NARRTBL, QUERIES):

                #Creates lists containing field names and field lengths.

                FIELD_NAMES1 = [f.name for f in arcpy.ListFields(NARRATIVEFILES) if f.name in NARRFILE_FIELDLIST]

                FIELD_LENGTHS1 = [f.length for f in arcpy.ListFields(NARRATIVEFILES) if f.name in NARRFILE_FIELDLIST]

                FIELD_TYPE1 = arcpy.ListFields(NARRATIVEFILES)

                FIELDVAL = []

                for field in FIELD_TYPE1:

                    if field.name in NARRFILE_FIELDLIST:

                        if str(field.type) == 'string':

                            FIELDVAL.append('Text')

                        else:

                            FIELDVAL.append(str(field.type))

                #Pulls values from names and lenths lists in order to structure the statement that outlines which fields are to be included in the table to table arcpy function.

                FIELDMAPNARR = ''

                FIELDMAPNARRSTRING = "'''"

                FIELDMAPNARR = FIELDMAPNARR + FIELD_NAMES1[0] + ' ' + '"' + FIELD_NAMES1[0] + '"' + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' + ' ' + str(FIELD_LENGTHS1[0]) + ' ' + FIELDVAL[0] + ' ' + '0' + ' ' + '0' + ',First,#' + ' ' + NARRATIVEFILES + ',' + ' ' + FIELD_NAMES1[0] + ',-1,-1' + ';'

                for i in range(0, len(FIELD_NAMES1)):

                    FIELDMAPNARR = FIELDMAPNARR + FIELD_NAMES1[i] + ' ' + '"' + FIELD_NAMES1[i] + '"'  + ' ' + 'true' + ' ' + 'false' + ' ' + 'false' +  ' ' + str(FIELD_LENGTHS1[i]) +  ' ' + FIELDVAL[i] + ' ' + '0' + ' ' + '0' + ' ' + ',First,#,' + ' ' + NARRATIVEFILES + ',' + ' ' + FIELD_NAMES1[i] + ',-1,-1' + ';'

                FIELDMAPNARR2 = FIELDMAPNARR[:-1]

                FIELDMAPNARRSTRING = FIELDMAPNARRSTRING + FIELDMAPNARR2 + FIELDMAPNARRSTRING

                #Perform arcpy functions' table-to-table and table-to-geodatabase on list of string fields.

                arcpy.TableToTable_conversion(NARRATIVEFILES, QUERIES, NARRTBL, "", FIELDMAPNARR, "")

                arcpy.TableToGeodatabase_conversion(NARRTBL_, SUMMARY_GDB)


if __name__ == '__main__':

#Calls bath of the QUERYtoGDB and CALCULATIONS classes.
#Individual functions are called if the main table or shapefile has a value.

        QUERY = QUERYtoGDB()

        if PERMITINFO != '':
            PERMIT = QUERY.PERMITQUERY(PERMITINFO, PERMITINFO_FIELDLIST, QUERIES)
        else:
            pass
        if ESC != '':
            ESC = QUERY.ESCQUERY(ESC, QUERIES, ESC_FIELDLIST, ESCTBL)
        else:
            pass
        if QUARTERLYGRADINGPMTINFO != '':
            QUGRPERM = QUERY.QUGRPERM(QUARTERLYGRADINGPMTINFO, QUARTERLYGRADINGPERMITS, QUGRINFO, QUGR, QGPI_FIELDLIST, QGP_FIELDLIST, QUGR_, QUGRPERM, QUGRPERM_, QUERIES, QUGRJOIN)
        else:
            pass
        if IDDE != '':
            IDDE = QUERY.IDDECALC(IDDE, IDDE_FIELDLIST, QUERIES)
        else:
            pass
        if SWM != '':
            SWMQUERY = QUERY.SWMCALC(SWM, SWM_FIELDLIST, QUERIES, SWMSUM)
        else:
            pass
        if FISCALANALYSES != '':
            FISCAN = QUERY.FISCQUERY(FISCALANALYSES, FISCTBL, FISCAN_FIELDLIST, FISCTBL_, QUERIES)
        else:
            pass
        if PERMITINFO != '':
            PERMINFO = QUERY.PERMITQUERY(PERMITINFO, PERMITINFO_FIELDLIST, QUERIES)
        else:
            pass
        if NARRATIVEFILES != '':
            NARRQUERY = QUERY.NARRQUERY(NARRATIVEFILES, NARRFILE_FIELDLIST, NARRTBL, QUERIES)
        else:
            pass

        CALC = CALCULATIONS()

        if ALTBMPPOINT != '':
            ALTBMPPtCALC = CALC.ALTBMPPOINT(ALTBMPPOINT, ALTBMPPOINT_FIELDLIST, QUERIES, ALTPTTBL_, ALTPOINTSUM)
        else:
            pass
        if ALTBMPPOLY != '':
            ALTBMPCALC = CALC.ALTBMPPOLY(ALTBMPPOLY, ALTBMPPOLY_FIELDLIST, ALTBMPPTABLE, ALTBMPPTABLE_, ALTPOLYSUM, QUERIES)
        else:
            pass
        if ALTBMPLINE != '':
            ALTBMPLINECALC = CALC.ALTBMPLINECALC(ALTBMPLINE, ALTBMPLINE_FIELDLIST, ALTBMPTABLE,ALTBMPTABLE_,ALTLINESUM, QUERIES)
        else:
            pass
        if BMP_TABLE != '':
            BMP = CALC.BMPCALC(QUERIES, BMP_POI, BMP_POI_TBL, BMP_TABLE, BMP_TABLE_FIELDLIST, BMPTABLE, BMPTABLE_, BMPTABLE2, BMPTABLE2_, REMOVE_RATE, BMPRATEJOIN)
        else:
            pass
        if BMPDRAIN != '':
            BMPDRAIN = CALC.BMPDRAINCALC(BMPDRAIN, BMPDRAINFIELDLIST, QUERIES, DRAINTBL, DRAINTBL_, BMPRATEJOIN, RESTTBL_, DRAINJOIN)
        else:
            pass
        if RESTBMP != '':
            RESTBMP = CALC.RESTBMPCALC(RESTBMP, RESTTBL, RESTTBL_, RESTBMP_FIELDLIST, QUERIES, JURIS_CHOICE, RESTJOIN, RESTJOIN_, RESTJOINTBL, JURIS_BORD, RESTSUM)
        else:
            pass
        if MUNICIPAL_FACILITIES != '':
            MUNFAC = CALC.MUNICIPALFACILITY(MUNICIPAL_FACILITIES, MUNFACFIELDLIST, QUERIES, MUN_FAC_TBL_, MUNFACSUM)
        else:
            pass
        if IMPERV != '':
            IMPSURF = CALC.IMPERVCALC(IMPERV, IMPSTBL, IMPSTBL_, field, IMPSUM, QUERIES)
        else:
            pass
        if LOSWA != '':
            LOSWA_ = CALC.LOCSTCALC(LOSWA, LOCSTTABLE, LSWA_FIELDLIST, LOCSTTABLE_, LOC_SWA_SUM, QUERIES)
        else:
            pass
        if CO_SWA != '':
            COSWA = CALC.COSWACALC(CO_SWA, CO_ST_WA, CO_ST_WA_, CO_SWA_SUM , CO_SWA_FIELDLIST, QUERIES)
        else:
            pass

