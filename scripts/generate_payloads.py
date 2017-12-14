#!/usr/bin/python
# Check to see how much was tranferred, what metadata is missing etc.

import os
import json
import fnmatch
import parse_dcc_data as dcc
import re
import jsonschema
from jsonschema import validate
import xmltodict
import subprocess
import cStringIO
import sys
import pycurl
from datetime import datetime
import hashlib

fileSongMapping = {'file_name': 'fileName', 'file_size': 'fileSize', 'file_md5sum':'fileMd5sum'}
indexFileSongMapping = {'idx_file_name': 'fileName', 'idx_file_size':'fileSize', 'idx_file_md5sum':'fileMd5sum'}
metadataFileSongMapping = {'xml_file_name': 'fileName', 'xml_file_size': 'fileSize', 'xml_file_md5sum': 'fileMd5sum'}
#metadataFileSongMapping = {'xml_file_name': 'fileName'} # temporarily manually generate MD5sums and file sizes for updated metadata XMLs

schemaFile = open("sequencingRead.json").read()
schema = json.loads(schemaFile)

log = open("generate-song-payload.log", "w")
missing_data_log = open("missing_data.log", "w")
incomplete = 0
complete = 0


def validate_payload(song_payload, analysisId, schema):
   print ("VALIDATING Payload %s"%analysisId)
   try:
      validate(song, schema)
      print "Successfully validated"
   except jsonschema.ValidationError as ve:
      print "Validation Failed"
      print ve.message
      log.write("[VALIDATION FAILED] for %s\n"%analysisId)
      log.write("\t%s\n"%ve.message)


def get_md5(fname):
    hash = hashlib.md5()
    if not os.path.isfile(fname): return None
    with open(fname) as f:
        for chunk in iter(lambda: f.read(1024*256), ""):
            hash.update(chunk)
    return hash.hexdigest()


def get_fileSize(fname):
   fileSize = None
   fileSize = os.path.getsize(fname)
   print "fileSize for fname %s = %s"%(fname, fileSize)
   if fileSize is None:
      log.write("ERROR generating file size for file %s\n"%fname)
   else:
      return fileSize


def get_metadataFromXML(bundle_type, xmlFile, song):
   pairedEnd = None
   insertSize = None
   aligned = None
   referenceGenome = None
   metadata = open(xmlFile, "r")
   obj = xmltodict.parse(metadata.read())
   if bundle_type == 'run':
      if 'PAIRED' in obj['root']['experiments_xml']['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']['LIBRARY_DESCRIPTOR']['LIBRARY_LAYOUT']:
         pairedEnd = True
         insertSize = int(obj['root']['experiments_xml']['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']['LIBRARY_DESCRIPTOR']['LIBRARY_LAYOUT']['PAIRED']['@NOMINAL_LENGTH'])
      elif 'SINGLE' in obj['root']['experiments_xml']['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']['LIBRARY_DESCRIPTOR']['LIBRARY_LAYOUT']:
         pairedEnd = False
   elif bundle_type == 'analysis':
      referenceGenome = obj['root']['analyses_xml']['ANALYSIS_SET']['ANALYSIS']['ANALYSIS_TYPE']['REFERENCE_ALIGNMENT']['ASSEMBLY']['STANDARD']['@refname']
   if pairedEnd is not None:
      song['experiment']['pairedEnd'] = pairedEnd
   if insertSize is not None:
      song['experiment']['insertSize'] = insertSize
   if referenceGenome is not None:
      song['experiment']['referenceGenome'] = referenceGenome
      song['experiment']['aligned'] = True
   return song
   

# Missing fields in Job JSONs that need to be added
def add_missing_data(job_json, field, missing_data):
   if job_json not in missing_data:
      missing_data[job_json] = {}
   missing_data[job_json][field] = 1
   return missing_data


def getFileInfo(filePath, fieldMapping, task_type, song, bundle_type):
   for worker_dir in os.listdir("%s/task_state.completed"%filePath):
      if not worker_dir.startswith('.'):
         task_filePath = "%s/task_state.completed/%s/%s/%s.json"%(filePath, worker_dir, task_type, task_type)
         if os.path.exists(task_filePath):
            file_info = {}
            task_data = open(task_filePath).read()
            task_json = json.loads(task_data)
            for i in range(len(task_json['output'])):
               for field in fieldMapping:
                  if field in task_json['output'][i]:
                     file_info[fieldMapping[field]] = task_json['output'][i][field]
            file_extension = file_info['fileName'].split(".")[-1]
            if file_extension in ('bz', 'gzip', 'gz', 'bzip2', 'bz2'):
               file_extension = file_info['fileName'].split(".")[-2]
            if file_extension == 'xml':
               file_info['fileAccess'] = 'open'
               if bundle_type == "run":
                  updated_metadata_xml = "updated_metadata_xmls/%s"%file_info['fileName']
                  file_info['fileMd5sum'] = get_md5(updated_metadata_xml)
                  file_info['fileSize'] = get_fileSize(updated_metadata_xml)
            else:
               file_info['fileAccess'] = 'controlled'
            file_info['fileType'] = file_extension.upper()
            song['file'].append(file_info)
   return song


def downloadMetadataXML(ega_file_name, object_id):
   if not os.path.exists("%s/%s"%("downloaded_analysis_metadata_xmls", ega_file_name)):
      subprocess.call(['/Users/hnahal/icgc-storage-client-1.0.23/bin/icgc-storage-client', 'download', '--object-id', object_id, '--output-dir', 'downloaded_analysis_metadata_xmls'])


def getGenderInfo():
   donorGender = {}
   dcc_file = open("donor.all_projects.tsv", "r")
   contents = dcc_file.readlines()
   contents.pop(0)
   for line in contents:
      info = line.split("\t")
      project_code = info[1]
      submitted_donor_id = info[3]
      donor_sex = info[4]
      if project_code not in donorGender:
         donorGender[project_code] = {}
      donorGender[project_code][submitted_donor_id] = donor_sex
    #  log.write("gender for %s %s = %s\n"%(project_code, submitted_donor_id, donor_sex))
   return donorGender 



#TODO Need ICGC ID service to generate DO* IDs before making call to API. For now donorGender is assigned 'unspecified' string
#def getGender(donor_id):
   #api_endpoint = "https://dcc.icgc.org/api/v1/donors?field=gender&filters={\"donor\":{\"id\":{\"is\":\"%s\"}}}&from=1&size=10&order=desc&facetsOnly=false"%donor_id
   #buf = cStringIO.StringIO()
   #c = pycurl.Curl()
   #c.setopt(c.URL, api_endpoint)
   #c.setopt(c.WRITEFUNCTION, buf.write)
   #c.perform()
   #output = buf.getvalue()
   #file_data = json.loads(output)
   #return file_data['hits'][0]['gender']

   # 


def getSampleType(library_strategy):
   if library_strategy in ('WGS', 'WXS', 'Bisulfite-Seq'):
      return "DNA"
   elif library_strategy in ('RNA-Seq', 'miRNA-Seq'):
      return "RNA"


def getSpecimenClass(specimen_type):
   if re.search("tumour", specimen_type, re.IGNORECASE):
      return "Tumour"
   elif re.search("normal", specimen_type, re.IGNORECASE):
      if re.search("adjacent", specimen_type, re.IGNORECASE):
         return "Adjacent normal"
      else:
         return "Normal"



def getSampleData(data, song, project_code):
   global donorGender
   sample_data = {}
   missing_data = {}
   sample_data['specimen'] = {}
   sample_data['donor'] = {}
   if 'submitter_specimen_id' not in data:
      missing_data = add_missing_data(job_json_file, 'submitter_specimen_id', missing_data)
   else:
      sample_data['specimen']['specimenSubmitterId'] = data['submitter_specimen_id'] 
   if 'submitter_specimen_type' not in data:
      missing_data = add_missing_data(job_json_file, 'submitter_specimen_type', missing_data)
   else:
      sample_data['specimen']['specimenType'] = data['submitter_specimen_type'] 
      sample_data['specimen']['specimenClass'] = getSpecimenClass(data['submitter_specimen_type'])
   if 'submitter_donor_id' not in data:
      missing_data = add_missing_data(job_json_file, 'submitter_donor_id', missing_data)
   else:
      sample_data['donor']['donorSubmitterId'] = data['submitter_donor_id']
      if data['submitter_donor_id'] in donorGender[project_code]:
         if donorGender[project_code][data['submitter_donor_id']] == "":
            log.write("[%s] Donor gender is not specified in DCC for donor ID %s\n"%(project_code, data['submitter_donor_id']))
            sample_data['donor']['donorGender'] = "unspecified"
         else:
            sample_data['donor']['donorGender'] = donorGender[project_code][data['submitter_donor_id']]
      else:
         log.write("[%s] Cannot retrieve submitted_donor_id %s or gender data in DCC\n"%(project_code, submitted_donor_id))
   if 'submitter_sample_id' not in data:
      missing_data = add_missing_data(job_json_file, 'submitter_sample_id', missing_data)
   else:
      sample_data['sampleSubmitterId'] = data['submitter_sample_id']
      sample_data['sampleType'] = getSampleType(data['library_strategy'])
   song['sample'].append(sample_data)
   return missing_data, song
   


donorGender = getGenderInfo()
task_number = 1 
while(task_number <=5):
   print "*** TASK NUMBER %s ***"%task_number
   if task_number == 1:
      filePath = "/Users/hnahal/ega-to-collab_transfers/ega-file-transfer-to-collab-jtracker/ega-file-transfer-to-collab.0.6.jtracker/job_state.completed"
   else:
      filePath = "/Users/hnahal/ega-to-collab_transfers/ega-file-transfer-to-collab-%s-new-jtracker/ega-file-transfer-to-collab.0.6.jtracker/job_state.completed"%task_number
   for job_dir in os.listdir(filePath):
      if not job_dir.startswith('.'):
         job_filePath = "%s/%s"%(filePath, job_dir)
         for job_json in os.listdir(job_filePath):
            song = {}
            song['experiment'] = {}
            song['sample'] = []
            song['info'] = {}
            if fnmatch.fnmatch(job_json, "job*") and (not job_json.startswith('.')):
               json_data = open("%s/%s"%(job_filePath, job_json)).read()
               data = json.loads(json_data)
               if data['project_code'] == 'ORCA-IN' or data['project_code'] == 'UTCA-FR':
                  continue
               job_json_file = "%s/%s"%(job_filePath, job_json)
               ##print "\njob json = %s"%job_json_file
               song['analysisId'] = data['bundle_id']
               song['info']['isPcawg'] = False
               if not os.path.exists("complete_song_payloads/%s/%s_song.json"%(data['project_code'], song['analysisId'])):
                  #print "OUT HERE %s"%song['analysisId']
	          song['analysisType'] = 'sequencingRead'
                  song['experiment']['libraryStrategy'] = data['library_strategy']
                  #song['study'] = data['project_code']
                  missing_data, song = getSampleData(data, song, data['project_code'])
                  bundle_type = data['bundle_type']
                  ega_metadata_file_name = data['ega_metadata_file_name']
                  ega_metadata_object_id = data['ega_metadata_object_id']
                  ega_metadata_repo = data['ega_metadata_repo']
                  song['file'] = []
                  song = getFileInfo(job_filePath, metadataFileSongMapping, "task.prepare_metadata_xml", song, bundle_type)

                  if len(missing_data) > 0:
                     print "---> ** Incomplete SONG payload due to missing metadata for %s **\n"%song['analysisId']
                     print  "Job JSON = %s\n"%job_filePath
                     missing_data_log.write("%s"%job_filePath)
                     for field in missing_data[job_json_file]:
                        missing_data_log.write("\t%s"%field)
                     missing_data_log.write("\n")
                     incomplete += 1
                  else:
                     # get metadata from XML in Collab
                     if bundle_type == 'run':
                        metadataXML = "%s/%s"%("updated_metadata_xmls", ega_metadata_file_name)
                     elif bundle_type == 'analysis':
                        downloadMetadataXML(ega_metadata_file_name, ega_metadata_object_id)
                        metadataXML = "%s/%s"%("downloaded_analysis_metadata_xmls", ega_metadata_file_name)

                     song = get_metadataFromXML(bundle_type, metadataXML, song)
                     #song['experiment']['alignmentTool'] = ""
                     # Get file metadata
                     for i in range(len(data['files'])):
                        ega_file_id = data['files'][i]['ega_file_id']
                        song = getFileInfo(job_filePath, fileSongMapping, "task.decryption.%s"%ega_file_id, song, bundle_type)
                        if data['files'][i]['idx_file_name'] is not None:
                           song = getFileInfo(job_filePath, indexFileSongMapping, "task.generate_bai.%s"%ega_file_id, song, bundle_type)
                     songFile = open("complete_song_payloads/%s/%s_song.json"%(data['project_code'], song['analysisId']), "w")
                     #print json.dumps(song, sort_keys=True, indent=4)
                     json.dump(song, songFile, sort_keys=True, indent=4)
                     validate_payload(song, song['analysisId'], schema)
                     songFile.close()
                     complete += 1
                     log.write("[CREATED NEW PAYLOAD %s]: complete_song_payloads/%s/%s_song.json\n\n"%(str(datetime.now()), data['project_code'],song['analysisId']))
               else:
                  print "SONG FILE already exists %s_song.json"%song['analysisId']
   task_number += 1
                    
print "Number of incomplete payloads = %s"%incomplete
print "Number of completed payloads = %s"%complete

