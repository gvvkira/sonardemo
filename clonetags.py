import pandas as pd
import numpy as np
import app_config as cfg
import sys, os
import json
import requests
import logging
from datetime import datetime, timedelta
from flask_cors import CORS
import timeseries as ts
from flask import Flask, jsonify, request, abort
from datetime import datetime as dt
import time
import math
from functools import reduce
from pprint import pprint as pp
from logzero import logger, logfile

config = cfg.getconfig()
qr=ts.timeseriesquery()

app = Flask(__name__)
CORS(app)

def getUnitDetails(idd):
    url = config['api']['meta'] + '/units/' + idd
    res = requests.get(url)
    content = json.loads(res.content)
    bu = content['bu'][0]
    
    return bu


@app.route('/devflask3/clonetags', methods=['POST'])
def clonetags():
    if not request.json or not "toUnitId" in request.json or not "listOfTags" in request.json:
        abort(400)
    
    if len(request.json['toUnitId']) == 0:
        abort(400)
    
    if type(request.json['listOfTags']) != list or len(request.json['listOfTags']) == 0:
        abort(400)
    
    print (request.json['toUnitId'])
    bu = getUnitDetails(request.json['toUnitId'])
    
    tagsList = []
    # lastData = []
    for i in range(len(request.json['listOfTags'])):
        dataTagId_from = request.json['listOfTags'][i]
        logger.info("From tag: " + dataTagId_from)

        ## ignoring dummtTag
        if "dummytag" in dataTagId_from.lower():
            continue

        url = config['api']['meta'] + '/tagmeta?filter={"where":{"dataTagId":"'+str(dataTagId_from)+'"}}'
        metaBody = requests.get(url) .
		

        try:


            metaOld = json.loads(metaBody.content)

#	    print(metaOld,"before "*20)	.    	
		### to replace NaN with '-'
	    metaBody = pd.DataFrame(metaOld)
            metaBody = metaBody.fillna("-")
#            print(metaBody,"dffff-"*20)
	    metaBody = metaBody.to_dict("records")
            meta = metaBody[0]
	    
#	    print(meta, "------------")
	    ret = dict()
            storeUnassigned = []
            keyList = ["equipment", "component", "subcomponent", "measureType", "measureProperty", "measureLocation", "tagType","system"]
            for key in keyList:
                if key not in meta.keys():
                    logger.info(key + " does not exist in meta")
                    continue
                if "system" in key and bu == "heating":
                    continue
                ret[key] = meta[key]
                if ret[key].lower() == 'unassigned':
                    storeUnassigned.append(0)


            if len(storeUnassigned) == len(keyList):
                ## search by description
                descriptionsList = meta['description'].split(" ")

                descList = []
                for desc in descriptionsList:
                    descList.append({"description":{"like":desc,"options":"i"}})

                query = {"where":{"and":descList}}
                getUrl = config['api']['meta'] + '/units/' + str(request.json['toUnitId']) +'/tagmeta?filter=' + json.dumps(query)
                logger.info("getUrl search by description " + getUrl)
                tagId_to = requests.get(getUrl)



            else:
                getUrl = config['api']['meta'] + '/units/' + str(request.json['toUnitId']) +'/tagmeta?filter={"where":'+json.dumps(ret)+'}'
                #logger.info("getUrl ",getUrl)
                tagId_to = requests.get(getUrl)
                #logger.info(tagId_to)

            if len(json.loads(tagId_to.content)) == 0:
               
                
                # lastData.append([])
                # errorTags.append(meta['description'])
                logger.info("ERR======")
                logger.info(meta['description'])


		## NOTE : Below part done by Rohit ,    Since we got the empty result, now trying to fetch meta by old query 

		for key in keyList:
                    if key not in metaOld[0].keys():
                        logger.info(key + " does not exist in meta")
                        continue
                    if "system" in key and bu == "heating":
                     	continue
                    ret[key] = metaOld[0][key].
    		                     

		getUrl = config['api']['meta'] + '/units/' + str(request.json['toUnitId']) +'/tagmeta?filter={"where":'+json.dumps(ret)+'}'
                logger.info("getUrl "+ getUrl)
                tagId_to = requests.get(getUrl)
                logger.info(tagId_to)
		print(" fetch meta by old query  ")
		print(json.loads(tagId_to.content))

		if len(json.loads(tagId_to.content)) > 0:       
	 	    dataTagIs_to = [i["dataTagId"] for i in json.loads(tagId_to.content)]
                    if bu == "heating":
                     	dataTagIs_to = dataTagIs_to[0]
                    tagsList.append([dataTagIs_to])
		
		else:
		    tagsList.append([])

	    ################################################################################# DONE BY ROHIT ABOVE LINE ############   

            elif len(json.loads(tagId_to.content)) == 1:
                dataTagIs_to = json.loads(tagId_to.content)[0]["dataTagId"]
                tagsList.append([dataTagIs_to])
                #lastData.append([getLastData(dataTagIs_to,request.json['toUnitId'])])
            else:
                dataTagIs_to = [i["dataTagId"] for i in json.loads(tagId_to.content)]
                if bu == "heating":
                    dataTagIs_to = dataTagIs_to[0]
                tagsList.append([dataTagIs_to])
                
                # lstData = []
                # if bu == "heating":
                    #lstData.append(getLastData(dataTagIs_to,request.json['toUnitId']))
                    # pass

                # else:
                #     for tag in dataTagIs_to:
                #         lstData.append(getLastData(tag,request.json['toUnitId']))

                # lastData.append(lstData)
        except Exception as e:
            logger.info("EXCEPTION: Unable to find tag because of error - ")
            logger.error(e)
            tagsList.append([])
            # lastData.append([])
    
    # logger.info("Tags list: " + str(tagsList))
    # logger.info("Last data: " + str(lastData))
    
    responseBody = {"tags":tagsList}

    return json.dumps(responseBody),200


    if __name__ == "__main__":
    app.run(host='0.0.0.0', threaded=True, port=7713, debug=False)
