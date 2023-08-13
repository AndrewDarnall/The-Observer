""" 
    ================================================================================
        
            Stream API connector, the foundational layer of the TAP project 


    ================================================================================
                
"""
import asyncio
import httpx
import sys
import json
import os

""" The script is made parametric in order to facilitate its Docker deployment """
STREAM_ENDPOINT = os.getenv("STREAM_ENDPOINT")
STREAM_API_NAME = os.getenv("STREAM_API_NAME")
# STREAM_ENDPOINT = 'https://mastodon.uno/api/v1/streaming/public'

""" 
    ================================================================================
    The stream_data() method is an async method for event retrieval

    This method asynchronously removes the event header and only parses and saves
    the relevant events, i.e. those that contain data

    However some further pre-processing will be required in order to remove
    the HTML tags from the text data
    ================================================================================
 """
async def stream_data(fp):

    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream('GET', STREAM_ENDPOINT) as response:
            async for line in response.aiter_lines():
                if line:
                    """ 
                        ================================================================================
                                Since the raw lines contain events which are quite unsafe to store
                                and parse, some pre-processing has to be made before the lines can
                                be forwarded to the API
                        ================================================================================
                       """
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith("event:"):
                        event_type = line.split(":", 1)[1].strip()
                        continue
                    if line.startswith("data:"):
                        data = line.split(":", 1)[1]
                        # This exception handling is important because the api often
                        # sends ill terminated JSON strings
                        try:
                            decoded = json.loads(data)
                        except json.JSONDecodeError as e:
                            print("JSON Decoder exception raised:\n\n{}".format(e))

                        if not isinstance(decoded, dict):
                            # This print line is useful to provide some feedback i.e. when using 'docker logs'
                            print("event_type", event_type, "data", data, " (not a dict)")
                            continue
                        
                        """ 
                            ================================================================================
                                Remember to flush the information from the buffer or there might be
                                some inconsistencies with the data, which will cause the pipeline 
                                to terminate abruptly
                            ================================================================================
                           """
                        written=fp.write(data+"\n")
                        # This print line is useful to provide some feedback i.e. when using 'docker logs'
                        print("written",written)
                        fp.flush() 
                        continue


                    # Use for debugging purposes only
                    # print(line)




# Defining the output-file handle
fh = open("./data/" + STREAM_API_NAME + ".jsonl","a")

loop = asyncio.get_event_loop()
loop.run_until_complete(stream_data(fh))