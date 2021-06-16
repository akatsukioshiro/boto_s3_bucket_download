import boto3
import json
import os
import tarfile
import glob
import shutil
from datetime import datetime

def s3extract_csvzip(**opt):
    print("Execution initiated:",datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    
    if opt.get("bucket","NA") == "NA":
        print("bucket not chosen.")
        exit()
    
    if opt.get("region","NA") == "NA":
        print("region not chosen.")
        exit()
    
    if opt.get("condition","NA") == "NA":
        print("condition not chosen.")
        exit()
    
    if opt.get("root_path","NA") == "NA":
        print("root_path not chosen.")
        exit()
    
    # Common variables
    bkt=opt.get("bucket")
    rgn=opt.get("region")
    cnd=opt.get("condition")
    rtp=opt.get("root_path")

    supported=["zip","tar","gztar","bztar","xztar"]
    
    if cnd not in supported:
        print("Unsupported compression requested.",cnd,"format not available")
        exit()

    if os.path.isdir(rtp) == False:
        print("Path:",rtp,"is not valid/not a valid directory.")
        exit()
    
    # Create a client to access s3
    client = boto3.client('s3', region_name=rgn)

    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    try:
        page_iterator = paginator.paginate(Bucket=bkt)
        
        buckp=os.path.join(rtp,bkt)

        # Delete old bucket directory and create new
        if os.path.isdir(buckp) == True:
            shutil.rmtree(buckp, ignore_errors=True)
            os.mkdir(buckp)

        # Create a directory with name of bucket
        elif os.path.isdir(buckp) == False:
            os.mkdir(buckp)

        # This is utilized b condition 4
        zippaths=[]

        # Iterate over every object in the chosen bucket
        for page in page_iterator:
            for cont in page['Contents']:
                
                # Object path
                path=cont["Key"]
                dest=os.path.join(buckp,path)
                if path[-1] == "/":
                    print("Diretory:",path)
                    try:
                        if os.path.isdir(dest) == False:
                            os.mkdir(dest)
                    except FileNotFoundError:
                        print("Path missing, creating it.")
                        print(dest)
                        piece=dest.split("/")
                        print(piece)
                        print(len(piece))
                        for x in range(1,len(piece)):
                            dr="/".join(piece[0:x])
                            print(dr)
                            if dr.strip() != "":
                                if os.path.isdir(dr) == False:
                                    os.mkdir(dr)
                    except FileExistsError:
                        pass

                else:
                    print("File:",path)
                    
                    try:
                        client.download_file(bkt, path, dest)
                    except FileNotFoundError:
                        print("Path missing, creating it.")
                        print(dest)
                        piece=dest.split("/")
                        print(piece)
                        print(len(piece))
                        for x in range(1,len(piece)):
                            dr="/".join(piece[0:x])
                            print(dr)
                            if dr.strip() != "":
                                if os.path.isdir(dr) == False:
                                    os.mkdir(dr)
                        client.download_file(bkt, path, dest)
                    except FileExistsError:
                        pass

        for name in glob.glob(os.path.join(rtp,"*")):
            compressed=False
            for types in supported:
                if types in name:
                    compressed=True
                    break
            if compressed == False:
                shutil.make_archive(name, cnd, name)
                shutil.rmtree(name, ignore_errors=True)
                

        print("Execution completed:",datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    except client.exceptions.NoSuchBucket as nsb:
        print(nsb)


s3extract_csvzip(
        bucket="bucket name here",
        condition="xztar",
        region="us-east-2",
        root_path="/home/akatsuki/aws_s3/new/s3"
        )
