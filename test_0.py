import boto3
import json
import os
import gzip
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
    
    if opt.get("folder","NA") == "NA":
        print("folder not chosen.")
        exit()
    
    if opt.get("file_type","NA") == "NA":
        print("file_type not chosen.")
        exit()
    
    # Common variables
    cwd=os.getcwd()
    bkt=opt.get("bucket")
    rgn=opt.get("region")
    fdr=opt.get("folder")
    cmt=opt.get("file_type")
    cnd=opt.get("condition")
    
    # Create a client to access s3
    client = boto3.client('s3', region_name=rgn)

    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    try:
        page_iterator = paginator.paginate(Bucket=bkt)

        # Delete old bucket directory and create new
        if os.path.isdir(bkt) == True:
            shutil.rmtree(os.path.join(cwd,bkt), ignore_errors=True)
            os.mkdir(bkt)

        # Create a directory with name of bucket
        elif os.path.isdir(bkt) == False:
            os.mkdir(bkt)

        # This is utilized b condition 4
        zippaths=[]

        # Iterate over every object in the chosen bucket
        for page in page_iterator:
            for cont in page['Contents']:
                
                # Object path
                path=cont["Key"]
                
                # Select only files that satisfy our requirements
                if fdr in path and path[-1]!="/" and path.split(".")[-1]==cmt: 
                    #print(path)

                    # Conditions discussed
                    if cnd == "1" or cnd == "2":
                    
                        filename=path.split("/")[0]
                        
                        # To prevent rerun error
                        if os.path.isdir(os.path.join(cwd,bkt,filename)) == False:
                            os.mkdir(os.path.join(cwd,bkt,filename))
                    
                        # Path to dump compressed file
                        zippath=os.path.join(cwd,bkt,filename,path.split("/")[-1])
                    
                        # File download
                        client.download_file(bkt, path, zippath)
                    elif cnd == "3" or cnd == "4":

                        # Directory structure being recreated
                        trail = path.split("/")
                        trail_builder=[cwd,bkt]
                        temp=""

                        for pieces in trail:
                        
                            if cmt not in pieces:
                            
                                trail_builder.append(pieces)
                                temp="/".join(trail_builder)
                                
                                if os.path.isdir(temp) == False:
                                    os.mkdir(temp)
                        
                        # Path to dump compressed file
                        zippath="/".join([temp,path.split("/")[-1]])
                        
                        # Remember paths
                        zippaths.append(zippath) 
                        
                        # File download
                        client.download_file(bkt, path, zippath)


        # Simple compress with files not unzipped
        if cnd == "1" or cnd == "3":
            for name in glob.glob(os.path.join(cwd,bkt,"*")):
                #print(name)
                if "zip" not in name:
                    shutil.make_archive(name, 'zip', name)
                    shutil.rmtree(name, ignore_errors=True)

        # Unzip and compress but no directory path maintained
        elif cnd == "2":
            files=[]
            for name in glob.glob(os.path.join(cwd,bkt,"*","*")):
                #print(name)
                if cmt in name:
                    unz_name=name.split(cmt)[0][:-1]
                    files.append(unz_name)
                    with gzip.open(name, 'rb') as f_in:
                        with open(unz_name, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                            os.remove(name)

            # Delete directories post zipping
            for name in glob.glob(os.path.join(cwd,bkt,"*")):
                if name not in files:
                    #print(name)
                    shutil.make_archive(name, 'zip', name)
                    shutil.rmtree(name, ignore_errors=True)

        # Unzip and compress with directory path maintained
        elif cnd == "4":
            files=[]
            for pathZ in zippaths:
                for name in glob.glob(pathZ):
                    #print(name)
                    unz_name=name.split(cmt)[0][:-1]
                    files.append(unz_name)
                    with gzip.open(name, 'rb') as f_in:
                        with open(unz_name, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                            os.remove(name)
            
            # Delete directories post zipping
            for name in glob.glob(os.path.join(cwd,bkt,"*")):
                if name not in files and "zip" not in name:
                    #print(name)
                    shutil.make_archive(name, 'zip', name)
                    shutil.rmtree(name, ignore_errors=True)


        print("Execution completed:",datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    except client.exceptions.NoSuchBucket as nsb:
        print(nsb)


s3extract_csvzip(
        bucket="s3_bucket_name",
        condition="4",
        region="us-east-2",
        folder="data",
        file_type="gz"
        )
