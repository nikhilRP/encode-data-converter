# https://www.encodeproject.org/search/?type=experiment&month_released=December,%202014&assay_term_name=ChIP-seq&month_released=September,%202014&month_released=February,%202014&month_released=June,%202015&month_released=March,%202014&month_released=October,%202014&month_released=November,%202014&month_released=February,%202015&month_released=April,%202014&replicates.library.biosample.donor.organism.scientific_name=Mus%20musculus&files.file_type=bam&assembly=mm10

import requests
import boto
import subprocess
import os
import sys
from urllib.parse import (
    urlencode,
    urlparse,
)

_hadoop_dir = '/user/nikhilrp/encoded-data/'
_adam_submit = '/home/nikhilrp/tools/adam/bin/adam-submit'
EPILOG = __doc__
SERVER = 'https://www.encodeproject.org'
S3_BUCKET = 'encode-files'
HEADERS = {'content-type': 'application/json'}


def transform_ADAM(file_name):
    """
    Moved the file to Hadoop and then transform the file to ADAM
    """
    try:
        output = subprocess.check_output([_adam_submit,
                                          'plugin',
                                          'org.encodedcc.Convert',
                                          _hadoop_dir + file_name,
					  '-plugin_args',
                                          file_name[:-4] + '.adam'])
    except:
        print("Failed to transform file %s" % file_name)
        return False
    else:
        return True
    

def delete_file(file_name):
    try:
        output = subprocess.check_output(['rm', file_name])
    except:
        print("Failed to delete file %s" % file_name)
        return False
    else:
        return True
    

def delete_HDFS_file(file_name):
    try:
        output = subprocess.check_output(['hadoop', 
					  'fs',
					  '-rm',
					  _hadoop_dir + file_name])
    except:
        print("Failed to delete file %s" % file_name)
        return False
    else:
        return True


def move_file_to_HDFS(file_name):
    """
    Move BAM file to hadoop
    """
    try:
        h_output = subprocess.check_output(['hadoop',
                                            'fs',
                                            '-put',
                                            file_name,
                                            _hadoop_dir])
    except:
        print("Unable to move file to HDFS - %s" % file_name)
        return False
    else:
        return True

    
def get_file(href):
    r = requests.get(SERVER + href,
                     headers=HEADERS, allow_redirects=True,
                     stream=True)
    try:
        r.raise_for_status
    except:
        print('%s href does not resolve' % (href))
        sys.exit()

    s3_url = r.url
    r.close()
    o = urlparse(s3_url)
    filename = os.path.basename(o.path)

    #boto.set_stream_logger('boto')
    s3 = boto.connect_s3()
    encode_bucket = s3.get_bucket('encode-files')
    key = encode_bucket.get_key(o.path)
    try:
        key.get_contents_to_filename(filename)
    except:
        response = requests.get(SERVER + href, allow_redirects=True)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
        else:
            return None
    return filename


def get_assay_JSON(url):
    """
    Searches and returns one experiment at a time
    """
    params = {
        'format': ['json'],
        'limit': ['all'],
        'field': ['files.accession', 'files.href',
                  'files.file_format', 'accession']
    }
    path = '%s&%s' % (url, urlencode(params, True))
    response = requests.get(path)
    for exp in response.json()['@graph']:
        yield exp
        

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Module to convert ENCODE data to parquet files",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    #parser.add_argument('--encode-url', help="ENCODE assay URL", required=True)
    args = parser.parse_args()
    encode_url = 'https://www.encodeproject.org/search/?type=experiment&month_released=December,%202014&assay_term_name=ChIP-seq&month_released=September,%202014&month_released=February,%202014&month_released=June,%202015&month_released=March,%202014&month_released=October,%202014&month_released=November,%202014&month_released=February,%202015&month_released=April,%202014&replicates.library.biosample.donor.organism.scientific_name=Mus%20musculus&files.file_type=bam&assembly=mm10'
    if encode_url:
        for exp in get_assay_JSON(encode_url):
            for f in exp.get('files', None):
                if f['file_format'] in ['bam', 'sam']:
                    file_name = get_file(f['href'])
                    if file_name is None:
                        print("Failed to download the file %s" % f[href])
                        continue
                    if not move_file_to_HDFS(file_name):
                        continue
                    if not delete_file(file_name):
                        continue
                    if not transform_ADAM(file_name):
                        continue
                    if not delete_HDFS_file(file_name):
                        continue


if __name__ == '__main__':
    main()
