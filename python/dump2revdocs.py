"""
Converts a MediaWiki XML dump to sorted, flattened JSON documents.

Usage:
    dump2revdocs <db-date> [-f]
    
Options:
    <input>              The path to an input directory to read XML dump data
    <output>             The path to an output directory to write revdocs.
    --name-node=<host>   The host of the cluster name-node
                         [default: http://nn-wikimedia.s3s.altiscale.com:50070]
    -j --jar=<path>      Set the mapreduce job jar path
                         [default: /wmf/jars/wikihadoop-0.2.jar]
    -c --class=<name>    Set the mapreduce job class
                         [default: org.wikimedia.wikihadoop.job.JsonRevisionsSortedPerPageMapReduce]
    -r --reducers=<num>  Set mapreduce job number of reducers
                         (defines the maximum number of output files) 
                         [default: 2000]
    -t --timeout         Set the mapreduce task timeout in millis
                         [default: 3600000]
    --mapper-mb=<mb>        Mapper memory in Yarn  [default: 2048]
    --mapper-mb-heap=<mb>   Mapper memory in JVM   [default: 1792]
    --reducer-mb=<mb>       Reducer memory in Yarn [default: 3072]
    --reducer-mb-heap=<mb>  Reducer memory in JVM  [default: 2816]
    -f --force              If set, will overwrite old output
"""
import os.path
import subprocess
import sys

import docopt
import hdfs

def main():
    args = docopt.docopt(__doc__)
    
    input_path = args['<input>']
    output_path = args['<output>']
    
    name_node = args['--name-node']
    
    jar = args['--jar']
    class_ = args['--class']
    reducers = args['--reducers']
    timeout = args['--timeout']
    mapper_mb = args['--mapper-mb']
    mapper_mb_heap = args['--mapper-mb-heap']
    reducer_mb = args['--reducer-mb']
    reducer_mb_heap = args['--reducer-mb-heap']
    
    force = args['--force']
    
    run(input_path, output_path, name_node, jar, class_, timeout, mapper_mb, 
        mapper_mb_heap, reducer_mb, reducer_mb_heap, reducers, force)


def run(input_path, output_path, name_node, jar, class_, timeout, mapper_mb, 
        mapper_mb_heap, reducer_mb, reducer_mb_heap, reducers, force):
    
    hdfs_client = hdfs.Client(NAME_NODE_URL)
    
    if done(hdfs_client, output_path) and not force:
        raise RuntimeError("Already completed.")
    
    dump2revdocs(input_path, output_path, jar, class_, timeout, mapper_mb, 
                 mapper_mb_heap, reducer_mb, reducer_mb_heap, reducers)
    
    if not done(hdfs_client, output_path):
        raise RuntimeError("Something went wrong.")
    

def done(hdfs_client, output_path):
    try: 
        hdfs_client.content(os.path.join(output_path, "_SUCCESS"))
        return True
    except hdfs.HdfsError:
        return False
        
def dump2revdocs(input_path, output_path, jar, class_, timeout, mapper_mb, mapper_mb_heap, 
                 reducer_mb, reducer_mb_heap, reducers):
    subprocess.call("hadoop", "jar", jar, class_,
                    "--task-timeout", timeout,
                    "--mapper-mb", mapper_mb,
                    "--mapper-mb-heap", mapper_mb_heap,
                    "--reducer-mb", reducer_mb,
                    "--reducer-mb-heap", reducer_mb_heap,
                    "-r", reducers,
                    "-i", input_path,
                    "-o", output_path, 
                    stderr=sys.stderr, 
                    stdout=sys.stdout)
    
    
    
    
    
    
    
    