import argparse
import logging
import os.path
import subprocess
import sys
import hdfs

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Converts a MediaWiki XML dump to sorted, flattened JSON documents.')

    parser.add_argument('input', help='path to input directory to read XML dump data')
    parser.add_argument('output', help='path to output directory to write revdocs')
    parser.add_argument('--name-node', metavar='HOST', help='cluster name-node', default='http://nn-ia.s3s.altiscale.com:50070')
    parser.add_argument('-j', '--jar', metavar='PATH', help='mapreduce job jar', default='/wmf/jars/wikihadoop-0.2.jar')
    parser.add_argument('-c', '--class', dest='job_class', metavar='NAME', help='mapreduce job class', default='org.wikimedia.wikihadoop.job.JsonRevisionsSortedPerPage')
    parser.add_argument('-r', '--reducers', metavar='NUM', type=int, help='number of reducers for mapreduce job', default=2000)
    parser.add_argument('-t', '--timeout', metavar='SECS', type=int, help='mapreduce task timeout in seconds', default=3600)
    parser.add_argument('--mapper-mb', metavar='MB', type=int, help='mapper memory in Yarn', default=2048)
    parser.add_argument('--mapper-mb-heap', metavar='MB', type=int, help='mapper memory in JVM', default=1792)
    parser.add_argument('--reducer-mb', metavar='MB', type=int, help='reducer memory in Yarn', default=3072)
    parser.add_argument('--reducer-mb-heap', metavar='MB', type=int, help='reducer memory in JVM', default=2816)
    parser.add_argument('-f', '--force', action='store_true', help='if set, will overwrite old output')
    parser.add_argument('-v', '--verbose', action='count', help='log verbosity', default=0)

    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s -- %(message)s')
    levels = [logging.WARN, logging.INFO, logging.DEBUG]
    logger.setLevel(levels[min(len(levels)-1, args.verbose)])

    run(args.input, args.output, args.name_node, args.jar, args.job_class, args.timeout * 1000, args.mapper_mb,
        args.mapper_mb_heap, args.reducer_mb, args.reducer_mb_heap, args.reducers, args.force)


def run(input_path, output_path, name_node, jar, job_class, timeout, mapper_mb,
        mapper_mb_heap, reducer_mb, reducer_mb_heap, reducers, force):
    
    hdfs_client = hdfs.Client(name_node)
    
    if done(hdfs_client, output_path) and not force:
        raise RuntimeError('already completed.')
    
    dump2revdocs(input_path, output_path, jar, job_class, timeout, mapper_mb,
                 mapper_mb_heap, reducer_mb, reducer_mb_heap, reducers)
    
    if not done(hdfs_client, output_path):
        raise RuntimeError('something went wrong')


def done(hdfs_client, output_path):
    logger.debug('checking for output at {}'.format(output_path))
    try: 
        hdfs_client.content(os.path.join(output_path, '_SUCCESS'))
        return True
    except hdfs.HdfsError:
        return False


def dump2revdocs(input_path, output_path, jar, job_class, timeout, mapper_mb, mapper_mb_heap,
                 reducer_mb, reducer_mb_heap, reducers):
    logger.debug('starting hadoop job')
    subprocess.call([str(x) for x in ['/opt/hadoop/bin/hadoop', 'jar', jar, job_class,
                     '--task-timeout', timeout,
                     '--mapper-mb', mapper_mb,
                     '--mapper-mb-heap', mapper_mb_heap,
                     '--reducer-mb', reducer_mb,
                     '--reducer-mb-heap', reducer_mb_heap,
                     '-r', reducers,
                     '-i', input_path,
                     '-o', output_path]],
                    stderr=sys.stderr,
                    stdout=sys.stdout)
    
    
if __name__ == '__main__':
    main()
