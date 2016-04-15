"""
Usage:
    hql_runner [options] <hql> [<param>...]

Runs a hql script transforming hql parameters.

Arguments:
    <hql>                    The path to the hql script to run
    <param>                  The optional list of parameters and values
                               in format param=value

Options:
    --hive-server=<host>     The host of the hive server to use
                               [default: hiveserver-ia.s3s.altiscale.com]
    --hive-port=<port>       The port of the hive server to use
                               [default: 10000]
    --user=<user>            The user to impersonate in hive
                               (defaults to user running the script)
    --hive-database=<db>     The hive database against which to run the script
                               [default: wmf_dumps]
    -d --debug               Print debug logging
"""
import logging
import sys
import os.path

import docopt
from impala import dbapi
import re


logger = logging.getLogger(__name__)


class HQLRunner(object):

    def __init__(self,
                 hql_path,
                 params,
                 server,
                 port,
                 user,
                 database,
                 debug):

        self.hql_path = hql_path
        self.params = params
        self.server = server
        self.port = port
        self.user = user
        self.database = database
        self.debug = debug

        self._init_logging()
        self._check_hql_script()
        self._init_and_check_hql_params()

    #
    # Init Functions
    #
    def _init_logging(self):
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(name)s -- %(message)s'
        )
        logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

    def _check_hql_script(self):
        logger.debug("Checking hql script is a file")
        if not os.path.isfile(self.hql_path):
            raise RuntimeError("Provided hql script is not a file")

    def _init_and_check_hql_params(self):
        logger.debug("Checking provided hql params match hql script ones")

        self.hql_params = {}
        if self.params:
            self.hql_params = dict(p.split("=")for p in self.params)

        param_pattern = re.compile("\$\{(\w+)\}")
        wrong_params = {}

        with open(self.hql_path, "r") as hql_file:
            hql = hql_file.read()
            for m in param_pattern.finditer(hql):
                param = m.group(1)
                if param not in self.hql_params or not hql_params[param]:
                    wrong_params[param] = True

        if wrong_params:
            raise RuntimeError("Undefined parameters: {0}".format(
                ", ".join(wrong_params.keys())))

    #
    # Run Functions
    #
    def run(self):
        self._hql_script_to_actions()
        self._execute_actions()

    def _hql_script_to_actions(self):
        logger.debug("Preparing hql script")
        comment_pattern = re.compile("--.*\n")

        with open(self.hql_path, "r") as hql_file:
            uncommented_hql = comment_pattern.sub("", hql_file.read())

        parameterized_hql = uncommented_hql
        for param, value in hql_params.iteritems():
            parameterized_hql = parameterized_hql.replace(
                "${{{0}}}".format(param), value)

        hql_actions = [s.strip() for s in parameterized_hql.split(";")]
        self.hql_actions = filter(bool, hql_actions)

    def _execute_actions(self):
        with dbapi.connect(host=server,
                           port=port,
                           user=user,
                           database=database,
                           auth_mechanism="PLAIN") as conn:
            cursor = conn.cursor()
            for action in self.hql_actions:
                logger.debug("Executing hql action:\n{0}\n".format(action))
                cursor.execute(action)


def main(args):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s -- %(message)s"
    )
    logger.setLevel(logging.DEBUG if args["--debug"] else logging.INFO)

    hql_path = args["<hql>"]
    params = args["<param>"]
    server = args["--hive-server"]
    port = int(args["--hive-port"])
    user = args["--user"]
    database = args["--hive-database"]

    hql_runner = HQLRunner(hql_path,
                           params,
                           server,
                           port,
                           user,
                           database)
    hql_runner.run()


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
