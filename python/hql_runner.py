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


def main(args):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s -- %(message)s"
    )
    logger.setLevel(logging.DEBUG if args["--debug"] else logging.INFO)

    hql_path = args["<hql>"]
    if args["<param>"]:
        hql_params = dict(param.split("=") for param in args["<param>"])
    else:
        hql_params = {}

    server = args["--hive-server"]
    port = int(args["--hive-port"])
    user = args["--user"]
    database = args["--hive-database"]

    logger.debug("Checking hql path and parameters")
    check_hql(hql_path, hql_params)

    run(hql_path, hql_params, server, port, user, database)


def check_hql(hql_path, hql_params):
    if not os.path.isfile(hql_path):
        raise RuntimeError("hql path is not a file.")
    param_pattern = re.compile("\$\{(\w+)\}")
    wrong_params = {}
    with open(hql_path, "r") as hql_file:
        hql = hql_file.read()
        for m in param_pattern.finditer(hql):
            param = m.group(1)
            if param not in hql_params or not hql_params[param]:
                wrong_params[param] = True
    if wrong_params:
        raise RuntimeError(
            "Undefined parameters: {0}".format(", ".join(wrong_params.keys())))


def run(hql_path, hql_params, server, port, user, database):
    logger.debug("Preparing hql script")
    hql_actions = hql_script_to_actions(hql_path, hql_params)

    with dbapi.connect(host=server, port=port, user=user,
                       database=database, auth_mechanism="PLAIN") as conn:
        cursor = conn.cursor()
        for action in hql_actions:
            logger.debug("Executing hql action:\n{0}\n".format(action))
            cursor.execute(action)

    logger.debug("Done !")


def hql_script_to_actions(hql_path, hql_params):
    comment_pat = re.compile("--.*\n")
    hql = None
    with open(hql_path, "r") as hql_file:
        hql = comment_pat.sub("", hql_file.read())
    for param, value in hql_params.iteritems():
        hql = hql.replace("${{{0}}}".format(param), value)
    return filter(bool, [s.strip() for s in hql.split(";")])


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except RuntimeError as e:
        logger.error(e)
        sys.exit(1)
