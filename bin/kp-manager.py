# Copyright 2021 ThinkPool, all rights reserved
"""
- ctrl+c dose not work with DISABLE_OOB = ON in sqlnet.ora
- run the below code as a makeshift
  $ kill -9 `ps -ef | grep "python bin/kp-manager.py" | grep -v grep | awk '{print $2}'` 
"""

__appname__ = "kafka-publisher-manager"
__version__ = "1.0"


import optparse
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import kpm
from kpm.kp_manager import KPManager


if __name__ == "__main__":
    usage = """%prog [options]"""
    parser = optparse.OptionParser(usage=usage, description=__doc__)
    # parser.add_option(
    #     "--deal-date", metavar="DEAL_DATE", dest="deal_date", help="test request"
    # )

    kpm.add_basic_options(parser)
    (options, args) = parser.parse_args()

    config_dict = kpm.read_config_file(options.config_file)

    config_dict["app_name"] = __appname__
    log_dict = config_dict.get("log", {})
    log_file_name = "kpm.log"
    kpm.setup_logging(
        appname=__appname__,
        appvers=__version__,
        filename=log_file_name,
        dirname=options.log_dir,
        debug=options.debug,
        log_dict=log_dict,
        emit_platform_info=True,
    )

    kp_manager = KPManager()
    kp_manager.initialize(config_dict)
    kp_manager.run()
